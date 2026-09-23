package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 基于本地 JSON 快照文件的任务存储。
 *
 * <p>所有变更在同一把内置锁下完成并立即全量持久化（先写临时文件再原子 move），
 * 因此进程崩溃 / kill -9 后，任务要么停留在变更前的已持久化状态，要么是变更后的状态，
 * 不会出现半条记录。不依赖任何外部服务。</p>
 *
 * <p>读取/写入均做防御性拷贝，避免调用方绕过 CAS 修改内存中的记录。</p>
 */
public class JsonFileTaskStore implements TaskStore {

    private static final class Snapshot {
        public Map<String, TaskRecord> records = new LinkedHashMap<>();
    }

    private final Object lock = new Object();
    private final Path file;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, TaskRecord> records = new LinkedHashMap<>();

    public JsonFileTaskStore(String storeDir) {
        try {
            Path dir = Path.of(storeDir);
            Files.createDirectories(dir);
            this.file = dir.resolve("tasks.json");
            if (Files.exists(file)) {
                Snapshot snapshot = mapper.readValue(Files.readString(file, StandardCharsets.UTF_8), Snapshot.class);
                if (snapshot.records != null) {
                    snapshot.records.forEach((k, v) -> records.put(k, v));
                }
            } else {
                persistLocked();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("init task store failed: " + storeDir, e);
        }
    }

    @Override
    public TaskRecord putIfAbsent(TaskRecord record) {
        synchronized (lock) {
            TaskRecord existing = records.get(record.getIdempotencyKey());
            if (existing != null) {
                return copy(existing);
            }
            records.put(record.getIdempotencyKey(), copy(record));
            persistLocked();
            return null;
        }
    }

    @Override
    public boolean compareAndUpdateStatus(String idempotencyKey, TaskStatus expected, TaskStatus target) {
        synchronized (lock) {
            TaskRecord current = records.get(idempotencyKey);
            if (current == null || current.getStatus() != expected) {
                return false;
            }
            if (!StateTransition.isAllowed(expected, target)) {
                throw new IllegalStateTransitionException(idempotencyKey, expected, target);
            }
            current.setStatus(target);
            current.setUpdatedAt(System.currentTimeMillis());
            persistLocked();
            return true;
        }
    }

    @Override
    public TaskRecord claimForExecution(String idempotencyKey) {
        synchronized (lock) {
            TaskRecord current = records.get(idempotencyKey);
            if (current == null || current.getStatus() != TaskStatus.PENDING) {
                return null;
            }
            if (!StateTransition.isAllowed(TaskStatus.PENDING, TaskStatus.RUNNING)) {
                throw new IllegalStateTransitionException(idempotencyKey, TaskStatus.PENDING, TaskStatus.RUNNING);
            }
            current.setExecutionEpoch(current.getExecutionEpoch() + 1);
            current.setStatus(TaskStatus.RUNNING);
            current.setUpdatedAt(System.currentTimeMillis());
            persistLocked();
            return copy(current);
        }
    }

    @Override
    public boolean recordAttemptResult(String idempotencyKey, long expectedEpoch, TaskStatus targetStatus,
                                       int attempts, String failureReason,
                                       FailureClassification classification, long nextRunAt) {
        synchronized (lock) {
            TaskRecord current = records.get(idempotencyKey);
            if (current == null || current.getStatus() != TaskStatus.RUNNING
                    || current.getExecutionEpoch() != expectedEpoch) {
                return false;
            }
            if (!StateTransition.isAllowed(TaskStatus.RUNNING, targetStatus)) {
                throw new IllegalStateTransitionException(idempotencyKey, TaskStatus.RUNNING, targetStatus);
            }
            current.setStatus(targetStatus);
            current.setAttempts(attempts);
            current.setLastFailureReason(failureReason);
            current.setLastFailureClassification(classification);
            current.setNextRunAt(nextRunAt);
            current.setUpdatedAt(System.currentTimeMillis());
            persistLocked();
            return true;
        }
    }

    @Override
    public List<String> resetRunningToPending(String reason) {
        synchronized (lock) {
            List<String> reset = new ArrayList<>();
            boolean changed = false;
            for (TaskRecord record : records.values()) {
                if (record.getStatus() == TaskStatus.RUNNING) {
                    record.setStatus(TaskStatus.PENDING);
                    record.setNextRunAt(0L);
                    record.setUpdatedAt(System.currentTimeMillis());
                    if (reason != null) {
                        record.setLastFailureReason(reason);
                        record.setLastFailureClassification(FailureClassification.RETRYABLE);
                    }
                    reset.add(record.getIdempotencyKey());
                    changed = true;
                }
            }
            if (changed) {
                persistLocked();
            }
            return reset;
        }
    }

    @Override
    public boolean removeIfFreshPending(String idempotencyKey) {
        synchronized (lock) {
            TaskRecord current = records.get(idempotencyKey);
            if (current != null && current.getStatus() == TaskStatus.PENDING && current.getAttempts() == 0) {
                records.remove(idempotencyKey);
                persistLocked();
                return true;
            }
            return false;
        }
    }

    @Override
    public TaskRecord find(String idempotencyKey) {
        synchronized (lock) {
            TaskRecord current = records.get(idempotencyKey);
            return current == null ? null : copy(current);
        }
    }

    @Override
    public List<TaskRecord> findAll() {
        synchronized (lock) {
            List<TaskRecord> result = new ArrayList<>(records.size());
            for (TaskRecord record : records.values()) {
                result.add(copy(record));
            }
            return result;
        }
    }

    private void persistLocked() {
        try {
            Snapshot snapshot = new Snapshot();
            for (Map.Entry<String, TaskRecord> entry : records.entrySet()) {
                snapshot.records.put(entry.getKey(), copy(entry.getValue()));
            }
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            Files.writeString(tmp, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(snapshot),
                    StandardCharsets.UTF_8);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("persist task store failed: " + file, e);
        }
    }

    private TaskRecord copy(TaskRecord source) {
        return mapper.convertValue(source, TaskRecord.class);
    }
}
