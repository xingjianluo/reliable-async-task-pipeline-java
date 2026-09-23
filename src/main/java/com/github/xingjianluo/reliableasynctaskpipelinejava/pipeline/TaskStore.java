package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内存任务存储：负责幂等去重与状态机流转。
 *
 * <p>所有状态变更按幂等键串行化（对 Task 实例加锁），保证：
 * 同一幂等键任一时刻至多一个执行者；状态只按合法路径流转，不出现非法回退或并发不一致。
 */
public class TaskStore {

    private static final Logger log = LoggerFactory.getLogger(TaskStore.class);

    private final ConcurrentHashMap<String, Task> tasks = new ConcurrentHashMap<>();

    /**
     * 幂等提交：键不存在则创建（初始状态 PENDING），已存在则返回现有任务，不产生新的业务效果。
     */
    public Task createIfAbsent(Task task) {
        Task existing = tasks.putIfAbsent(task.getIdempotencyKey(), task);
        if (existing != null) {
            log.info("[dedup] idempotencyKey={} 重复提交被识别，返回现有任务 status={} attempts={}",
                    task.getIdempotencyKey(), existing.getStatus(), existing.getAttempts());
            return existing;
        }
        log.info("[submit] idempotencyKey={} type={} maxAttempts={} 状态流转: <none> -> PENDING",
                task.getIdempotencyKey(), task.getType(), task.getMaxAttempts());
        return task;
    }

    public Optional<Task> find(String idempotencyKey) {
        return Optional.ofNullable(tasks.get(idempotencyKey));
    }

    /** PENDING -> RUNNING，仅当到达可执行时间；成功则占用该键的唯一执行权 */
    public boolean tryStart(String idempotencyKey) {
        Task task = tasks.get(idempotencyKey);
        if (task == null) {
            return false;
        }
        synchronized (task) {
            if (task.getStatus() != TaskStatus.PENDING || Instant.now().isBefore(task.getNextAttemptAt())) {
                return false;
            }
            task.setAttempts(task.getAttempts() + 1);
            task.setStatus(TaskStatus.RUNNING);
            log.info("[transition] idempotencyKey={} attempt={} 状态流转: PENDING -> RUNNING",
                    idempotencyKey, task.getAttempts());
            return true;
        }
    }

    /** RUNNING -> SUCCEEDED */
    public boolean markSucceeded(String idempotencyKey) {
        Task task = tasks.get(idempotencyKey);
        if (task == null) {
            return false;
        }
        synchronized (task) {
            if (task.getStatus() != TaskStatus.RUNNING) {
                return false;
            }
            task.setStatus(TaskStatus.SUCCEEDED);
            log.info("[transition] idempotencyKey={} attempt={} 状态流转: RUNNING -> SUCCEEDED",
                    idempotencyKey, task.getAttempts());
            return true;
        }
    }

    /** RUNNING -> PENDING（可重试失败或停机回收），记录退避后的下次执行时间 */
    public boolean markRetryPending(String idempotencyKey, FailureKind kind, String reason, long backoffMillis) {
        Task task = tasks.get(idempotencyKey);
        if (task == null) {
            return false;
        }
        synchronized (task) {
            if (task.getStatus() != TaskStatus.RUNNING) {
                return false;
            }
            task.setFailureKind(kind);
            task.setFailureReason(reason);
            task.setNextAttemptAt(Instant.now().plusMillis(backoffMillis));
            task.setStatus(TaskStatus.PENDING);
            log.info("[transition] idempotencyKey={} attempt={} 状态流转: RUNNING -> PENDING, 失败分类={}, 原因={}, 退避={}ms",
                    idempotencyKey, task.getAttempts(), kind, reason, backoffMillis);
            return true;
        }
    }

    /** RUNNING -> DEAD_LETTER，保留可区分的失败原因 */
    public boolean markDeadLetter(String idempotencyKey, FailureKind kind, String reason) {
        Task task = tasks.get(idempotencyKey);
        if (task == null) {
            return false;
        }
        synchronized (task) {
            if (task.getStatus() != TaskStatus.RUNNING) {
                return false;
            }
            task.setFailureKind(kind);
            task.setFailureReason(reason);
            task.setStatus(TaskStatus.DEAD_LETTER);
            log.info("[transition] idempotencyKey={} attempt={} 状态流转: RUNNING -> DEAD_LETTER, 失败分类={}, 原因={}",
                    idempotencyKey, task.getAttempts(), kind, reason);
            return true;
        }
    }

    /** 背压拒绝时移除尚未被消费的 PENDING 任务；若已被执行线程占用则不移除 */
    public boolean removeIfPending(String idempotencyKey) {
        Task task = tasks.get(idempotencyKey);
        if (task == null) {
            return false;
        }
        synchronized (task) {
            if (task.getStatus() != TaskStatus.PENDING) {
                return false;
            }
            tasks.remove(idempotencyKey, task);
            log.info("[reject] idempotencyKey={} 背压拒绝，移除未消费的 PENDING 任务", idempotencyKey);
            return true;
        }
    }

    /** 所有 PENDING 任务的幂等键，用于停机恢复 */
    public java.util.List<String> pendingKeys() {
        java.util.List<String> keys = new java.util.ArrayList<>();
        for (Task task : tasks.values()) {
            if (task.getStatus() == TaskStatus.PENDING) {
                keys.add(task.getIdempotencyKey());
            }
        }
        return keys;
    }

    /** 当前积压（PENDING）任务数，用于背压判断 */
    public int backlog() {
        int count = 0;
        for (Task task : tasks.values()) {
            if (task.getStatus() == TaskStatus.PENDING) {
                count++;
            }
        }
        return count;
    }
}
