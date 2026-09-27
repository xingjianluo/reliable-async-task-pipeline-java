package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.persist.InMemoryTaskRepository;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.persist.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 任务存储：幂等去重、状态机流转与 fencing token 校验。
 *
 * <p>内存中的 {@link Task} 是持久化仓储（{@link TaskRepository}）的索引与工作副本：
 * 每次状态流转先以“领取执行权时拿到的 epoch”为条件写入仓储（CAS），仓储确认后才更新内存；
 * 仓储拒绝（令牌过期，通常意味着重启恢复已 bump 世代、另一个执行者已接管）时，
 * 立即从仓储重载最新状态，旧执行放弃写回，不会覆盖更新后的结果。
 *
 * <p>所有状态变更按幂等键串行化（对 Task 实例加锁），保证同一幂等键任一时刻至多一个
 * 执行者在本进程内推进；跨进程/重启的竞争由仓储中的 epoch 条件更新裁决。
 */
public class TaskStore {

    private static final Logger log = LoggerFactory.getLogger(TaskStore.class);

    private final TaskRepository repository;
    private final ConcurrentHashMap<String, Task> tasks = new ConcurrentHashMap<>();

    /** 纯内存模式（不落盘，保持原有行为） */
    public TaskStore() {
        this(new InMemoryTaskRepository());
    }

    /**
     * 持久化模式：启动时从仓储加载全部任务。可能加载到上一个被强杀进程遗留在 RUNNING
     * 的任务，由 {@link #reclaimInterrupted()} 在新流水线启动时统一回收。
     */
    public TaskStore(TaskRepository repository) {
        this.repository = repository;
        for (Task persisted : repository.findAll()) {
            tasks.put(persisted.getIdempotencyKey(), persisted);
        }
        log.info("[persist] 已加载持久化任务数={}", tasks.size());
    }

    /**
     * 幂等提交：键不存在则创建（落盘，初始 PENDING），已存在则返回现有任务。
     * 返回值即调用方传入对象表示新受理；否则为重复投递命中的已有任务。
     */
    public Task createIfAbsent(Task task) {
        Task existing = tasks.putIfAbsent(task.getIdempotencyKey(), task);
        if (existing != null) {
            log.info("[dedup] idempotencyKey={} 重复提交被识别，返回现有任务 status={} attempts={}",
                    task.getIdempotencyKey(), existing.getStatus(), existing.getAttempts());
            return existing;
        }
        Optional<Task> persisted = repository.insertIfAbsent(task);
        if (persisted.isPresent()) {
            // 内存里没有但仓储里有：来自上一个进程（含被强杀的），以持久化任务为准
            Task stored = persisted.get();
            tasks.put(task.getIdempotencyKey(), stored);
            log.info("[dedup] idempotencyKey={} 重复提交命中重启前持久化任务 status={} attempts={}",
                    task.getIdempotencyKey(), stored.getStatus(), stored.getAttempts());
            return stored;
        }
        log.info("[submit] idempotencyKey={} type={} maxAttempts={} 状态流转: <none> -> PENDING",
                task.getIdempotencyKey(), task.getType(), task.getMaxAttempts());
        return task;
    }

    public Optional<Task> find(String idempotencyKey) {
        return Optional.ofNullable(tasks.get(idempotencyKey));
    }

    /**
     * PENDING -> RUNNING，仅当到达可执行时间。成功则占用唯一执行权并返回新 fencing token
     * （调用方之后写终态必须携带它）；未抢到返回 -1。
     */
    public long tryStart(String idempotencyKey) {
        Task task = tasks.get(idempotencyKey);
        if (task == null) {
            return -1;
        }
        synchronized (task) {
            if (task.getStatus() != TaskStatus.PENDING || Instant.now().isBefore(task.getNextAttemptAt())) {
                return -1;
            }
            long fence = repository.claimForRun(idempotencyKey, task.getEpoch());
            if (fence < 0) {
                // 竞态失败（如别的进程已领取）：以仓储为准重载，旧判断作废
                reloadFromRepository(task);
                return -1;
            }
            task.setAttempts(task.getAttempts() + 1);
            task.setEpoch(fence);
            task.setStatus(TaskStatus.RUNNING);
            log.info("[transition] idempotencyKey={} attempt={} epoch={} 状态流转: PENDING -> RUNNING",
                    idempotencyKey, task.getAttempts(), fence);
            return fence;
        }
    }

    /** RUNNING -> SUCCEEDED，仅当调用方持有当前 fencing token；终态只允许发生一次 */
    public boolean markSucceeded(String idempotencyKey, long fence) {
        Task task = tasks.get(idempotencyKey);
        if (task == null) {
            return false;
        }
        synchronized (task) {
            if (task.getStatus() != TaskStatus.RUNNING || fence != task.getEpoch()) {
                if (task.getStatus() == TaskStatus.RUNNING) {
                    rejectStaleWrite(idempotencyKey, fence, task, "SUCCEEDED");
                }
                return false;
            }
            if (!repository.completeSuccess(idempotencyKey, fence)) {
                rejectStaleWrite(idempotencyKey, fence, task, "SUCCEEDED");
                return false;
            }
            task.setEpoch(fence + 1);
            task.setStatus(TaskStatus.SUCCEEDED);
            log.info("[transition] idempotencyKey={} attempt={} epoch={} 状态流转: RUNNING -> SUCCEEDED",
                    idempotencyKey, task.getAttempts(), fence + 1);
            return true;
        }
    }

    /** RUNNING -> PENDING（可重试失败或停机/崩溃回收），记录退避后的下次执行时间 */
    public boolean markRetryPending(String idempotencyKey, long fence, FailureKind kind,
                                    String reason, long backoffMillis) {
        Task task = tasks.get(idempotencyKey);
        if (task == null) {
            return false;
        }
        synchronized (task) {
            if (task.getStatus() != TaskStatus.RUNNING) {
                return false;
            }
            if (fence != task.getEpoch()) {
                rejectStaleWrite(idempotencyKey, fence, task, "PENDING(retry)");
                return false;
            }
            Instant nextAttemptAt = Instant.now().plusMillis(backoffMillis);
            if (!repository.scheduleRetry(idempotencyKey, fence, kind.name(), reason,
                    nextAttemptAt.toEpochMilli())) {
                rejectStaleWrite(idempotencyKey, fence, task, "PENDING(retry)");
                return false;
            }
            task.setEpoch(fence + 1);
            task.setFailureKind(kind);
            task.setFailureReason(reason);
            task.setNextAttemptAt(nextAttemptAt);
            task.setStatus(TaskStatus.PENDING);
            log.info("[transition] idempotencyKey={} attempt={} epoch={} 状态流转: RUNNING -> PENDING, 失败分类={}, 原因={}, 退避={}ms",
                    idempotencyKey, task.getAttempts(), fence + 1, kind, reason, backoffMillis);
            return true;
        }
    }

    /** RUNNING -> DEAD_LETTER，保留可区分的失败原因 */
    public boolean markDeadLetter(String idempotencyKey, long fence, FailureKind kind, String reason) {
        Task task = tasks.get(idempotencyKey);
        if (task == null) {
            return false;
        }
        synchronized (task) {
            if (task.getStatus() != TaskStatus.RUNNING) {
                return false;
            }
            if (fence != task.getEpoch()) {
                rejectStaleWrite(idempotencyKey, fence, task, "DEAD_LETTER");
                return false;
            }
            if (!repository.moveToDeadLetter(idempotencyKey, fence, kind.name(), reason)) {
                rejectStaleWrite(idempotencyKey, fence, task, "DEAD_LETTER");
                return false;
            }
            task.setEpoch(fence + 1);
            task.setFailureKind(kind);
            task.setFailureReason(reason);
            task.setStatus(TaskStatus.DEAD_LETTER);
            log.info("[transition] idempotencyKey={} attempt={} epoch={} 状态流转: RUNNING -> DEAD_LETTER, 失败分类={}, 原因={}",
                    idempotencyKey, task.getAttempts(), fence + 1, kind, reason);
            return true;
        }
    }

    /** 背压拒绝时移除尚未被消费的 PENDING 任务（同时从持久化中删除）；若已被占用则不移除 */
    public boolean removeIfPending(String idempotencyKey) {
        Task task = tasks.get(idempotencyKey);
        if (task == null) {
            return false;
        }
        synchronized (task) {
            if (task.getStatus() != TaskStatus.PENDING) {
                return false;
            }
            if (!repository.deleteIfPending(idempotencyKey)) {
                reloadFromRepository(task);
                return false;
            }
            tasks.remove(idempotencyKey, task);
            log.info("[reject] idempotencyKey={} 背压拒绝，移除未消费的 PENDING 任务", idempotencyKey);
            return true;
        }
    }

    /**
     * 启动恢复：把所有 RUNNING 任务（含进程被强杀、机器重启遗留的）重置为 PENDING 并立即
     * 可执行，同时 bump epoch。任何中断后才醒来的旧执行持有旧令牌，之后写终态会被拒绝。
     *
     * @return 被回收任务的键（流水线据此重新入队）
     */
    public List<String> reclaimInterrupted() {
        List<String> reclaimed = new ArrayList<>();
        for (Task task : new ArrayList<>(tasks.values())) {
            synchronized (task) {
                if (task.getStatus() != TaskStatus.RUNNING) {
                    continue;
                }
                String key = task.getIdempotencyKey();
                long staleFence = task.getEpoch();
                if (!repository.scheduleRetry(key, staleFence, FailureKind.RETRYABLE.name(),
                        "recovered: interrupted by previous process shutdown",
                        System.currentTimeMillis())) {
                    reloadFromRepository(task);
                    log.warn("[recover] idempotencyKey={} RUNNING 记录已被其他执行者推进，跳过回收，当前状态={}",
                            key, task.getStatus());
                    continue;
                }
                task.setEpoch(staleFence + 1);
                task.setFailureKind(FailureKind.RETRYABLE);
                task.setFailureReason("recovered: interrupted by previous process shutdown");
                task.setNextAttemptAt(Instant.now());
                task.setStatus(TaskStatus.PENDING);
                reclaimed.add(key);
                log.info("[recover] idempotencyKey={} 回收中断任务 RUNNING -> PENDING，旧 epoch={} 已失效",
                        key, staleFence);
            }
        }
        return reclaimed;
    }

    /** 过期执行（fencing token 不匹配）试图写终态：拒绝并以仓储最新状态覆盖本地副本 */
    private void rejectStaleWrite(String idempotencyKey, long staleFence, Task task, String wanted) {
        reloadFromRepository(task);
        log.warn("[fence] idempotencyKey={} 过期执行(epoch={})试图流转 {} 被拒绝，当前状态={} epoch={}（以较新执行为准）",
                idempotencyKey, staleFence, wanted, task.getStatus(), task.getEpoch());
    }

    /** 在任务锁内用仓储中的最新快照原地刷新本地副本，避免旧副本覆盖新状态 */
    private void reloadFromRepository(Task task) {
        repository.find(task.getIdempotencyKey()).ifPresent(fresh -> {
            task.setStatus(fresh.getStatus());
            task.setAttempts(fresh.getAttempts());
            task.setFailureKind(fresh.getFailureKind());
            task.setFailureReason(fresh.getFailureReason());
            task.setNextAttemptAt(fresh.getNextAttemptAt());
            task.setEpoch(fresh.getEpoch());
        });
    }

    /** 所有 PENDING 任务的幂等键，用于停机恢复 */
    public List<String> pendingKeys() {
        List<String> keys = new ArrayList<>();
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
