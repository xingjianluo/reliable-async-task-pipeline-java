package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

import java.util.List;

/**
 * 任务记录的持久化存储，保证状态修改的原子性与进程重启后的可恢复性。
 */
public interface TaskStore {

    /** 若幂等键不存在则插入新记录，返回 null；否则返回已存在记录。 */
    TaskRecord putIfAbsent(TaskRecord record);

    /** 仅当当前状态等于 expected 时原子改为 target，成功返回 true。 */
    boolean compareAndUpdateStatus(String idempotencyKey, TaskStatus expected, TaskStatus target);

    TaskRecord find(String idempotencyKey);

    List<TaskRecord> findAll();

    /**
     * 停机恢复：原子地把所有 RUNNING 记录安全转为 PENDING（可重试状态）并立即持久化，
     * 返回被重置的幂等键。用于优雅停机超时或启动时发现上一进程遗留的在途任务。
     */
    java.util.List<String> resetRunningToPending(String reason);

    /**
     * 仅当记录处于 PENDING 且尝试次数为 0 时删除（提交背压回滚用）。
     * 若记录已被领取/执行，则不得删除，返回 false。
     */
    boolean removeIfFreshPending(String idempotencyKey);

    /**
     * 原子认领：仅当状态为 PENDING 时转为 RUNNING，执行代次加一，返回带新代次的记录副本；
     * 否则返回 null。代次用于隔离同一幂等键的重叠执行（如旧线程迟到写回）。
     */
    TaskRecord claimForExecution(String idempotencyKey);

    /**
     * 落定一次尝试结果，仅当状态为 RUNNING 且执行代次等于 expectedEpoch 时生效，
     * 否则返回 false（结果属于已失效的旧执行，必须丢弃）。
     */
    boolean recordAttemptResult(String idempotencyKey, long expectedEpoch, TaskStatus targetStatus,
                                int attempts, String failureReason,
                                FailureClassification classification, long nextRunAt);
}
