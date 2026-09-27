package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

import java.time.Instant;

/**
 * 任务实体。状态变更只能通过 {@link TaskStore} 的 CAS 流转方法进行。
 */
public class Task {

    private final String idempotencyKey;
    private final String type;
    private final String payload;
    private final int maxAttempts;
    private volatile TaskStatus status;
    private volatile int attempts;
    private volatile FailureKind failureKind;
    private volatile String failureReason;
    private volatile Instant nextAttemptAt;
    /**
     * Fencing token（世代号）：每次状态流转 +1。执行者领取任务时拿到当时的 epoch，
     * 之后写终态必须携带它；重启恢复会 bump 遗留在 RUNNING 任务的 epoch，
     * 使中断后才醒来的旧执行令牌过期，无法覆盖更新后的结果。
     */
    private volatile long epoch;

    public Task(String idempotencyKey, String type, String payload, int maxAttempts) {
        this(idempotencyKey, type, payload, maxAttempts, TaskStatus.PENDING, 0, null, null, Instant.now(), 0);
    }

    /** 仓储加载持久化任务时使用的完整构造器 */
    public Task(String idempotencyKey, String type, String payload, int maxAttempts,
                TaskStatus status, int attempts, FailureKind failureKind, String failureReason,
                Instant nextAttemptAt, long epoch) {
        this.idempotencyKey = idempotencyKey;
        this.type = type;
        this.payload = payload;
        this.maxAttempts = maxAttempts;
        this.status = status;
        this.attempts = attempts;
        this.failureKind = failureKind;
        this.failureReason = failureReason;
        this.nextAttemptAt = nextAttemptAt;
        this.epoch = epoch;
    }

    public String getIdempotencyKey() { return idempotencyKey; }
    public String getType() { return type; }
    public String getPayload() { return payload; }
    public int getMaxAttempts() { return maxAttempts; }
    public TaskStatus getStatus() { return status; }
    public int getAttempts() { return attempts; }
    public FailureKind getFailureKind() { return failureKind; }
    public String getFailureReason() { return failureReason; }
    public Instant getNextAttemptAt() { return nextAttemptAt; }
    public long getEpoch() { return epoch; }

    void setStatus(TaskStatus status) { this.status = status; }
    void setAttempts(int attempts) { this.attempts = attempts; }
    void setFailureKind(FailureKind failureKind) { this.failureKind = failureKind; }
    void setFailureReason(String failureReason) { this.failureReason = failureReason; }
    void setNextAttemptAt(Instant nextAttemptAt) { this.nextAttemptAt = nextAttemptAt; }
    void setEpoch(long epoch) { this.epoch = epoch; }
}
