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
    /** 当前租约纪元：每次 PENDING -> RUNNING 递增；终态回写必须携带与之相等的 epoch */
    private volatile int leaseEpoch;
    private volatile FailureKind failureKind;
    private volatile String failureReason;
    private volatile Instant nextAttemptAt;

    public Task(String idempotencyKey, String type, String payload, int maxAttempts) {
        this.idempotencyKey = idempotencyKey;
        this.type = type;
        this.payload = payload;
        this.maxAttempts = maxAttempts;
        this.status = TaskStatus.PENDING;
        this.attempts = 0;
        this.nextAttemptAt = Instant.now();
    }

    public String getIdempotencyKey() { return idempotencyKey; }
    public String getType() { return type; }
    public String getPayload() { return payload; }
    public int getMaxAttempts() { return maxAttempts; }
    public TaskStatus getStatus() { return status; }
    public int getAttempts() { return attempts; }
    int getLeaseEpoch() { return leaseEpoch; }
    public FailureKind getFailureKind() { return failureKind; }
    public String getFailureReason() { return failureReason; }
    public Instant getNextAttemptAt() { return nextAttemptAt; }

    void setStatus(TaskStatus status) { this.status = status; }
    void setAttempts(int attempts) { this.attempts = attempts; }
    void setLeaseEpoch(int leaseEpoch) { this.leaseEpoch = leaseEpoch; }
    void setFailureKind(FailureKind failureKind) { this.failureKind = failureKind; }
    void setFailureReason(String failureReason) { this.failureReason = failureReason; }
    void setNextAttemptAt(Instant nextAttemptAt) { this.nextAttemptAt = nextAttemptAt; }
}
