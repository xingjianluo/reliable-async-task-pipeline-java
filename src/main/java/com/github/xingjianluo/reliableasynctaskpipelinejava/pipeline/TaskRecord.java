package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

/**
 * 一个任务的持久化记录。状态的所有修改都经 TaskStore 的原子操作完成。
 */
public class TaskRecord {
    private String idempotencyKey;
    private String payload;
    private TaskStatus status = TaskStatus.PENDING;
    private int attempts;
    private long createdAt;
    private long updatedAt;
    private long nextRunAt;
    /** 执行代次：每次 PENDING->RUNNING 的认领加一，用于丢弃旧执行迟到的结果。 */
    private long executionEpoch;
    private String lastFailureReason;
    private FailureClassification lastFailureClassification;

    public String getIdempotencyKey() { return idempotencyKey; }
    public void setIdempotencyKey(String idempotencyKey) { this.idempotencyKey = idempotencyKey; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public TaskStatus getStatus() { return status; }
    public void setStatus(TaskStatus status) { this.status = status; }
    public int getAttempts() { return attempts; }
    public void setAttempts(int attempts) { this.attempts = attempts; }
    public long getCreatedAt() { return createdAt; }
    public void setCreatedAt(long createdAt) { this.createdAt = createdAt; }
    public long getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(long updatedAt) { this.updatedAt = updatedAt; }
    public long getNextRunAt() { return nextRunAt; }
    public void setNextRunAt(long nextRunAt) { this.nextRunAt = nextRunAt; }
    public long getExecutionEpoch() { return executionEpoch; }
    public void setExecutionEpoch(long executionEpoch) { this.executionEpoch = executionEpoch; }
    public String getLastFailureReason() { return lastFailureReason; }
    public void setLastFailureReason(String lastFailureReason) { this.lastFailureReason = lastFailureReason; }
    public FailureClassification getLastFailureClassification() { return lastFailureClassification; }
    public void setLastFailureClassification(FailureClassification classification) { this.lastFailureClassification = classification; }
}
