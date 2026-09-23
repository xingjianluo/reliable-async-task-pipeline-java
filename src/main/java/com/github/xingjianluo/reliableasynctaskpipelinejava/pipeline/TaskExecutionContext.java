package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

/**
 * 单次执行尝试的上下文，传给业务处理器。
 */
public class TaskExecutionContext {
    private final String idempotencyKey;
    private final String payload;
    private final int attempt;

    public TaskExecutionContext(String idempotencyKey, String payload, int attempt) {
        this.idempotencyKey = idempotencyKey;
        this.payload = payload;
        this.attempt = attempt;
    }

    public String getIdempotencyKey() { return idempotencyKey; }
    public String getPayload() { return payload; }
    public int getAttempt() { return attempt; }
}
