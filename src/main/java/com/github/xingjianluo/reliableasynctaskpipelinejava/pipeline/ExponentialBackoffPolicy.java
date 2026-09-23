package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

/**
 * 指数退避：{@code min(base * 2^(attempt-1), max)}，attempt 为刚刚失败的尝试序号（从 1 开始）。
 */
public class ExponentialBackoffPolicy implements BackoffPolicy {

    private final long baseMillis;
    private final long maxMillis;

    public ExponentialBackoffPolicy(long baseMillis, long maxMillis) {
        if (baseMillis <= 0) {
            throw new IllegalArgumentException("baseMillis must be positive: " + baseMillis);
        }
        this.baseMillis = baseMillis;
        this.maxMillis = Math.max(baseMillis, maxMillis);
    }

    @Override
    public long nextDelayMillis(int attempt) {
        int exp = Math.max(0, attempt - 1);
        // 位移前做上限保护，避免极大 attempt 导致溢出。
        long delay = exp >= 62 ? Long.MAX_VALUE : baseMillis << exp;
        if (delay < 0 || (maxMillis > 0 && delay > maxMillis)) {
            return maxMillis;
        }
        return delay;
    }
}
