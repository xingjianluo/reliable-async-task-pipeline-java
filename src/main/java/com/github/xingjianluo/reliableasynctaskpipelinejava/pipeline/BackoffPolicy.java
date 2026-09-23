package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

/**
 * 重试退避策略：根据当前已尝试次数计算下一次执行前的等待毫秒数。
 */
@FunctionalInterface
public interface BackoffPolicy {
    long nextDelayMillis(int attempt);
}
