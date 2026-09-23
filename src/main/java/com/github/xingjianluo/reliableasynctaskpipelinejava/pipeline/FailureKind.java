package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

/**
 * 失败分类依据。
 */
public enum FailureKind {
    /** 可重试失败：按退避策略在有限次数内重试 */
    RETRYABLE,
    /** 不可重试失败：直接进入死信 */
    NON_RETRYABLE,
    /** 可重试但重试次数耗尽：进入死信 */
    RETRY_EXHAUSTED
}
