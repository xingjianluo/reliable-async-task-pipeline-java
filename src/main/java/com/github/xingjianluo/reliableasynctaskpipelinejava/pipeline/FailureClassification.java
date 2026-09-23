package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

/**
 * 失败分类：决定任务是否可重试。
 */
public enum FailureClassification {
    RETRYABLE,
    NON_RETRYABLE
}
