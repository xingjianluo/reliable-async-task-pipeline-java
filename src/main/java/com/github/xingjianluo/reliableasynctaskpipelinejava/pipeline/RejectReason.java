package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

/**
 * 提交被拒绝的原因（与业务失败明确区分）。
 */
public enum RejectReason {
    SHUTTING_DOWN,
    QUEUE_FULL
}
