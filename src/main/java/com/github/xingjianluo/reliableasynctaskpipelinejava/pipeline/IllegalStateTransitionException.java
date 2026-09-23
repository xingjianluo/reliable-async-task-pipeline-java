package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

/**
 * 发生非法状态回退 / 跳转时抛出，说明代码存在并发缺陷，不应在正常流程中吞掉。
 */
public class IllegalStateTransitionException extends IllegalStateException {

    public IllegalStateTransitionException(String idempotencyKey, TaskStatus from, TaskStatus to) {
        super("illegal state transition for task [" + idempotencyKey + "]: " + from + " -> " + to);
    }
}
