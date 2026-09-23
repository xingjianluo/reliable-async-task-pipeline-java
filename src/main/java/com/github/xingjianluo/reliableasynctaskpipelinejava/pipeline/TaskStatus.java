package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

/**
 * 任务生命周期状态。合法流转见 {@link StateTransition#isAllowed}。
 */
public enum TaskStatus {
    /** 已持久化，等待被工作线程领取。 */
    PENDING,
    /** 正被某个工作线程执行（同一幂等键任意时刻最多一个执行）。 */
    RUNNING,
    /** 业务成功完成，终态，业务效果只产生一次。 */
    SUCCEEDED,
    /** 一次可重试尝试失败，等待退避后重新入队。 */
    RETRYING,
    /** 重试耗尽或遇到不可重试失败，进入死信，终态。 */
    DEAD,
    /** 被取消，终态。 */
    CANCELED;

    /** 是否允许从当前状态流转到目标状态。 */
    public boolean canTransitTo(TaskStatus target) {
        return StateTransition.isAllowed(this, target);
    }
}
