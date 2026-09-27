package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

/**
 * 提交结果：显式区分“新受理”与“重复投递命中已有任务”。
 *
 * <p>调用方不应再通过任务状态/尝试次数猜测，哪怕原任务仍排队未执行（attempts=0），
 * 只要幂等键已存在，{@link #duplicate()} 即为 true。
 */
public record SubmitResult(Task task, boolean duplicate) {

    public static SubmitResult accepted(Task task) {
        return new SubmitResult(task, false);
    }

    public static SubmitResult duplicated(Task task) {
        return new SubmitResult(task, true);
    }
}
