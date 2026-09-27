package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

/**
 * 提交结果：让调用方明确区分“新受理”与“重复投递命中已有任务”。
 *
 * @param task      任务实体（新任务或已存在的同键任务，二者必为同一实例）
 * @param duplicate true 表示这是一次重复投递，返回的是原来那条任务；
 *                  false 表示本次请求首次受理了该键
 */
public record SubmitOutcome(Task task, boolean duplicate) {

    static SubmitOutcome accepted(Task task) {
        return new SubmitOutcome(task, false);
    }

    static SubmitOutcome duplicate(Task task) {
        return new SubmitOutcome(task, true);
    }
}
