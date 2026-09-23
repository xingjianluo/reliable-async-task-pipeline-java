package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

/**
 * 任务状态。合法流转路径：
 * PENDING -> RUNNING -> SUCCEEDED
 * PENDING -> RUNNING -> PENDING（可重试失败或停机回收）
 * PENDING -> RUNNING -> DEAD_LETTER（不可重试失败或重试耗尽）
 */
public enum TaskStatus {
    PENDING,
    RUNNING,
    SUCCEEDED,
    DEAD_LETTER
}
