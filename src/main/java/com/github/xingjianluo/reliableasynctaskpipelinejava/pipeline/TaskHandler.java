package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

/**
 * 业务任务处理器，由使用方实现。抛出 NonRetryableTaskException 表示不可重试，
 * 其它异常视为可重试失败。
 */
@FunctionalInterface
public interface TaskHandler {
    void handle(TaskExecutionContext context) throws Exception;
}
