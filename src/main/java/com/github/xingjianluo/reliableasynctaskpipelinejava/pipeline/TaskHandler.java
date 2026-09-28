package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

/**
 * 业务任务处理器。实现必须保证幂等：同一幂等键的业务效果只应产生一次。
 */
@FunctionalInterface
public interface TaskHandler {

    /**
     * @throws TaskFailedException 业务失败，携带失败分类（可重试/不可重试）
     * @throws Exception 未分类异常，按可重试失败处理
     * @throws Error 严重错误（如 OutOfMemoryError）也会被流水线兜底捕获，按可重试失败处理：
     *               走退避重试，重试次数耗尽进入死信；任务不会卡在运行中，流水线也不会因此停止消费。
     */
    void handle(Task task) throws Exception;
}
