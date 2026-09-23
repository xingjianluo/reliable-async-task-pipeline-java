package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

/**
 * 业务任务处理器。实现必须保证幂等：同一幂等键的业务效果只应产生一次。
 */
@FunctionalInterface
public interface TaskHandler {

    /**
     * @throws TaskFailedException 业务失败，携带失败分类（可重试/不可重试）
     * @throws Exception 未分类异常，按可重试失败处理
     */
    void handle(Task task) throws Exception;
}
