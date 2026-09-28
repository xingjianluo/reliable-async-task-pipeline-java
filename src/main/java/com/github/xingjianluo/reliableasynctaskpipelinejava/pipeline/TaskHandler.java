package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

/**
 * 业务任务处理器。实现必须保证幂等：同一幂等键的业务效果只应产生一次。
 */
@FunctionalInterface
public interface TaskHandler {

    /**
     * @throws TaskFailedException 业务失败，携带失败分类（可重试/不可重试）
     * @throws Exception 未分类异常，按可重试失败处理
     * @throws Error 严重错误（如 {@link OutOfMemoryError}）同样被流水线捕获，按可重试失败收敛：
     *               退避重试、次数耗尽后进入死信，错误类型与信息写入 failureReason；
     *               单条任务的严重错误不会杀死工作线程，也不会停止流水线消费后续任务。
     */
    void handle(Task task) throws Exception;
}
