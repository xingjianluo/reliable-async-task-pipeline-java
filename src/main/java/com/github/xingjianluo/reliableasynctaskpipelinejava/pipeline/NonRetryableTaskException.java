package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

/**
 * 业务处理器抛出表示该失败不可重试，任务直接进入死信。
 */
public class NonRetryableTaskException extends RuntimeException {
    public NonRetryableTaskException(String message) {
        super(message);
    }

    public NonRetryableTaskException(String message, Throwable cause) {
        super(message, cause);
    }
}
