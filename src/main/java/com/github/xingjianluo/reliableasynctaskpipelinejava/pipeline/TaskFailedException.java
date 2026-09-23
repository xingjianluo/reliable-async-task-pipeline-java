package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

/**
 * 任务处理器抛出的业务失败，携带失败分类。
 */
public class TaskFailedException extends Exception {

    private final FailureKind failureKind;

    public TaskFailedException(FailureKind failureKind, String message) {
        super(message);
        this.failureKind = failureKind;
    }

    public TaskFailedException(FailureKind failureKind, String message, Throwable cause) {
        super(message, cause);
        this.failureKind = failureKind;
    }

    public FailureKind getFailureKind() {
        return failureKind;
    }
}
