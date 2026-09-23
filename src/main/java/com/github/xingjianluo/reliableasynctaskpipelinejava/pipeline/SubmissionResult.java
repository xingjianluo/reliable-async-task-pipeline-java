package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

/**
 * 提交结果：区分新任务被接受、重复幂等键命中、被背压拒绝。
 */
public class SubmissionResult {
    private final boolean accepted;
    private final boolean duplicate;
    private final TaskRecord task;
    private final RejectReason rejectReason;

    private SubmissionResult(boolean accepted, boolean duplicate, TaskRecord task, RejectReason rejectReason) {
        this.accepted = accepted;
        this.duplicate = duplicate;
        this.task = task;
        this.rejectReason = rejectReason;
    }

    public static SubmissionResult accepted(TaskRecord task) {
        return new SubmissionResult(true, false, task, null);
    }

    public static SubmissionResult duplicate(TaskRecord task) {
        return new SubmissionResult(false, true, task, null);
    }

    public static SubmissionResult rejected(RejectReason reason) {
        return new SubmissionResult(false, false, null, reason);
    }

    public boolean isAccepted() { return accepted; }
    public boolean isDuplicate() { return duplicate; }
    public TaskRecord getTask() { return task; }
    public RejectReason getRejectReason() { return rejectReason; }
}
