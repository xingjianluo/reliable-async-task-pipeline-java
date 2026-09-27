package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

import java.util.List;

/**
 * 任务状态流转日志（WAL）抽象。
 *
 * <p>{@link TaskStore} 的每一次状态变更都先追加日志再改内存，保证强制结束进程后
 * 可以通过 {@link #replay()} 重放重建；启动时重放得到的 RUNNING 任务
 * （上次被中断的执行）会被重置为 PENDING 并 bump 租约纪元，旧执行的回写将被拒绝。
 *
 * <p>每个流转事件都携带租约纪元 epoch：只有持有当前 epoch 的执行者才能写入终态，
 * 这是“旧执行不能覆盖新结果”的 fencing 依据。
 */
public interface TaskJournal extends AutoCloseable {
    /** 空实现：纯内存模式（不持久化，行为与改造前一致） */
    TaskJournal NOOP = new TaskJournal() {
        @Override public void submitted(String key, String type, String payload, int maxAttempts) { }
        @Override public void started(String key, int attempts, int epoch) { }
        @Override public void succeeded(String key, int epoch) { }
        @Override public void retryPending(String key, int epoch, FailureKind kind, String reason, long nextAttemptAtMillis) { }
        @Override public void deadLetter(String key, int epoch, FailureKind kind, String reason) { }
        @Override public void reclaimed(String key, int attempts, int epoch, String reason) { }
        @Override public void removed(String key) { }
        @Override public List<Event> replay() { return List.of(); }
        @Override public void compact(List<Event> snapshot) { }
        @Override public void close() { }
    };

    void submitted(String key, String type, String payload, int maxAttempts);

    /** PENDING -> RUNNING，占用新的租约纪元 */
    void started(String key, int attempts, int epoch);

    /** RUNNING -> SUCCEEDED，终态 */
    void succeeded(String key, int epoch);

    /** RUNNING -> PENDING（可重试失败），记录退避后的下次执行时间 */
    void retryPending(String key, int epoch, FailureKind kind, String reason, long nextAttemptAtMillis);

    /** RUNNING -> DEAD_LETTER，终态 */
    void deadLetter(String key, int epoch, FailureKind kind, String reason);

    /** 崩溃恢复或停机回收：RUNNING -> PENDING 并 bump 纪元，旧租约立即失效 */
    void reclaimed(String key, int attempts, int epoch, String reason);

    /** 背压拒绝后移除未消费任务 */
    void removed(String key);

    /** 按追加顺序重放全部历史事件 */
    List<Event> replay();

    /** 用给定快照（每个键的完整最新状态）重写底层存储，回收历史空间 */
    void compact(List<Event> snapshot);

    /** 释放底层资源（文件句柄与锁） */
    @Override
    void close();

    /** 日志事件，按类型区分的状态流转记录 */
    sealed interface Event permits Event.Submitted, Event.Started, Event.Succeeded,
            Event.RetryPending, Event.DeadLetter, Event.Reclaimed, Event.Removed {

        String key();

        record Submitted(String key, String type, String payload, int maxAttempts) implements Event {}
        record Started(String key, int attempts, int epoch) implements Event {}
        record Succeeded(String key, int epoch) implements Event {}
        record RetryPending(String key, int epoch, FailureKind kind, String reason, long nextAttemptAtMillis) implements Event {}
        record DeadLetter(String key, int epoch, FailureKind kind, String reason) implements Event {}
        record Reclaimed(String key, int attempts, int epoch, String reason) implements Event {}
        record Removed(String key) implements Event {}
    }
}
