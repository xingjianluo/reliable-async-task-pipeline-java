package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.persist;

import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.Task;

import java.util.List;
import java.util.Optional;

/**
 * 纯内存仓储：不落盘，所有条件写入恒确认成功（状态机与 epoch 校验由 TaskStore 负责）。
 *
 * <p>语义上等价于一个不会丢数据、也没有第二个进程竞争的存储，保持原有纯内存行为，
 * 便于不需要跨进程持久化的本地快速测试。
 */
public class InMemoryTaskRepository implements TaskRepository {

    @Override
    public Optional<Task> insertIfAbsent(Task task) {
        // TaskStore 的内存索引已完成去重，此处恒为新插入
        return Optional.empty();
    }

    @Override
    public Optional<Task> find(String idempotencyKey) {
        return Optional.empty();
    }

    @Override
    public List<Task> findAll() {
        return List.of();
    }

    @Override
    public long claimForRun(String idempotencyKey, long expectedEpoch) {
        return expectedEpoch + 1;
    }

    @Override
    public boolean completeSuccess(String idempotencyKey, long expectedEpoch) {
        return true;
    }

    @Override
    public boolean scheduleRetry(String idempotencyKey, long expectedEpoch,
                                 String failureKind, String failureReason, long nextAttemptAtMillis) {
        return true;
    }

    @Override
    public boolean moveToDeadLetter(String idempotencyKey, long expectedEpoch,
                                    String failureKind, String failureReason) {
        return true;
    }

    @Override
    public boolean deleteIfPending(String idempotencyKey) {
        return true;
    }
}
