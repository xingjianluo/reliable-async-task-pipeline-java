package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.persist;

import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.Task;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.TaskStatus;

import java.util.List;
import java.util.Optional;

/**
 * 任务持久化仓储：负责任务状态的落盘与重启后恢复。
 *
 * <p>所有带 {@code expectedEpoch} 的方法都是基于 fencing token（世代号）的 CAS 写入：
 * 只有存储中当前 epoch 与调用方持有的令牌一致时才更新，并把 epoch +1。
 * 旧执行（如重启前被中断、之后才醒来的线程）持有的过期令牌无法覆盖较新结果。
 */
public interface TaskRepository {

    /**
     * 幂等插入：键不存在则落盘并返回空；已存在则返回存储中的任务（可能来自上一个进程）。
     */
    Optional<Task> insertIfAbsent(Task task);

    /** 按键查询持久化任务 */
    Optional<Task> find(String idempotencyKey);

    /** 读取全部任务（启动恢复时加载到内存索引） */
    List<Task> findAll();

    /**
     * PENDING -> RUNNING：仅当当前状态为 PENDING 且 epoch 匹配时占用执行权，epoch +1。
     *
     * @return 新的 fencing token（更新后的 epoch）；未抢到返回 -1
     */
    long claimForRun(String idempotencyKey, long expectedEpoch);

    /**
     * RUNNING -> SUCCEEDED：仅当 epoch 匹配。过期执行调用返回 false，无法覆盖新结果。
     */
    boolean completeSuccess(String idempotencyKey, long expectedEpoch);

    /**
     * RUNNING -> PENDING：仅当 epoch 匹配，记录失败信息、退避时间，epoch +1。
     */
    boolean scheduleRetry(String idempotencyKey, long expectedEpoch,
                         String failureKind, String failureReason, long nextAttemptAtMillis);

    /** RUNNING -> DEAD_LETTER：仅当 epoch 匹配，epoch +1。 */
    boolean moveToDeadLetter(String idempotencyKey, long expectedEpoch,
                             String failureKind, String failureReason);

    /** 删除任务（仅背压拒绝尚未消费的 PENDING 时使用）；状态需仍为 PENDING */
    boolean deleteIfPending(String idempotencyKey);
}
