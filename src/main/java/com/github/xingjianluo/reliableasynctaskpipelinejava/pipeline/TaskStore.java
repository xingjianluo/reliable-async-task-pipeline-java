package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 任务存储：负责幂等去重、租约纪元（fencing epoch）状态机与持久化联动。
 *
 * <p>所有状态变更按幂等键串行化（对 Task 实例加锁），并且<b>先写日志（WAL）再改内存</b>，
 * 保证：
 * <ul>
 *   <li>同一幂等键任一时刻至多一个执行者：{@code PENDING -> RUNNING} 只能由一个执行者获得；</li>
 *   <li>每次获得执行权都会递增租约纪元 epoch，终态回写必须携带匹配的 epoch，
 *       因此被中断/崩溃的旧执行即使稍后醒来，也无法覆盖已经往前走的新结果；</li>
 *   <li>构造时重放 {@link TaskJournal}，强制结束进程后已受理任务不丢失；
 *       重放末尾仍为 RUNNING 的任务（上次执行被杀死）被重置为 PENDING 并 bump 纪元。</li>
 * </ul>
 */
public class TaskStore {

    private static final Logger log = LoggerFactory.getLogger(TaskStore.class);

    private final ConcurrentHashMap<String, Task> tasks = new ConcurrentHashMap<>();
    private final TaskJournal journal;

    /** 纯内存模式：不持久化（行为与无 WAL 时一致） */
    public TaskStore() {
        this(TaskJournal.NOOP);
    }

    /**
     * 持久化模式：重放日志重建全部任务，并回收上次进程被强制结束时遗留的 RUNNING 任务。
     * 恢复完成后将当前状态压缩为快照，控制日志体积。
     */
    public TaskStore(TaskJournal journal) {
        this.journal = journal;
        long t0 = System.currentTimeMillis();
        int events = replay();
        int reclaimed = reclaimInterruptedTasks();
        if (events > 0 || reclaimed > 0) {
            journal.compact(snapshotEvents());
            log.info("[recover] 日志重放完成: 事件={} 任务={} 回收中断任务={} 耗时={}ms",
                    events, tasks.size(), reclaimed, System.currentTimeMillis() - t0);
        }
    }

    /** 重放历史事件，重建任务实体与最新状态 */
    private int replay() {
        int count = 0;
        for (TaskJournal.Event event : journal.replay()) {
            applyReplayed(event);
            count++;
        }
        return count;
    }

    private void applyReplayed(TaskJournal.Event e) {
        switch (e) {
            case TaskJournal.Event.Submitted s ->
                    tasks.put(s.key(), new Task(s.key(), s.type(), s.payload(), s.maxAttempts()));
            case TaskJournal.Event.Started s -> {
                Task t = tasks.get(s.key());
                if (t == null) {
                    break;
                }
                t.setAttempts(s.attempts());
                t.setLeaseEpoch(s.epoch());
                t.setStatus(TaskStatus.RUNNING);
            }
            case TaskJournal.Event.Succeeded s ->
                    applyTerminal(s.key(), s.epoch(), TaskStatus.SUCCEEDED, null, null);
            case TaskJournal.Event.RetryPending r -> {
                Task t = tasks.get(r.key());
                // 完整历史：RUNNING 且纪元匹配；压缩快照：已是 PENDING 的重试等待
                if (t != null && (t.getStatus() == TaskStatus.RUNNING || t.getStatus() == TaskStatus.PENDING)
                        && (t.getStatus() == TaskStatus.PENDING || t.getLeaseEpoch() == r.epoch())) {
                    t.setFailureKind(r.kind());
                    t.setFailureReason(r.reason());
                    t.setLeaseEpoch(r.epoch());
                    t.setNextAttemptAt(Instant.ofEpochMilli(r.nextAttemptAtMillis()));
                    t.setStatus(TaskStatus.PENDING);
                }
            }
            case TaskJournal.Event.DeadLetter d ->
                    applyTerminal(d.key(), d.epoch(), TaskStatus.DEAD_LETTER, d.kind(), d.reason());
            case TaskJournal.Event.Reclaimed r -> {
                Task t = tasks.get(r.key());
                if (t == null) {
                    break;
                }
                t.setAttempts(r.attempts());
                t.setLeaseEpoch(r.epoch());
                t.setFailureKind(FailureKind.RETRYABLE);
                t.setFailureReason(r.reason());
                // 回收（强制结束遗留）后立即可以重新执行
                t.setNextAttemptAt(Instant.now());
                t.setStatus(TaskStatus.PENDING);
            }
            case TaskJournal.Event.Removed r -> tasks.remove(r.key());
        }
    }

    /**
     * 重放终态事件（SUCCEEDED / DEAD_LETTER）。
     * 完整历史里它跟在同纪元 STARTED 之后（RUNNING + epoch 匹配）；
     * 压缩快照里它直接跟在 SUBMITTED 之后（PENDING + epoch=0），此时以事件自带纪元为准还原。
     */
    private void applyTerminal(String key, int epoch, TaskStatus terminal, FailureKind kind, String reason) {
        Task t = tasks.get(key);
        if (t == null) {
            return;
        }
        if (t.getStatus() == TaskStatus.RUNNING && t.getLeaseEpoch() != epoch) {
            return; // 完整历史中的旧纪元回写，忽略
        }
        if (t.getStatus() != TaskStatus.RUNNING && t.getStatus() != TaskStatus.PENDING) {
            return; // 已有终态，不覆盖
        }
        t.setAttempts(Math.max(t.getAttempts(), 1));
        t.setLeaseEpoch(epoch);
        if (kind != null) {
            t.setFailureKind(kind);
            t.setFailureReason(reason);
        }
        t.setStatus(terminal);
    }

    /**
     * 幂等提交：键不存在则创建（初始状态 PENDING），已存在则返回现有任务，不产生新的业务效果。
     * 日志追加与去重在哈希桶锁内原子完成：同一个键只会有一条 SUBMITTED 落盘。
     */
    public Task createIfAbsent(Task task) {
        Task stored = tasks.computeIfAbsent(task.getIdempotencyKey(), k -> {
            journal.submitted(k, task.getType(), task.getPayload(), task.getMaxAttempts());
            return task;
        });
        if (stored != task) {
            log.info("[dedup] idempotencyKey={} 重复提交被识别，返回现有任务 status={} attempts={} epoch={}",
                    task.getIdempotencyKey(), stored.getStatus(), stored.getAttempts(), stored.getLeaseEpoch());
            return stored;
        }
        log.info("[submit] idempotencyKey={} type={} maxAttempts={} 状态流转: <none> -> PENDING",
                task.getIdempotencyKey(), task.getType(), task.getMaxAttempts());
        return task;
    }

    public Optional<Task> find(String idempotencyKey) {
        return Optional.ofNullable(tasks.get(idempotencyKey));
    }

    /**
     * PENDING -> RUNNING，仅当到达可执行时间；成功则占用该键的唯一执行权并取得新租约纪元。
     *
     * @return 本次执行的租约纪元（&gt;0）；未获得执行权返回 0
     */
    public int tryStart(String idempotencyKey) {
        Task task = tasks.get(idempotencyKey);
        if (task == null) {
            return 0;
        }
        synchronized (task) {
            if (task.getStatus() != TaskStatus.PENDING || Instant.now().isBefore(task.getNextAttemptAt())) {
                return 0;
            }
            int epoch = task.getLeaseEpoch() + 1;
            int attempts = task.getAttempts() + 1;
            journal.started(idempotencyKey, attempts, epoch);
            task.setAttempts(attempts);
            task.setLeaseEpoch(epoch);
            task.setStatus(TaskStatus.RUNNING);
            log.info("[transition] idempotencyKey={} attempt={} epoch={} 状态流转: PENDING -> RUNNING",
                    idempotencyKey, attempts, epoch);
            return epoch;
        }
    }

    /** RUNNING(epoch) -> SUCCEEDED；epoch 不匹配（旧执行）一律拒绝 */
    public boolean markSucceeded(String idempotencyKey, int epoch) {
        Task task = tasks.get(idempotencyKey);
        if (task == null) {
            return false;
        }
        synchronized (task) {
            if (!currentRunning(task, epoch)) {
                return rejectStaleWrite(idempotencyKey, epoch, task, "SUCCEEDED");
            }
            journal.succeeded(idempotencyKey, epoch);
            task.setStatus(TaskStatus.SUCCEEDED);
            log.info("[transition] idempotencyKey={} attempt={} epoch={} 状态流转: RUNNING -> SUCCEEDED",
                    idempotencyKey, task.getAttempts(), epoch);
            return true;
        }
    }

    /** RUNNING(epoch) -> PENDING（可重试失败），记录退避后的下次执行时间；epoch 不匹配则拒绝 */
    public boolean markRetryPending(String idempotencyKey, int epoch, FailureKind kind,
                                    String reason, long backoffMillis) {
        Task task = tasks.get(idempotencyKey);
        if (task == null) {
            return false;
        }
        synchronized (task) {
            if (!currentRunning(task, epoch)) {
                return rejectStaleWrite(idempotencyKey, epoch, task, "PENDING(retry)");
            }
            Instant nextAttemptAt = Instant.now().plusMillis(backoffMillis);
            journal.retryPending(idempotencyKey, epoch, kind, reason, nextAttemptAt.toEpochMilli());
            task.setFailureKind(kind);
            task.setFailureReason(reason);
            task.setNextAttemptAt(nextAttemptAt);
            task.setStatus(TaskStatus.PENDING);
            log.info("[transition] idempotencyKey={} attempt={} epoch={} 状态流转: RUNNING -> PENDING, 失败分类={}, 原因={}, 退避={}ms",
                    idempotencyKey, task.getAttempts(), epoch, kind, reason, backoffMillis);
            return true;
        }
    }

    /** RUNNING(epoch) -> DEAD_LETTER，保留可区分的失败原因；epoch 不匹配则拒绝 */
    public boolean markDeadLetter(String idempotencyKey, int epoch, FailureKind kind, String reason) {
        Task task = tasks.get(idempotencyKey);
        if (task == null) {
            return false;
        }
        synchronized (task) {
            if (!currentRunning(task, epoch)) {
                return rejectStaleWrite(idempotencyKey, epoch, task, "DEAD_LETTER");
            }
            journal.deadLetter(idempotencyKey, epoch, kind, reason);
            task.setFailureKind(kind);
            task.setFailureReason(reason);
            task.setStatus(TaskStatus.DEAD_LETTER);
            log.info("[transition] idempotencyKey={} attempt={} epoch={} 状态流转: RUNNING -> DEAD_LETTER, 失败分类={}, 原因={}",
                    idempotencyKey, task.getAttempts(), epoch, kind, reason);
            return true;
        }
    }

    private static boolean currentRunning(Task task, int epoch) {
        return task.getStatus() == TaskStatus.RUNNING && task.getLeaseEpoch() == epoch;
    }

    private boolean rejectStaleWrite(String key, int epoch, Task current, String wanted) {
        log.warn("[fence] idempotencyKey={} 旧执行(epoch={})的终态回写({})被拒绝，当前 status={} epoch={}，旧执行不得覆盖新状态",
                key, epoch, wanted, current.getStatus(), current.getLeaseEpoch());
        return false;
    }

    /**
     * 回收被中断的执行：RUNNING -> PENDING 并 bump 租约纪元，旧租约立即失效。
     * 用于：启动重放（进程被强制结束的遗留任务）、优雅停机回收、测试模拟崩溃。
     *
     * @return 被回收的任务数
     */
    public int reclaimInterruptedTasks() {
        int reclaimed = 0;
        for (Task task : List.copyOf(tasks.values())) {
            synchronized (task) {
                if (task.getStatus() != TaskStatus.RUNNING) {
                    continue;
                }
                int epoch = task.getLeaseEpoch() + 1;
                String reason = "reclaimed: previous execution was interrupted";
                journal.reclaimed(task.getIdempotencyKey(), task.getAttempts(), epoch, reason);
                task.setLeaseEpoch(epoch);
                task.setFailureKind(FailureKind.RETRYABLE);
                task.setFailureReason(reason);
                task.setNextAttemptAt(Instant.now());
                task.setStatus(TaskStatus.PENDING);
                reclaimed++;
                log.warn("[reclaim] idempotencyKey={} attempt={} 中断执行被回收: RUNNING -> PENDING, epoch={} -> {}，旧租约失效",
                        task.getIdempotencyKey(), task.getAttempts(), epoch - 1, epoch);
            }
        }
        return reclaimed;
    }

    /** 背压拒绝时移除尚未被消费的 PENDING 任务；若已被执行线程占用则不移除 */
    public boolean removeIfPending(String idempotencyKey) {
        Task task = tasks.get(idempotencyKey);
        if (task == null) {
            return false;
        }
        synchronized (task) {
            if (task.getStatus() != TaskStatus.PENDING) {
                return false;
            }
            journal.removed(idempotencyKey);
            tasks.remove(idempotencyKey, task);
            log.info("[reject] idempotencyKey={} 背压拒绝，移除未消费的 PENDING 任务", idempotencyKey);
            return true;
        }
    }

    /** 所有 PENDING 任务的幂等键，用于调度恢复 */
    public List<String> pendingKeys() {
        List<String> keys = new ArrayList<>();
        for (Task task : tasks.values()) {
            if (task.getStatus() == TaskStatus.PENDING) {
                keys.add(task.getIdempotencyKey());
            }
        }
        return keys;
    }

    /** 当前积压（PENDING）任务数，用于背压判断 */
    public int backlog() {
        int count = 0;
        for (Task task : tasks.values()) {
            if (task.getStatus() == TaskStatus.PENDING) {
                count++;
            }
        }
        return count;
    }

    /**
     * 当前全部任务的最小等价快照：每个键一条 SUBMITTED（创建参数）+ 一条最新状态事件。
     * 重放该快照可得到与当前一致的任务集合、状态、attempts 与 epoch。
     */
    private List<TaskJournal.Event> snapshotEvents() {
        List<TaskJournal.Event> events = new ArrayList<>();
        for (Task t : List.copyOf(tasks.values())) {
            events.add(new TaskJournal.Event.Submitted(
                    t.getIdempotencyKey(), t.getType(), t.getPayload(), t.getMaxAttempts()));
            switch (t.getStatus()) {
                case PENDING -> {
                    if (t.getAttempts() > 0) {
                        // 执行过又回到 PENDING 等待退避：用 RETRY_PENDING 快照保留下次执行时间
                        events.add(new TaskJournal.Event.RetryPending(t.getIdempotencyKey(),
                                t.getLeaseEpoch(), FailureKind.RETRYABLE, t.getFailureReason(),
                                t.getNextAttemptAt().toEpochMilli()));
                    }
                }
                case RUNNING -> events.add(new TaskJournal.Event.Started(
                        t.getIdempotencyKey(), t.getAttempts(), t.getLeaseEpoch()));
                case SUCCEEDED -> events.add(new TaskJournal.Event.Succeeded(
                        t.getIdempotencyKey(), t.getLeaseEpoch()));
                case DEAD_LETTER -> events.add(new TaskJournal.Event.DeadLetter(t.getIdempotencyKey(),
                        t.getLeaseEpoch(), t.getFailureKind(), t.getFailureReason()));
            }
        }
        return events;
    }
}
