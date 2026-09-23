package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 本地异步任务流水线。
 *
 * <p>语义要点：</p>
 * <ul>
 *   <li><b>幂等</b>：提交以幂等键去重，重复提交返回同一记录，业务效果最多一次；</li>
 *   <li><b>串行</b>：同一幂等键任意时刻最多一个执行实例（领取时 PENDING->RUNNING 的 CAS + 内存去重）；</li>
 *   <li><b>状态机</b>：所有流转经 {@link StateTransition} 校验，非法回退抛
 *       {@link IllegalStateTransitionException}；</li>
 *   <li><b>重试/死信</b>：可重试失败按指数退避在 maxAttempts 内重试，超限或不可重试进 DEAD，
 *       并保留可区分的失败原因与分类；</li>
 *   <li><b>背压</b>：就绪队列满时提交被拒绝，拒绝原因 {@link RejectReason#QUEUE_FULL}
 *       与业务失败明确区分；</li>
 *   <li><b>优雅停机</b>：先停止接单，等待在途任务完成或退避；超时则把仍在 RUNNING 的任务
 *       原子转为 PENDING 并持久化，重启后恢复，不丢任务。</li>
 * </ul>
 */
public class TaskPipelineService {

    private static final Logger log = LoggerFactory.getLogger(TaskPipelineService.class);

    private final PipelineConfig config;
    private final TaskHandler handler;
    private final FailureClassifier classifier;
    private final BackoffPolicy backoff;
    private final TaskStore store;

    /** 就绪待执行队列（仅放幂等键），容量即背压阈值。 */
    private final BlockingQueue<String> readyQueue;
    /** 已被领取、正在执行的幂等键，保证同一键不被并发执行。 */
    private final Set<String> inFlight = ConcurrentHashMap.newKeySet();

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean shutdownInitiated = new AtomicBoolean(false);
    private volatile ExecutorService workers;
    private volatile Thread scheduler;

    public TaskPipelineService(PipelineConfig config, TaskHandler handler) {
        this(config, handler, new DefaultFailureClassifier(),
                new ExponentialBackoffPolicy(config.getBackoffBaseMillis(), config.getBackoffMaxMillis()),
                new JsonFileTaskStore(config.getStoreDir()));
    }

    public TaskPipelineService(PipelineConfig config, TaskHandler handler,
                               FailureClassifier classifier, BackoffPolicy backoff, TaskStore store) {
        this.config = config;
        this.handler = handler;
        this.classifier = classifier;
        this.backoff = backoff;
        this.store = store;
        this.readyQueue = new ArrayBlockingQueue<>(Math.max(1, config.getQueueCapacity()));
    }

    /**
     * 启动工作线程、重试调度器，并恢复上一进程中断遗留的任务。
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        shutdownInitiated.set(false);

        // 恢复：上一进程在 RUNNING 中崩溃/被 kill 的任务安全转为可重试。
        List<String> recovered = store.resetRunningToPending("recovered after abnormal termination");
        for (String key : recovered) {
            log.info("idempotencyKey={} recovered interrupted task to PENDING on startup", key);
        }
        // PENDING / RETRYING（退避已过）的任务重新入队。
        long now = System.currentTimeMillis();
        for (TaskRecord record : store.findAll()) {
            if (record.getStatus() == TaskStatus.PENDING) {
                enqueueDropping(record.getIdempotencyKey(), "startup-recovery");
            } else if (record.getStatus() == TaskStatus.RETRYING && record.getNextRunAt() <= now) {
                if (store.compareAndUpdateStatus(record.getIdempotencyKey(),
                        TaskStatus.RETRYING, TaskStatus.PENDING)) {
                    enqueueDropping(record.getIdempotencyKey(), "startup-recovery-retry-due");
                }
            }
        }

        int n = Math.max(1, config.getWorkerThreads());
        workers = Executors.newFixedThreadPool(n, r -> {
            Thread t = new Thread(r, "task-pipeline-worker");
            t.setDaemon(true);
            return t;
        });
        for (int i = 0; i < n; i++) {
            workers.submit(this::workerLoop);
        }

        scheduler = new Thread(this::retryScheduleLoop, "task-pipeline-retry-scheduler");
        scheduler.setDaemon(true);
        scheduler.start();

        log.info("task pipeline started: workers={}, queueCapacity={}, maxAttempts={}",
                n, config.getQueueCapacity(), config.getMaxAttempts());
    }

    /**
     * 提交任务。
     *
     * @return accepted=首次接受；duplicate=幂等键已存在（可查询其当前状态）；
     *         rejected=停机或背压拒绝（拒绝原因与业务失败区分）。
     */
    public SubmissionResult submit(String idempotencyKey, String payload) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey must not be blank");
        }
        if (shutdownInitiated.get()) {
            log.warn("idempotencyKey={} submission rejected: SHUTTING_DOWN", idempotencyKey);
            return SubmissionResult.rejected(RejectReason.SHUTTING_DOWN);
        }

        TaskRecord record = new TaskRecord();
        record.setIdempotencyKey(idempotencyKey);
        record.setPayload(payload);
        record.setStatus(TaskStatus.PENDING);
        record.setAttempts(0);
        long now = System.currentTimeMillis();
        record.setCreatedAt(now);
        record.setUpdatedAt(now);
        record.setNextRunAt(0L);

        // 先持久化占用幂等键，再入队；入队失败则删除记录并拒绝，避免孤儿记录。
        TaskRecord existing;
        synchronized (this) {
            existing = store.putIfAbsent(record);
            if (existing == null) {
                if (!readyQueue.offer(idempotencyKey)) {
                    // 背压：队列积压达到阈值。回滚刚插入的记录，保持“拒绝即不存在”。
                    store.removeIfFreshPending(idempotencyKey);
                    log.warn("idempotencyKey={} submission rejected: QUEUE_FULL (backlog={})",
                            idempotencyKey, readyQueue.size());
                    return SubmissionResult.rejected(RejectReason.QUEUE_FULL);
                }
            }
        }

        if (existing != null) {
            log.info("idempotencyKey={} duplicate submission recognized, currentStatus={}, attempts={}",
                    idempotencyKey, existing.getStatus(), existing.getAttempts());
            return SubmissionResult.duplicate(existing);
        }
        log.info("idempotencyKey={} submitted and persisted as PENDING (backlog={})",
                idempotencyKey, readyQueue.size());
        return SubmissionResult.accepted(store.find(idempotencyKey));
    }

    public TaskRecord getStatus(String idempotencyKey) {
        return store.find(idempotencyKey);
    }

    /**
     * 优雅停机：停止接单 -> 等待在途任务完成或退避 -> 超时把未完成者安全转为 PENDING 并持久化。
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        shutdownInitiated.set(true);
        log.info("task pipeline shutting down: no new submissions accepted");

        if (scheduler != null) {
            scheduler.interrupt();
        }
        if (workers != null) {
            workers.shutdown();
            try {
                boolean finished = workers.awaitTermination(config.getShutdownWaitMillis(), TimeUnit.MILLISECONDS);
                if (!finished) {
                    // 超时仍在 RUNNING 的任务：原子转 PENDING 并持久化，再中断工作线程。
                    List<String> stuck = store.resetRunningToPending(
                            "interrupted during graceful shutdown, marked retryable");
                    log.warn("graceful shutdown timeout ({}ms), {} in-flight tasks reset to PENDING: {}",
                            config.getShutdownWaitMillis(), stuck.size(), stuck);
                    workers.shutdownNow();
                    workers.awaitTermination(2, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                workers.shutdownNow();
            }
        }
        log.info("task pipeline stopped");
    }

    // ---- 内部实现 -------------------------------------------------------

    private void workerLoop() {
        while (!shutdownInitiated.get() || !readyQueue.isEmpty()) {
            String key = null;
            try {
                key = readyQueue.poll(200, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (key != null) {
                executeOnce(key);
            }
        }
    }

    private void executeOnce(String key) {
        // 领取：PENDING -> RUNNING 的 CAS 保证同一键不被并发执行。
        if (!inFlight.add(key)) {
            // 内存层面重复（理论上队列去重已避免），不做处理。
            return;
        }
        try {
            // 原子认领 PENDING->RUNNING 并取得执行代次令牌；同一键任意时刻最多一个有效执行。
            TaskRecord runningRecord = store.claimForExecution(key);
            if (runningRecord == null) {
                TaskRecord current = store.find(key);
                log.info("idempotencyKey={} skipped at claim, status={}", key,
                        current == null ? "MISSING" : current.getStatus());
                return;
            }
            long epoch = runningRecord.getExecutionEpoch();
            int attempt = runningRecord.getAttempts() + 1;
            log.info("idempotencyKey={} state PENDING->RUNNING, attempt={}, epoch={}", key, attempt, epoch);

            TaskExecutionContext context = new TaskExecutionContext(key, runningRecord.getPayload(), attempt);
            try {
                handler.handle(context);
                onSuccess(key, epoch, attempt);
            } catch (Throwable t) {
                onFailure(key, epoch, attempt, t);
            }
        } finally {
            inFlight.remove(key);
        }
    }

    private void onSuccess(String key, long epoch, int attempt) {
        if (store.recordAttemptResult(key, epoch, TaskStatus.SUCCEEDED, attempt, null, null, 0L)) {
            log.info("idempotencyKey={} state RUNNING->SUCCEEDED, attempt={}, epoch={} "
                    + "(business effect applied once)", key, attempt, epoch);
        } else {
            log.warn("idempotencyKey={} success result discarded: stale epoch {} or no longer RUNNING "
                    + "(likely reset by shutdown), attempt={}", key, epoch, attempt);
        }
    }

    private void onFailure(String key, long epoch, int attempt, Throwable error) {
        FailureClassification classification = classifier.classify(error);
        String reason = describe(error);
        boolean attemptsExhausted = attempt >= Math.max(1, config.getMaxAttempts());

        if (classification == FailureClassification.NON_RETRYABLE || attemptsExhausted) {
            String categorized;
            if (classification == FailureClassification.NON_RETRYABLE) {
                categorized = "NON_RETRYABLE";
            } else {
                categorized = "RETRY_LIMIT_EXHAUSTED";
            }
            String deadReason = categorized + ": " + reason;
            if (store.recordAttemptResult(key, epoch, TaskStatus.DEAD, attempt,
                    deadReason, classification, 0L)) {
                log.error("idempotencyKey={} state RUNNING->DEAD, attempts={}, epoch={}, "
                                + "classificationBasis={}, reason={}",
                        key, attempt, epoch, categorized, reason);
            } else {
                log.warn("idempotencyKey={} dead result discarded: stale epoch {} or no longer RUNNING, "
                        + "attempts={}", key, epoch, attempt);
            }
            return;
        }

        long delay = backoff.nextDelayMillis(attempt);
        long nextRunAt = System.currentTimeMillis() + delay;
        if (store.recordAttemptResult(key, epoch, TaskStatus.RETRYING, attempt,
                "RETRYABLE: " + reason, FailureClassification.RETRYABLE, nextRunAt)) {
            log.warn("idempotencyKey={} state RUNNING->RETRYING, attempts={}, epoch={}, "
                            + "classificationBasis=RETRYABLE, backoffMs={}, nextRunAt={}, reason={}",
                    key, attempt, epoch, delay, nextRunAt, reason);
        } else {
            log.warn("idempotencyKey={} retry result discarded: stale epoch {} or no longer RUNNING, "
                    + "attempts={}", key, epoch, attempt);
        }
    }

    /**
     * 扫描 RETRYING 到期任务重新入队；同时兜底保证所有 PENDING 都在队列或执行中，
     * 避免启动恢复时积压超过队列容量而产生未入队的孤儿任务（重复队列项由认领 CAS 安全跳过）。
     */
    private void retryScheduleLoop() {
        while (running.get()) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            long now = System.currentTimeMillis();
            for (TaskRecord record : store.findAll()) {
                if (record.getStatus() == TaskStatus.RETRYING && record.getNextRunAt() <= now) {
                    if (store.compareAndUpdateStatus(record.getIdempotencyKey(),
                            TaskStatus.RETRYING, TaskStatus.PENDING)) {
                        log.info("idempotencyKey={} state RETRYING->PENDING, backoff elapsed, re-enqueued",
                                record.getIdempotencyKey());
                        enqueueDropping(record.getIdempotencyKey(), "retry-due");
                    }
                } else if (record.getStatus() == TaskStatus.PENDING
                        && !inFlight.contains(record.getIdempotencyKey())
                        && !readyQueue.contains(record.getIdempotencyKey())) {
                    enqueueDropping(record.getIdempotencyKey(), "pending-guard");
                }
            }
        }
    }

    private void enqueueDropping(String key, String source) {
        if (readyQueue.offer(key)) {
            return;
        }
        // 仅在恢复路径可能出现（停机中的队列拒绝等）：退避后由调度器再次尝试，不丢任务。
        log.warn("idempotencyKey={} enqueue deferred (queue full) from {}", key, source);
    }

    private static String describe(Throwable t) {
        String message = t.getMessage();
        String type = t.getClass().getName();
        return type + (message == null ? "" : (": " + message));
    }
}
