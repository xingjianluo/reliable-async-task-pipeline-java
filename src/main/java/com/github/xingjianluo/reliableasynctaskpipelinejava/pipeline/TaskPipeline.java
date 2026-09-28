package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.*;

/**
 * 本地异步任务流水线：提交、幂等去重、并发执行、失败重试、死信、持久化恢复与优雅停机。
 *
 * <p>语义：同一幂等键的任务至多被并发执行一次（由 {@link TaskStore} 状态机保证），
 * 业务效果至多成功提交一次（RUNNING -> SUCCEEDED 仅允许一次流转，且由 fencing token 防旧执行覆盖）。
 * 任务状态由 {@link com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.persist.TaskRepository}
 * 持久化：进程被直接杀死后重启，已受理未完成任务自动恢复；已成功任务不会被再跑一遍。
 */
public class TaskPipeline implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TaskPipeline.class);
    private static final long MAX_BACKOFF_MILLIS = 30_000;

    private final TaskStore store;
    private final PipelineProperties properties;
    private final ConcurrentHashMap<String, TaskHandler> handlers = new ConcurrentHashMap<>();

    private final BlockingQueue<String> readyQueue = new LinkedBlockingQueue<>();
    private final ExecutorService workers;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> daemon(r, "pipeline-retry-scheduler"));
    /** 在途任务 -> (执行线程, 领取执行权时拿到的 fencing token)，用于停机与旧执行判定 */
    private final ConcurrentHashMap<String, InFlight> inFlight = new ConcurrentHashMap<>();
    private volatile boolean closing = false;

    private record InFlight(Thread thread, long fence) {}

    public TaskPipeline(TaskStore store, PipelineProperties properties) {
        this.store = store;
        this.properties = properties;
        // 恢复（强杀/崩溃重启）：先把遗留在 RUNNING 的任务回收为 PENDING 并 bump epoch，
        // 使可能醒来的旧执行令牌失效；再接管所有可执行的 PENDING 任务。
        java.util.List<String> reclaimed = store.reclaimInterrupted();
        for (String key : reclaimed) {
            log.info("[recover] idempotencyKey={} 上次进程被中断的任务已回收，重新入队", key);
            readyQueue.offer(key);
        }
        this.workers = Executors.newFixedThreadPool(properties.getConcurrency(),
                r -> daemon(r, "pipeline-worker"));
        for (int i = 0; i < properties.getConcurrency(); i++) {
            workers.submit(this::workerLoop);
        }
        for (String key : store.pendingKeys()) {
            if (!reclaimed.contains(key)) {
                log.info("[recover] idempotencyKey={} 恢复遗留 PENDING 任务，重新入队", key);
            }
            // 未到退避时间的任务由 worker 自行按 nextAttemptAt 延迟调度
            readyQueue.offer(key);
        }
    }

    private static Thread daemon(Runnable r, String name) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }

    /** 注册某类型任务的处理器 */
    public void registerHandler(String type, TaskHandler handler) {
        handlers.put(type, handler);
    }

    /**
     * 幂等提交任务。
     *
     * @return {@link SubmitResult}：新受理 {@code duplicate=false}；重复投递命中已有任务
     *         （哪怕该任务仍在排队、attempts=0）{@code duplicate=true}。不产生新的业务效果。
     * @throws QueueFullException     积压超过阈值，背压拒绝（与业务失败可区分）
     * @throws IllegalStateException  流水线已关闭
     */
    public SubmitResult submit(String idempotencyKey, String type, String payload, Integer maxAttempts) {
        if (closing) {
            throw new IllegalStateException("pipeline is closing, submission rejected");
        }
        if (!handlers.containsKey(type)) {
            throw new IllegalArgumentException("no handler registered for type: " + type);
        }
        Task task = new Task(idempotencyKey, type, payload,
                maxAttempts != null ? maxAttempts : properties.getDefaultMaxAttempts());
        Task stored = store.createIfAbsent(task);
        if (stored != task) {
            return SubmitResult.duplicated(stored); // 重复投递：精确命中已有任务
        }
        if (store.backlog() > properties.getMaxBacklog()) {
            // 背压拒绝：任务从队列与持久化中移除，与业务失败可区分
            store.removeIfPending(idempotencyKey);
            throw new QueueFullException("backlog exceeds maxBacklog=" + properties.getMaxBacklog()
                    + ", submission rejected for idempotencyKey=" + idempotencyKey);
        }
        readyQueue.offer(idempotencyKey);
        return SubmitResult.accepted(stored);
    }

    public Optional<Task> query(String idempotencyKey) {
        return store.find(idempotencyKey);
    }

    private void workerLoop() {
        while (true) {
            String key;
            try {
                key = readyQueue.poll(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                return; // 停机中断
            }
            if (key == null) {
                if (closing) {
                    return;
                }
                continue;
            }
            try {
                dispatch(key);
            } catch (Throwable t) {
                // 兜底：任何漏网的严重错误（Error）都不得杀死工作线程。
                // 工作线程一旦死亡，线程池不会再提交新的 workerLoop，流水线会静默停止消费。
                log.error("[worker] idempotencyKey={} 处理过程发生未预期的严重错误，工作线程保持运行: {}",
                        key, t.toString(), t);
            }
        }
    }

    private void dispatch(String key) {
        long fence = store.tryStart(key);
        if (fence < 0) {
            // 未获得执行权：重复入队、未到执行时间或已终态。若仍是 PENDING（未到时间），重新调度
            store.find(key).filter(t -> t.getStatus() == TaskStatus.PENDING)
                    .ifPresent(t -> scheduleRetry(key, delayUntil(t)));
            return;
        }
        inFlight.put(key, new InFlight(Thread.currentThread(), fence));
        try {
            execute(key, fence);
        } finally {
            inFlight.remove(key);
        }
    }

    private static long delayUntil(Task t) {
        return Math.max(0, t.getNextAttemptAt().toEpochMilli() - System.currentTimeMillis());
    }

    private void execute(String key, long fence) {
        Task task = store.find(key).orElseThrow();
        TaskHandler handler = handlers.get(task.getType());
        try {
            handler.handle(task);
            if (!store.markSucceeded(key, fence)) {
                // 本执行持有过期令牌（重启恢复后已有更新的执行者接管）：放弃写回，不产生成功提交
                log.warn("[fence] idempotencyKey={} epoch={} 执行完成但令牌已过期，不提交业务成功结果",
                        key, fence);
            }
        } catch (TaskFailedException e) {
            onFailure(task, fence, e.getFailureKind(), e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            onFailure(task, fence, FailureKind.RETRYABLE, "interrupted (shutdown): " + e.getMessage());
        } catch (Exception e) {
            // 未分类异常按可重试失败处理
            onFailure(task, fence, FailureKind.RETRYABLE, e.getClass().getSimpleName() + ": " + e.getMessage());
        } catch (Throwable t) {
            // 严重错误（Error 级别，如 OutOfMemoryError / StackOverflowError）：同样按失败语义
            // 收敛——记为可重试失败走退避重试，次数耗尽进死信，绝不让任务卡在 RUNNING，
            // 也绝不让错误逃出工作线程导致整个流水线停止消费。
            log.error("[severe] idempotencyKey={} attempt={} 处理器抛出严重错误，按可重试失败收敛: {}",
                    key, task.getAttempts(), t.toString(), t);
            onFailure(task, fence, FailureKind.RETRYABLE,
                    "severe error " + t.getClass().getSimpleName() + ": " + t.getMessage());
        }
    }

    private void onFailure(Task task, long fence, FailureKind kind, String reason) {
        String key = task.getIdempotencyKey();
        int attempt = task.getAttempts();
        if (kind == FailureKind.NON_RETRYABLE) {
            log.info("[classify] idempotencyKey={} attempt={} 失败分类=NON_RETRYABLE，依据=处理器显式声明，直接进入死信",
                    key, attempt);
            store.markDeadLetter(key, fence, FailureKind.NON_RETRYABLE, reason);
            return;
        }
        if (attempt >= task.getMaxAttempts()) {
            log.info("[classify] idempotencyKey={} attempt={} 失败分类=RETRY_EXHAUSTED，依据=attempt({}) >= maxAttempts({})，进入死信",
                    key, attempt, attempt, task.getMaxAttempts());
            store.markDeadLetter(key, fence, FailureKind.RETRY_EXHAUSTED, reason);
            return;
        }
        long backoff = backoff(attempt);
        log.info("[classify] idempotencyKey={} attempt={} 失败分类=RETRYABLE，依据=可重试且未达上限，退避={}ms",
                key, attempt, backoff);
        if (store.markRetryPending(key, fence, kind, reason, backoff)) {
            scheduleRetry(key, backoff);
        }
    }

    /** 指数退避：base * 2^(attempt-1)，封顶 MAX_BACKOFF_MILLIS */
    private long backoff(int attempt) {
        long backoff = properties.getBackoffBaseMillis() * (1L << Math.min(attempt - 1, 20));
        return Math.min(backoff, MAX_BACKOFF_MILLIS);
    }

    private void scheduleRetry(String key, long delayMillis) {
        scheduler.schedule(() -> readyQueue.offer(key), delayMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * 优雅停机：停止接收新任务；在途任务在 shutdownTimeoutMillis 内完成，
     * 超时则中断执行线程，并将已退出线程遗留的 RUNNING 任务安全转回 PENDING（可重试状态）。
     * 仍存活线程持有的任务保持 RUNNING，由该线程以其 fencing token 完成终态流转，
     * 令牌过期则被拒绝，避免并发执行产生重复业务效果。
     */
    @Override
    public void close() {
        closing = true;
        workers.shutdown();
        boolean terminated;
        try {
            terminated = workers.awaitTermination(properties.getShutdownTimeoutMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            terminated = false;
        }
        if (!terminated) {
            log.warn("[shutdown] 在途任务未在 {}ms 内完成，中断执行线程并回收为可重试状态",
                    properties.getShutdownTimeoutMillis());
            workers.shutdownNow();
            try {
                workers.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // 回收：执行线程已退出但状态仍为 RUNNING 的任务，安全转为 PENDING
        inFlight.forEach((key, inFlightEntry) -> {
            if (!inFlightEntry.thread().isAlive()) {
                store.markRetryPending(key, inFlightEntry.fence(),
                        FailureKind.RETRYABLE, "shutdown: reclaimed in-flight task", 0);
            } else {
                log.warn("[shutdown] idempotencyKey={} 执行线程仍存活，保持 RUNNING 直至其自行终态流转", key);
            }
        });
        scheduler.shutdown();
        log.info("[shutdown] 流水线已停止，积压 PENDING={}，可在重启后恢复", store.backlog());
    }

    /**
     * 测试/故障注入用：模拟进程被强制结束（kill -9、机器重启）。
     * 立即停止工作线程与调度器，<b>不</b>做任何在途回收与状态清理——RUNNING 任务原样
     * 留在持久化中，由下一个流水线实例启动时按恢复流程接管。
     */
    public void abort() {
        closing = true;
        workers.shutdownNow();
        scheduler.shutdownNow();
        log.warn("[abort] 流水线被强制结束（模拟强杀），未回收在途任务数={}", inFlight.size());
    }
}
