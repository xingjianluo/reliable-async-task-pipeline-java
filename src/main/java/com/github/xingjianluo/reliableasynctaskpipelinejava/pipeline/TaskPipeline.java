package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.*;

/**
 * 本地异步任务流水线：提交、幂等去重、并发执行、失败重试、死信与优雅停机。
 *
 * <p>语义：同一幂等键的任务至多被并发执行一次（由 {@link TaskStore} 的状态机保证），
 * 业务效果至多成功提交一次（RUNNING -> SUCCEEDED 仅允许一次流转）。
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
    /** 在途任务 -> 执行线程，用于停机时判断线程是否仍存活 */
    private final ConcurrentHashMap<String, Thread> inFlight = new ConcurrentHashMap<>();
    private volatile boolean closing = false;

    public TaskPipeline(TaskStore store, PipelineProperties properties) {
        this(store, properties, java.util.Map.of());
    }

    /**
     * 带初始处理器的构造器：处理器必须在恢复任务入队之前就位，
     * 否则重启恢复出来的任务可能在 {@link #registerHandler} 之前就被工作线程取走。
     */
    public TaskPipeline(TaskStore store, PipelineProperties properties,
                        java.util.Map<String, TaskHandler> initialHandlers) {
        this.store = store;
        this.properties = properties;
        this.handlers.putAll(initialHandlers);
        this.workers = Executors.newFixedThreadPool(properties.getConcurrency(),
                r -> daemon(r, "pipeline-worker"));
        // 恢复：接管存储中遗留的 PENDING 任务（优雅停机回收或崩溃重放回收）
        for (String key : store.pendingKeys()) {
            log.info("[recover] idempotencyKey={} 恢复遗留 PENDING 任务，重新入队", key);
            readyQueue.offer(key);
        }
        for (int i = 0; i < properties.getConcurrency(); i++) {
            workers.submit(this::workerLoop);
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
     * @return {@link SubmitOutcome}：新受理 {@code duplicate=false}；
     *         同键重复投递 {@code duplicate=true} 且任务为原来那条（即使它还在 PENDING、attempts=0）
     * @throws QueueFullException     积压超过阈值，背压拒绝（与业务失败可区分）
     * @throws IllegalStateException  流水线已关闭
     */
    public SubmitOutcome submit(String idempotencyKey, String type, String payload, Integer maxAttempts) {
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
            // 重复提交：幂等去重，直接返回现有任务（可能仍 PENDING、attempts=0，也必须识别为重复）
            return SubmitOutcome.duplicate(stored);
        }
        if (store.backlog() > properties.getMaxBacklog()) {
            // 背压拒绝：任务未入队，与业务失败可区分
            store.removeIfPending(idempotencyKey);
            throw new QueueFullException("backlog exceeds maxBacklog=" + properties.getMaxBacklog()
                    + ", submission rejected for idempotencyKey=" + idempotencyKey);
        }
        readyQueue.offer(idempotencyKey);
        return SubmitOutcome.accepted(stored);
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
            int epoch = store.tryStart(key);
            if (epoch == 0) {
                // 未获得执行权：重复入队、未到执行时间或已终态。若仍是 PENDING（未到时间），重新调度
                store.find(key).filter(t -> t.getStatus() == TaskStatus.PENDING)
                        .ifPresent(t -> scheduleRetry(key, delayUntil(t)));
                continue;
            }
            inFlight.put(key, Thread.currentThread());
            try {
                execute(key, epoch);
            } finally {
                inFlight.remove(key);
            }
        }
    }

    private static long delayUntil(Task t) {
        return Math.max(0, t.getNextAttemptAt().toEpochMilli() - System.currentTimeMillis());
    }

    private void execute(String key, int epoch) {
        Task task = store.find(key).orElseThrow();
        TaskHandler handler = handlers.get(task.getType());
        try {
            handler.handle(task);
            store.markSucceeded(key, epoch);
        } catch (TaskFailedException e) {
            onFailure(task, epoch, e.getFailureKind(), e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            onFailure(task, epoch, FailureKind.RETRYABLE, "interrupted (shutdown): " + e.getMessage());
        } catch (Exception e) {
            // 未分类异常按可重试失败处理
            onFailure(task, epoch, FailureKind.RETRYABLE, e.getClass().getSimpleName() + ": " + e.getMessage());
        } catch (Error e) {
            // 严重错误（OOM/StackOverflow 等）也不能让任务永久卡在 RUNNING：
            // 按可重试失败处理，交给后续重试/重启恢复；fencing 保证不会产生重复效果
            onFailure(task, epoch, FailureKind.RETRYABLE, "fatal: " + e.getClass().getSimpleName()
                    + ": " + e.getMessage());
        }
    }

    private void onFailure(Task task, int epoch, FailureKind kind, String reason) {
        String key = task.getIdempotencyKey();
        int attempt = task.getAttempts();
        if (kind == FailureKind.NON_RETRYABLE) {
            log.info("[classify] idempotencyKey={} attempt={} epoch={} 失败分类=NON_RETRYABLE，依据=处理器显式声明，直接进入死信",
                    key, attempt, epoch);
            store.markDeadLetter(key, epoch, FailureKind.NON_RETRYABLE, reason);
            return;
        }
        if (attempt >= task.getMaxAttempts()) {
            log.info("[classify] idempotencyKey={} attempt={} epoch={} 失败分类=RETRY_EXHAUSTED，依据=attempt({}) >= maxAttempts({})，进入死信",
                    key, attempt, epoch, attempt, task.getMaxAttempts());
            store.markDeadLetter(key, epoch, FailureKind.RETRY_EXHAUSTED, reason);
            return;
        }
        long backoff = backoff(attempt);
        log.info("[classify] idempotencyKey={} attempt={} epoch={} 失败分类=RETRYABLE，依据=可重试且未达上限，退避={}ms",
                key, attempt, epoch, backoff);
        if (store.markRetryPending(key, epoch, kind, reason, backoff)) {
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
     * 测试/演练支撑：模拟进程被强制结束（kill -9 / 机器重启）。
     * <p><b>不</b>做优雅停机的任何回收：立即中断工作线程与调度器。RUNNING 任务保持 RUNNING，
     * 由新实例在重放日志后按崩溃语义回收。注意忽略中断的处理器线程在同 JVM 内仍会继续，
     * 这正好用于验证“旧执行晚醒不能覆盖新状态”。
     */
    public void hardStopForCrashSimulation() {
        closing = true;
        workers.shutdownNow();
        scheduler.shutdownNow();
        log.warn("[crash-sim] 模拟强制结束：不回收在途任务（保持 RUNNING），等待新实例重放恢复");
    }

    /**
     * 优雅停机：停止接收新任务；在途任务在 shutdownTimeoutMillis 内完成，
     * 超时则中断执行线程，并将已退出线程遗留的 RUNNING 任务安全转回 PENDING（可重试状态）。
     * 仍存活线程持有的任务保持 RUNNING，由该线程自行完成终态流转，避免并发执行产生重复业务效果。
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
        // 回收：执行线程已退出但状态仍为 RUNNING 的任务，安全转为 PENDING 并 bump 纪元，
        // 旧执行稍后醒来也无法回写终态
        boolean anyDead = inFlight.values().stream().anyMatch(t -> !t.isAlive());
        inFlight.forEach((key, thread) -> {
            if (thread.isAlive()) {
                log.warn("[shutdown] idempotencyKey={} 执行线程仍存活，保持 RUNNING 直至其自行终态流转", key);
            }
        });
        if (anyDead) {
            store.reclaimInterruptedTasks();
        }
        scheduler.shutdown();
        log.info("[shutdown] 流水线已停止，积压 PENDING={}，可在重启后恢复", store.backlog());
    }
}
