package com.github.xingjianluo.reliableasynctaskpipelinejava.tests;

import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.FailureClassification;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.NonRetryableTaskException;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.PipelineConfig;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.RejectReason;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.SubmissionResult;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.TaskHandler;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.TaskPipelineService;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.TaskRecord;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.TaskStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 端到端场景测试，全部基于本地文件存储，不依赖任何外部服务，结果可重复。
 * 运行时日志会打印 idempotencyKey / attempt / 状态流转 / 失败分类依据。
 */
class TaskPipelineIntegrationTest {

    @TempDir
    Path tempDir;

    private final List<TaskPipelineService> services = new ArrayList<>();

    private PipelineConfig config(int workers, int queueCapacity, int maxAttempts, long shutdownWaitMs) {
        PipelineConfig config = new PipelineConfig();
        config.setWorkerThreads(workers);
        config.setQueueCapacity(queueCapacity);
        config.setMaxAttempts(maxAttempts);
        config.setBackoffBaseMillis(10);
        config.setBackoffMaxMillis(50);
        config.setShutdownWaitMillis(shutdownWaitMs);
        config.setStoreDir(tempDir.resolve("store-" + services.size()).toString());
        return config;
    }

    private TaskPipelineService newService(PipelineConfig config, TaskHandler handler) {
        TaskPipelineService service = new TaskPipelineService(config, handler);
        services.add(service);
        service.start();
        return service;
    }

    @AfterEach
    void tearDown() {
        for (TaskPipelineService service : services) {
            service.stop();
        }
    }

    private static void awaitStatus(TaskPipelineService service, String key, TaskStatus status) {
        await().atMost(10, TimeUnit.SECONDS).pollInterval(20, TimeUnit.MILLISECONDS)
                .until(() -> {
                    TaskRecord r = service.getStatus(key);
                    return r != null && r.getStatus() == status;
                });
    }

    // ---- 场景 1：重复提交被识别，业务效果只产生一次 ----------------------

    @Test
    void duplicateSubmissionRecognizedAndBusinessEffectAppliedOnce() {
        AtomicInteger effects = new AtomicInteger();
        TaskPipelineService service = newService(config(2, 100, 3, 5000),
                ctx -> effects.incrementAndGet());

        SubmissionResult first = service.submit("dup-key", "p1");
        SubmissionResult second = service.submit("dup-key", "p2-different-payload-ignored");

        assertThat(first.isAccepted()).isTrue();
        assertThat(second.isDuplicate()).isTrue();
        assertThat(second.getTask().getIdempotencyKey()).isEqualTo("dup-key");

        awaitStatus(service, "dup-key", TaskStatus.SUCCEEDED);
        TaskRecord record = service.getStatus("dup-key");
        assertThat(record.getAttempts()).isEqualTo(1);
        assertThat(effects.get()).isEqualTo(1);

        // 成功后再次提交仍是重复，终态不回退
        SubmissionResult third = service.submit("dup-key", "p3");
        assertThat(third.isDuplicate()).isTrue();
        assertThat(service.getStatus("dup-key").getStatus()).isEqualTo(TaskStatus.SUCCEEDED);
    }

    // ---- 场景 2：可重试失败重试耗尽后进入死信，保留区分原因 --------------

    @Test
    void retryableFailureExhaustsAttemptsAndGoesDeadWithReason() {
        AtomicInteger calls = new AtomicInteger();
        TaskPipelineService service = newService(config(1, 100, 3, 5000), ctx -> {
            calls.incrementAndGet();
            throw new IllegalStateException("transient downstream failure");
        });

        service.submit("retry-key", null);
        awaitStatus(service, "retry-key", TaskStatus.DEAD);

        TaskRecord record = service.getStatus("retry-key");
        assertThat(record.getAttempts()).isEqualTo(3);
        assertThat(calls.get()).isEqualTo(3);
        assertThat(record.getLastFailureClassification()).isEqualTo(FailureClassification.RETRYABLE);
        assertThat(record.getLastFailureReason())
                .contains("RETRY_LIMIT_EXHAUSTED")
                .contains("IllegalStateException")
                .contains("transient downstream failure");
    }

    // ---- 场景 3：不可重试失败直接进死信，不消耗重试 ----------------------

    @Test
    void nonRetryableFailureGoesDeadImmediately() {
        AtomicInteger calls = new AtomicInteger();
        TaskPipelineService service = newService(config(2, 100, 5, 5000), ctx -> {
            calls.incrementAndGet();
            throw new NonRetryableTaskException("invalid payload: missing field x");
        });

        service.submit("fatal-key", null);
        awaitStatus(service, "fatal-key", TaskStatus.DEAD);

        TaskRecord record = service.getStatus("fatal-key");
        assertThat(record.getAttempts()).isEqualTo(1);
        assertThat(calls.get()).isEqualTo(1);
        assertThat(record.getLastFailureClassification()).isEqualTo(FailureClassification.NON_RETRYABLE);
        assertThat(record.getLastFailureReason())
                .contains("NON_RETRYABLE")
                .contains("invalid payload: missing field x");
    }

    // ---- 场景 4：同一幂等键的并发提交只接受一次，且只执行一次 ------------

    @Test
    void concurrentSubmissionsWithSameKeyAreDeduplicatedAndSerialized() throws Exception {
        AtomicInteger effects = new AtomicInteger();
        CountDownLatch inside = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        TaskPipelineService service = newService(config(4, 100, 3, 5000), ctx -> {
            // 首次执行挂住，期间其它线程并发重复提交
            if (ctx.getAttempt() == 1) {
                inside.countDown();
                release.await(5, TimeUnit.SECONDS);
            }
            effects.incrementAndGet();
        });

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        List<SubmissionResult> results = new java.util.concurrent.CopyOnWriteArrayList<>();
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    results.add(service.submit("race-key", "payload"));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertThat(inside.await(5, TimeUnit.SECONDS)).isTrue();
        // 任务正在 RUNNING 时重复提交也必须被识别（提交线程不会被执行阻塞）
        release.countDown();
        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        pool.shutdown();

        long accepted = results.stream().filter(SubmissionResult::isAccepted).count();
        long duplicates = results.stream().filter(SubmissionResult::isDuplicate).count();
        assertThat(accepted).isEqualTo(1);
        assertThat(duplicates).isEqualTo(threads - 1);

        awaitStatus(service, "race-key", TaskStatus.SUCCEEDED);
        assertThat(effects.get()).isEqualTo(1);
        assertThat(service.getStatus("race-key").getAttempts()).isEqualTo(1);
    }

    // ---- 场景 5：队列积压超阈值，提交被拒绝且原因与业务失败可区分 --------

    @Test
    void submissionRejectedWithQueueFullWhenBacklogExceedsThreshold() throws Exception {
        CountDownLatch block = new CountDownLatch(1);
        // 容量为 1：q-1 被领取执行后，队列里的一个名额让给 q-2，q-3 必须被背压拒绝
        TaskPipelineService service = newService(config(1, 1, 3, 5000), ctx -> block.await(10, TimeUnit.SECONDS));

        assertThat(service.submit("q-1", null).isAccepted()).isTrue();
        assertThat(service.submit("q-2", null).isAccepted()).isTrue();
        await().atMost(2, TimeUnit.SECONDS).until(() ->
                service.getStatus("q-1") != null
                        && service.getStatus("q-1").getStatus() == TaskStatus.RUNNING);

        SubmissionResult rejected = service.submit("q-3", null);
        assertThat(rejected.isAccepted()).isFalse();
        assertThat(rejected.getRejectReason()).isEqualTo(RejectReason.QUEUE_FULL);
        assertThat(service.getStatus("q-3")).isNull(); // 背压回滚，拒绝即不存在

        block.countDown();
        awaitStatus(service, "q-2", TaskStatus.SUCCEEDED);
        // 队列腾出后同样的键可以正常提交
        assertThat(service.submit("q-3", null).isAccepted()).isTrue();
        awaitStatus(service, "q-3", TaskStatus.SUCCEEDED);
    }

    // ---- 场景 6：优雅停机超时，在途任务安全转为可重试，重启后不重复生效 --

    @Test
    void interruptedInFlightTaskIsRecoveredWithoutDuplicateEffect() throws Exception {
        // 第一阶段：任务执行时间超过优雅停机窗口，记录执行已产生的业务效果
        CountDownLatch firstExecutionEntered = new CountDownLatch(1);
        CountDownLatch firstRunAllowExit = new CountDownLatch(1);
        ConcurrentHashMap.KeySetView<String, Boolean> effectKeys = ConcurrentHashMap.newKeySet();

        PipelineConfig cfg = config(1, 100, 3, 400);
        TaskPipelineService first = newService(cfg, ctx -> {
            firstExecutionEntered.countDown();
            // 模拟忽略中断的慢任务：停机窗口内未提交业务效果，直到测试放行后才返回
            firstRunAllowExit.await(30, TimeUnit.SECONDS);
            // 注意：这里即使返回成功也不应覆盖已恢复的 PENDING（由执行代次保证）
        });

        first.submit("interrupt-key", null);
        assertThat(firstExecutionEntered.await(5, TimeUnit.SECONDS)).isTrue();

        // 停机超时：RUNNING 必须被持久化为 PENDING
        first.stop();
        await().atMost(2, TimeUnit.SECONDS).until(() -> {
            TaskRecord r = first.getStatus("interrupt-key");
            return r != null && r.getStatus() == TaskStatus.PENDING;
        });
        assertThat(first.getStatus("interrupt-key").getLastFailureClassification())
                .as("中断恢复原因保留为可重试")
                .isEqualTo(FailureClassification.RETRYABLE);

        // 旧执行线程此刻才真正结束：其迟到的成功结果必须被代次机制丢弃
        firstRunAllowExit.countDown();
        Thread.sleep(200);
        assertThat(first.getStatus("interrupt-key").getStatus())
                .as("旧执行迟到的 SUCCEEDED 不得覆盖已恢复的 PENDING")
                .isEqualTo(TaskStatus.PENDING);
        assertThat(effectKeys).isEmpty();

        // 第二阶段：用同一存储目录重启，任务恢复执行并成功，业务效果只产生一次
        PipelineConfig cfg2 = config(1, 100, 3, 5000);
        cfg2.setStoreDir(cfg.getStoreDir());
        TaskPipelineService second = newService(cfg2, ctx -> effectKeys.add(ctx.getIdempotencyKey()));

        awaitStatus(second, "interrupt-key", TaskStatus.SUCCEEDED);
        TaskRecord finalRecord = second.getStatus("interrupt-key");
        assertThat(finalRecord.getAttempts()).isEqualTo(1);
        assertThat(effectKeys).containsExactly("interrupt-key");
    }

    // ---- 场景 7：不同任务可并发处理 ------------------------------------

    @Test
    void differentTasksRunConcurrentlyUpToWorkerLimit() throws Exception {
        int taskCount = 8;
        CountDownLatch gate = new CountDownLatch(1);
        AtomicInteger maxConcurrency = new AtomicInteger();
        AtomicInteger runningNow = new AtomicInteger();
        TaskPipelineService service = newService(config(4, taskCount, 3, 5000), ctx -> {
            int current = runningNow.incrementAndGet();
            maxConcurrency.updateAndGet(prev -> Math.max(prev, current));
            gate.await(5, TimeUnit.SECONDS);
            runningNow.decrementAndGet();
        });

        for (int i = 0; i < taskCount; i++) {
            assertThat(service.submit("parallel-" + i, null).isAccepted()).isTrue();
        }
        await().atMost(2, TimeUnit.SECONDS).until(() -> runningNow.get() == 4);
        gate.countDown();
        for (int i = 0; i < taskCount; i++) {
            awaitStatus(service, "parallel-" + i, TaskStatus.SUCCEEDED);
        }
        assertThat(maxConcurrency.get()).isEqualTo(4);
    }
}
