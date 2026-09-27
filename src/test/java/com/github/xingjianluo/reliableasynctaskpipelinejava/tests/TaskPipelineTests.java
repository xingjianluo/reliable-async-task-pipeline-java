package com.github.xingjianluo.reliableasynctaskpipelinejava.tests;

import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 流水线行为测试：重复提交、重试耗尽、不可重试失败、并发提交、背压拒绝、停机恢复。
 * 全部本地内存执行，不依赖外部服务，可重复验证。
 */
class TaskPipelineTests {

    private static final Logger log = LoggerFactory.getLogger(TaskPipelineTests.class);

    private TaskPipeline pipeline;

    @AfterEach
    void tearDown() {
        if (pipeline != null) {
            pipeline.close();
        }
    }

    private TaskPipeline newPipeline(PipelineProperties props, TaskStore store) {
        pipeline = new TaskPipeline(store, props);
        return pipeline;
    }

    private PipelineProperties fastProps() {
        PipelineProperties props = new PipelineProperties();
        props.setConcurrency(4);
        props.setMaxBacklog(100);
        props.setDefaultMaxAttempts(3);
        props.setBackoffBaseMillis(20);
        props.setShutdownTimeoutMillis(500);
        return props;
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("timeout waiting for: " + what);
            }
            Thread.sleep(10);
        }
    }

    @Test
    void duplicateSubmissionIsDeduplicatedAndExecutedOnce() throws Exception {
        TaskPipeline p = newPipeline(fastProps(), new TaskStore());
        AtomicInteger executions = new AtomicInteger();
        p.registerHandler("t", task -> executions.incrementAndGet());

        SubmitOutcome first = p.submit("dup-key", "t", "payload", null);
        SubmitOutcome second = p.submit("dup-key", "t", "payload", null);
        log.info("[test] idempotencyKey=dup-key 两次提交: first.duplicate={} second.duplicate={} status={}",
                first.duplicate(), second.duplicate(), second.task().getStatus());

        assertFalse(first.duplicate(), "首次提交应被受理为新任务");
        assertTrue(second.duplicate(), "第二次提交必须被识别为重复投递");
        assertSame(first.task(), second.task(), "重复提交应返回同一任务（幂等去重）");
        await("SUCCEEDED", () -> first.task().getStatus() == TaskStatus.SUCCEEDED);
        log.info("[test] idempotencyKey=dup-key 终态={} attempts={} executions={}",
                first.task().getStatus(), first.task().getAttempts(), executions.get());
        assertEquals(1, executions.get(), "同一幂等键只能产生一次业务效果");
        assertEquals(1, first.task().getAttempts());
    }

    @Test
    void retryableFailureExhaustsAttemptsAndGoesToDeadLetter() throws Exception {
        TaskPipeline p = newPipeline(fastProps(), new TaskStore());
        AtomicInteger executions = new AtomicInteger();
        p.registerHandler("t", task -> {
            int attempt = executions.incrementAndGet();
            log.info("[test] idempotencyKey={} attempt={} 抛出可重试失败",
                    task.getIdempotencyKey(), attempt);
            throw new TaskFailedException(FailureKind.RETRYABLE, "simulated transient failure");
        });

        Task task = p.submit("retry-key", "t", null, 3).task();
        await("DEAD_LETTER", () -> task.getStatus() == TaskStatus.DEAD_LETTER);
        log.info("[test] idempotencyKey=retry-key 终态={} attempts={} failureKind={} reason={}",
                task.getStatus(), task.getAttempts(), task.getFailureKind(), task.getFailureReason());

        assertEquals(3, task.getAttempts(), "应在有限次数内重试");
        assertEquals(3, executions.get());
        assertEquals(FailureKind.RETRY_EXHAUSTED, task.getFailureKind(), "重试耗尽需保留可区分的失败原因");
        assertTrue(task.getFailureReason().contains("transient"));
    }

    @Test
    void nonRetryableFailureGoesToDeadLetterImmediately() throws Exception {
        TaskPipeline p = newPipeline(fastProps(), new TaskStore());
        AtomicInteger executions = new AtomicInteger();
        p.registerHandler("t", task -> {
            executions.incrementAndGet();
            throw new TaskFailedException(FailureKind.NON_RETRYABLE, "invalid payload, not retryable");
        });

        Task task = p.submit("nonretry-key", "t", null, 5).task();
        await("DEAD_LETTER", () -> task.getStatus() == TaskStatus.DEAD_LETTER);
        log.info("[test] idempotencyKey=nonretry-key 终态={} attempts={} failureKind={}",
                task.getStatus(), task.getAttempts(), task.getFailureKind());

        assertEquals(1, task.getAttempts(), "不可重试失败不应重试");
        assertEquals(1, executions.get());
        assertEquals(FailureKind.NON_RETRYABLE, task.getFailureKind());
    }

    @Test
    void concurrentSubmissionsOfSameKeyProduceSingleTaskAndSingleExecution() throws Exception {
        TaskPipeline p = newPipeline(fastProps(), new TaskStore());
        AtomicInteger executions = new AtomicInteger();
        AtomicInteger running = new AtomicInteger();
        AtomicInteger maxConcurrent = new AtomicInteger();
        p.registerHandler("t", task -> {
            int now = running.incrementAndGet();
            maxConcurrent.accumulateAndGet(now, Math::max);
            Thread.sleep(50);
            running.decrementAndGet();
            executions.incrementAndGet();
        });

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<SubmitOutcome>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                return p.submit("concurrent-key", "t", null, null);
            }));
        }
        start.countDown();
        List<SubmitOutcome> outcomes = new ArrayList<>();
        for (Future<SubmitOutcome> f : futures) {
            outcomes.add(f.get(5, TimeUnit.SECONDS));
        }
        pool.shutdown();

        Task first = outcomes.get(0).task();
        long accepted = outcomes.stream().filter(o -> !o.duplicate()).count();
        long duplicates = outcomes.stream().filter(SubmitOutcome::duplicate).count();
        assertTrue(outcomes.stream().allMatch(o -> o.task() == first), "并发提交同一幂等键必须得到同一任务");
        assertEquals(1, accepted, "并发提交同一幂等键只能有一个被当作新任务受理");
        assertEquals(threads - 1, duplicates, "其余并发提交必须全部识别为重复投递");
        await("SUCCEEDED", () -> first.getStatus() == TaskStatus.SUCCEEDED);
        log.info("[test] idempotencyKey=concurrent-key 终态={} attempts={} executions={} maxConcurrent={}",
                first.getStatus(), first.getAttempts(), executions.get(), maxConcurrent.get());

        assertEquals(1, executions.get(), "同一幂等键不得被并发执行产生多次业务效果");
        assertEquals(1, maxConcurrent.get(), "任一时刻同一幂等键不得被并发执行");
    }

    @Test
    void backpressureRejectsSubmissionWhenBacklogExceedsThreshold() throws Exception {
        PipelineProperties props = fastProps();
        props.setConcurrency(1);
        props.setMaxBacklog(1);
        TaskPipeline p = newPipeline(props, new TaskStore());
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        p.registerHandler("t", task -> {
            entered.countDown();
            release.await();
        });

        p.submit("bp-1", "t", null, null);
        await("bp-1 RUNNING", () -> entered.getCount() == 0);
        p.submit("bp-2", "t", null, null); // 积压=1，未超阈值，受理
        QueueFullException rejected = assertThrows(QueueFullException.class,
                () -> p.submit("bp-3", "t", null, null));
        log.info("[test] idempotencyKey=bp-3 背压拒绝: {}", rejected.getMessage());

        assertTrue(p.query("bp-3").isEmpty(), "被拒绝的任务不应入队");
        assertEquals(TaskStatus.PENDING, p.query("bp-2").orElseThrow().getStatus());
        release.countDown();
        await("bp-2 SUCCEEDED", () -> p.query("bp-2").orElseThrow().getStatus() == TaskStatus.SUCCEEDED);
    }

    @Test
    void shutdownReclaimsInFlightTaskAndRestartRecoversWithoutDuplicateEffect() throws Exception {
        TaskStore store = new TaskStore();
        PipelineProperties props = fastProps();
        props.setConcurrency(1);
        props.setShutdownTimeoutMillis(200);
        TaskPipeline p1 = newPipeline(props, store);

        AtomicInteger effects = new AtomicInteger();
        CountDownLatch firstAttemptStarted = new CountDownLatch(1);
        p1.registerHandler("t", task -> {
            if (firstAttemptStarted.getCount() > 0) {
                // 首次执行：阻塞直至被停机中断，业务效果不产生
                firstAttemptStarted.countDown();
                log.info("[test] idempotencyKey={} 首次执行阻塞，等待停机中断", task.getIdempotencyKey());
                Thread.sleep(60_000);
            }
            // 恢复后的执行：产生业务效果
            effects.incrementAndGet();
            log.info("[test] idempotencyKey={} 恢复执行成功，effects={}", task.getIdempotencyKey(), effects.get());
        });

        Task task = p1.submit("shutdown-key", "t", null, 3).task();
        await("first attempt started", () -> firstAttemptStarted.getCount() == 0);

        p1.close(); // 在途任务超时未完成，应被安全转为可重试状态
        log.info("[test] idempotencyKey=shutdown-key 停机后状态={} attempts={}",
                task.getStatus(), task.getAttempts());
        assertEquals(TaskStatus.PENDING, task.getStatus(), "在途任务需安全转为可重试状态，不得丢失");
        assertEquals(0, effects.get(), "被中断的首次执行不得产生业务效果");

        // 模拟重启：新流水线接管同一存储，恢复遗留 PENDING 任务
        TaskPipeline p2 = newPipeline(props, store);
        p2.registerHandler("t", task2 -> effects.incrementAndGet());
        await("SUCCEEDED after recovery", () -> task.getStatus() == TaskStatus.SUCCEEDED);
        log.info("[test] idempotencyKey=shutdown-key 恢复后终态={} attempts={} effects={}",
                task.getStatus(), task.getAttempts(), effects.get());
        assertEquals(1, effects.get(), "恢复后不得产生重复业务效果");
        p2.close();
    }

    @Test
    void fatalErrorFromHandlerDoesNotStallTaskAndIsRetried() throws Exception {
        PipelineProperties props = fastProps();
        props.setConcurrency(1);
        props.setBackoffBaseMillis(5);
        TaskPipeline p = newPipeline(props, new TaskStore());
        AtomicInteger executions = new AtomicInteger();
        p.registerHandler("t", task -> {
            int n = executions.incrementAndGet();
            if (n == 1) {
                throw new OutOfMemoryError("simulated fatal error");
            }
        });

        Task task = p.submit("fatal-key", "t", null, 3).task();
        await("SUCCEEDED after fatal error retry", () -> task.getStatus() == TaskStatus.SUCCEEDED);
        assertEquals(2, executions.get(), "抛出 Error 不应杀死工作线程/卡住任务，应可重试");
        assertEquals(2, task.getAttempts());
        log.info("[test] idempotencyKey=fatal-key 处理器抛 OOM 后任务正常重试成功，终态={}",
                task.getStatus());
    }

    @Test
    void illegalTransitionsAreRejected() {
        TaskStore store = new TaskStore();
        Task task = store.createIfAbsent(new Task("sm-key", "t", null, 3));
        assertFalse(store.markSucceeded("sm-key", 1), "PENDING 不得直接流转 SUCCEEDED");
        assertFalse(store.markDeadLetter("sm-key", 1, FailureKind.NON_RETRYABLE, "x"), "PENDING 不得直接流转 DEAD_LETTER");
        assertEquals(1, store.tryStart("sm-key"), "首次获得执行权，租约纪元=1");
        assertEquals(0, store.tryStart("sm-key"), "RUNNING 不得重复获取执行权（防并发执行）");
        assertTrue(store.markSucceeded("sm-key", 1));
        assertFalse(store.markRetryPending("sm-key", 1, FailureKind.RETRYABLE, "x", 0), "SUCCEEDED 不得回退 PENDING");
        assertFalse(store.markDeadLetter("sm-key", 1, FailureKind.NON_RETRYABLE, "x"), "SUCCEEDED 不得流转 DEAD_LETTER");
        assertEquals(TaskStatus.SUCCEEDED, task.getStatus());
        log.info("[test] idempotencyKey=sm-key 非法流转均被拒绝，终态={}", task.getStatus());
    }
}
