package com.github.xingjianluo.reliableasynctaskpipelinejava.tests;

import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 强制结束（不走优雅停机）后重启恢复的端到端测试：
 * 1. 在途任务不丢，重启自动接着处理，且只产生一次业务效果；
 * 2. 已成功任务重启后不再执行，重复提交被识别；
 * 3. 排队中（attempts=0）的重复提交也被识别；
 * 4. 被中断后晚醒的旧执行不能覆盖已经往前走的新结果（fencing epoch）。
 *
 * <p>全部基于本地临时目录的文件 WAL，不依赖任何外部服务，可重复运行。
 */
class CrashRecoveryTests {

    private static final Logger log = LoggerFactory.getLogger(CrashRecoveryTests.class);

    @TempDir
    Path dir;

    private PipelineProperties fastProps() {
        PipelineProperties props = new PipelineProperties();
        props.setConcurrency(2);
        props.setMaxBacklog(100);
        props.setDefaultMaxAttempts(3);
        props.setBackoffBaseMillis(20);
        props.setShutdownTimeoutMillis(200);
        props.setJournalPath(""); // 由各用例自行用 FileTaskJournal 装配
        return props;
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("timeout waiting for: " + what);
            }
            Thread.sleep(10);
        }
    }

    private FileTaskJournal newJournal(String name) {
        return new FileTaskJournal(dir.resolve(name));
    }

    @Test
    void inFlightTaskSurvivesHardKillAndCompletesExactlyOnceAfterRestart() throws Exception {
        Path ignored = dir;
        PipelineProperties props = fastProps();
        FileTaskJournal journal = newJournal("inflight.log");
        TaskStore store = new TaskStore(journal);

        AtomicInteger effects = new AtomicInteger();
        CountDownLatch started = new CountDownLatch(1);
        TaskPipeline p1 = new TaskPipeline(store, props);
        // 首次执行进入后阻塞等待中断（模拟在途时进程被强制结束，业务效果尚未产生）。
        // hardStop 中断本线程后 execute() 走 InterruptedException 路径，不会产生业务效果；
        // 持久化保证的关键是：此刻 WAL 里只有 SUBMITTED + STARTED，重启重放按崩溃语义回收。
        p1.registerHandler("t", task -> {
            started.countDown();
            try {
                Thread.sleep(60_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            }
        });

        Task task = p1.submit("crash-inflight", "t", null, 3).task();
        await("first execution started", () -> started.getCount() == 0);
        p1.hardStopForCrashSimulation(); // 不走优雅停机回收
        Thread.sleep(150); // 等中断落地
        journal.close(); // 模拟进程死亡后 OS 释放文件句柄
        assertEquals(0, effects.get(), "被强杀的首次执行不得产生业务效果");
        // 无论中断路径是否来得及写回退，持久化关键不变量：WAL 里没有 SUCCEEDED

        // 模拟重启：新存储重放 WAL，遗留 RUNNING 回收为 PENDING（PENDING 则直接续跑）
        try (FileTaskJournal reopenedJournal = newJournal("inflight.log")) {
            TaskStore reopenedStore = new TaskStore(reopenedJournal);
            Task recovered = reopenedStore.find("crash-inflight").orElseThrow();
            assertTrue(recovered.getStatus() == TaskStatus.PENDING,
                    "重启后未完成任务必须处于可续跑的 PENDING，实际=" + recovered.getStatus());
            log.info("[test] 重启恢复：idempotencyKey=crash-inflight status={} attempts={}",
                    recovered.getStatus(), recovered.getAttempts());

            TaskPipeline p2 = new TaskPipeline(reopenedStore, props,
                    java.util.Map.of("t", t -> effects.incrementAndGet()));
            await("SUCCEEDED after restart", () -> recovered.getStatus() == TaskStatus.SUCCEEDED);
            assertEquals(1, effects.get(), "恢复执行后业务效果恰好一次");
            log.info("[test] idempotencyKey=crash-inflight 重启后终态={} effects={}",
                    recovered.getStatus(), effects.get());
            p2.close();
        }
    }

    @Test
    void succeededTaskIsNotReRunAfterRestartAndDuplicateSubmitIsRecognized() throws Exception {
        PipelineProperties props = fastProps();
        FileTaskJournal journal = newJournal("succeeded.log");
        AtomicInteger effects = new AtomicInteger();

        Task succeededTask;
        TaskPipeline p1 = new TaskPipeline(new TaskStore(journal), props);
        p1.registerHandler("t", task -> effects.incrementAndGet());
        succeededTask = p1.submit("done-key", "t", "p", null).task();
        await("SUCCEEDED before kill", () -> succeededTask.getStatus() == TaskStatus.SUCCEEDED);
        p1.hardStopForCrashSimulation();
        Thread.sleep(50);
        assertEquals(1, effects.get());
        journal.close();

        // 重启
        try (FileTaskJournal reopenedJournal = newJournal("succeeded.log")) {
            TaskStore store2 = new TaskStore(reopenedJournal);
            Task before = store2.find("done-key").orElseThrow();
            assertEquals(TaskStatus.SUCCEEDED, before.getStatus(), "成功状态必须从日志恢复，不重跑");

            TaskPipeline p2 = new TaskPipeline(store2, props,
                    java.util.Map.of("t", task -> effects.incrementAndGet()));
            Thread.sleep(200); // 给点时间确认不会重新执行
            assertEquals(1, effects.get(), "已成功的任务重启后不得再跑一遍");

            SubmitOutcome again = p2.submit("done-key", "t", "p", null);
            assertTrue(again.duplicate(), "同键重启后再次提交必须识别为重复投递");
            assertSame(before, again.task(), "重复投递拿到的是原来那条任务");
            assertEquals(1, effects.get(), "重复投递不得产生额外业务效果");
            log.info("[test] idempotencyKey=done-key 重启后保持 SUCCEEDED，重复提交 duplicate=true，effects={}",
                    effects.get());
            p2.close();
        }
    }

    @Test
    void duplicateSubmitWhileStillQueuedWithZeroAttemptsIsRecognized() throws Exception {
        PipelineProperties props = fastProps();
        props.setConcurrency(1);
        try (FileTaskJournal journal = newJournal("queued.log")) {
            TaskStore store = new TaskStore(journal);
            // 占住唯一工作线程，让 bp 任务一直排在队列里
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            TaskPipeline p = new TaskPipeline(store, props);
            p.registerHandler("t", task -> {
                entered.countDown();
                release.await();
            });
            p.submit("holder", "t", null, null);
            await("holder running", () -> entered.getCount() == 0);

            SubmitOutcome first = p.submit("queued-key", "t", null, null);
            SubmitOutcome second = p.submit("queued-key", "t", null, null);
            assertFalse(first.duplicate());
            assertTrue(second.duplicate(), "任务还在排队、attempts=0，也必须认出是重复投递");
            assertSame(first.task(), second.task());
            assertEquals(0, first.task().getAttempts());
            assertEquals(TaskStatus.PENDING, first.task().getStatus());
            log.info("[test] idempotencyKey=queued-key 排队中重复投递被识别 duplicate=true");

            release.countDown();
            await("queued-key SUCCEEDED", () -> second.task().getStatus() == TaskStatus.SUCCEEDED);
            p.close();
        }
    }

    @Test
    void staleExecutionFromBeforeCrashCannotOverwriteNewerSuccessfulResult() throws Exception {
        PipelineProperties props = fastProps();
        props.setConcurrency(1);
        // 同 JVM 模拟“旧执行僵尸存活”：保持同一 WAL 句柄（真实场景里旧进程已死、由新进程持锁）
        try (FileTaskJournal journal = newJournal("fence.log")) {
            TaskStore store = new TaskStore(journal);

            CountDownLatch firstStarted = new CountDownLatch(1);
            CountDownLatch letOldFinish = new CountDownLatch(1);
            AtomicInteger effects = new AtomicInteger();
            AtomicInteger startedCount = new AtomicInteger();
            TaskPipeline p1 = new TaskPipeline(store, props);
            p1.registerHandler("t", task -> {
                int n = startedCount.incrementAndGet();
                if (n == 1) {
                    // 旧执行：忽略中断一直阻塞，直到“新执行成功之后”才被放行返回
                    firstStarted.countDown();
                    while (true) {
                        try {
                            letOldFinish.await();
                            break;
                        } catch (InterruptedException e) {
                            // 故意忽略中断，模拟杀不死的旧执行
                        }
                    }
                    log.info("[test] 旧执行（第1次）晚醒并返回，尝试回写 SUCCEEDED");
                } else {
                    effects.incrementAndGet(); // 新执行：真正产生业务效果
                }
            });

            Task task = p1.submit("fence-key", "t", null, 3).task();
            await("old execution started", () -> firstStarted.getCount() == 0);

            // 模拟强杀：shutdownNow 无法结束忽略中断的旧执行，它仍持有旧租约
            p1.hardStopForCrashSimulation();
            Thread.sleep(100);
            assertEquals(TaskStatus.RUNNING, task.getStatus());

            // 重启恢复：新实例重放日志后回收遗留 RUNNING（bump 纪元，旧租约失效），再接管执行
            int reclaimed = store.reclaimInterruptedTasks();
            assertEquals(1, reclaimed);
            TaskPipeline p2 = new TaskPipeline(store, props,
                    java.util.Map.of("t", t2 -> {
                        if (startedCount.incrementAndGet() == 2) {
                            effects.incrementAndGet(); // 新执行：真正产生唯一的业务效果
                        }
                    }));
            await("re-execution succeeds", () -> task.getStatus() == TaskStatus.SUCCEEDED);
            assertEquals(1, effects.get(), "只有新执行产生业务效果");

            // 此时才放行旧执行：它带着旧 epoch 回写 SUCCEEDED，必须被 fencing 拒绝
            letOldFinish.countDown();
            await("stale handler returned", () -> startedCount.get() >= 2);
            Thread.sleep(150); // 等待旧执行走完 markSucceeded（被拒绝路径）
            assertEquals(TaskStatus.SUCCEEDED, task.getStatus(), "旧执行不得覆盖新状态");
            assertEquals(1, effects.get(), "最终业务效果仍只有一次");
            log.info("[test] idempotencyKey=fence-key 旧执行晚醒回写被纪元拒绝，终态仍为新执行的 SUCCEEDED");
            p2.close();
        }
    }
}
