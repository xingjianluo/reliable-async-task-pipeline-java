package com.github.xingjianluo.reliableasynctaskpipelinejava.tests;

import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.*;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.persist.JdbcTaskRepository;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.persist.TaskRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 强制结束（kill -9 / 机器重启）与恢复语义测试。
 *
 * <p>使用嵌入式 H2 文件库与临时目录，纯本地运行、不依赖外部服务，可重复执行。
 * “强杀”由 {@link TaskPipeline#abort()} 精确模拟：立即停线程、不做任何在途回收。
 */
class PersistenceRecoveryTests {

    private static final Logger log = LoggerFactory.getLogger(PersistenceRecoveryTests.class);

    @TempDir
    Path dataDir;

    private PipelineProperties props() {
        PipelineProperties props = new PipelineProperties();
        props.setConcurrency(1);
        props.setMaxBacklog(100);
        props.setDefaultMaxAttempts(3);
        props.setBackoffBaseMillis(20);
        props.setShutdownTimeoutMillis(200);
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

    /**
     * 进程在任务执行中被强杀：RUNNING 记录原样落盘；重启后自动回收为 PENDING 并重新执行，
     * 只成功一次。旧执行稍后“醒来”拿着过期令牌写 SUCCEEDED 必须被拒绝，不能覆盖新结果。
     */
    @Test
    void hardKillMidExecutionResumesAfterRestartAndStaleExecutionIsFenced() throws Exception {
        TaskRepository repo1 = new JdbcTaskRepository(dataDir);
        TaskStore store1 = new TaskStore(repo1);
        TaskPipeline p1 = new TaskPipeline(store1, props());

        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch releaseZombie = new CountDownLatch(1);
        CountDownLatch zombieReturned = new CountDownLatch(1);
        // 模拟一个不响应中断的处理器（强杀时线程被直接终止，停机逻辑根本没机会跑）
        p1.registerHandler("t", task -> {
            started.countDown();
            log.info("[test] idempotencyKey=kill-key 旧执行进入，阻塞等待（忽略中断）");
            while (true) {
                try {
                    if (releaseZombie.await(1, TimeUnit.SECONDS)) {
                        break;
                    }
                } catch (InterruptedException e) {
                    Thread.interrupted(); // 故意吞掉中断，模拟没走正常停机
                }
            }
            zombieReturned.countDown();
            log.info("[test] idempotencyKey=kill-key 旧执行醒来，试图以过期令牌提交成功");
        });

        Task task = p1.submit("kill-key", "t", null, 3).task();
        await("old execution started", () -> started.getCount() == 0);

        p1.abort(); // 强杀：不回收在途任务
        assertEquals(TaskStatus.RUNNING, repo1.find("kill-key").orElseThrow().getStatus(),
                "强杀后 RUNNING 记录必须原样留在持久化中（任务不能丢）");
        long epochAtKill = repo1.find("kill-key").orElseThrow().getEpoch();
        log.info("[test] 强杀完成：kill-key status=RUNNING epoch={}", epochAtKill);

        // 模拟重启：新存储/新流水线接管同一数据目录
        TaskRepository repo2 = new JdbcTaskRepository(dataDir);
        TaskStore store2 = new TaskStore(repo2);
        AtomicInteger committedEffects = new AtomicInteger();
        TaskPipeline p2 = new TaskPipeline(store2, props());
        p2.registerHandler("t", task2 -> {
            committedEffects.incrementAndGet();
            log.info("[test] idempotencyKey=kill-key 新执行成功提交业务效果 effects={}", committedEffects.get());
        });

        await("SUCCEEDED after restart",
                () -> repo2.find("kill-key").map(t -> t.getStatus() == TaskStatus.SUCCEEDED).orElse(false));
        assertEquals(1, committedEffects.get(), "恢复后只允许产生一次业务效果");
        long epochAfterRecovery = repo2.find("kill-key").orElseThrow().getEpoch();
        assertTrue(epochAfterRecovery > epochAtKill, "恢复执行推进了世代号");
        log.info("[test] 恢复完成：kill-key SUCCEEDED epoch={}", epochAfterRecovery);

        // 旧执行此刻才醒来：放它去写终态，必须被 fencing 拒绝，状态/世代保持不变
        releaseZombie.countDown();
        await("zombie returned", () -> zombieReturned.getCount() == 0);
        long deadline = System.currentTimeMillis() + 2_000;
        while (System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        Task finalView = repo2.find("kill-key").orElseThrow();
        assertEquals(TaskStatus.SUCCEEDED, finalView.getStatus(), "旧执行不得覆盖新状态");
        assertEquals(epochAfterRecovery, finalView.getEpoch(), "旧执行的过期令牌不得推进状态");
        assertEquals(1, committedEffects.get(), "旧执行醒来不得再多出一次业务效果");
        log.info("[test] 旧执行写回被 fencing 拒绝，最终 status={} epoch={}", finalView.getStatus(), finalView.getEpoch());

        p2.close();
        p1.abort(); // 再次中断残留的旧工作线程，让它退出
    }

    /**
     * 已成功的任务在强杀重启后不得再跑一遍；同一幂等键再次提交必须识别为重复投递。
     * 强杀时仍排队（attempts=0）的 PENDING 任务也不能丢，重启后照常执行且只执行一次。
     */
    @Test
    void succeededTaskIsNotReRunAfterRestartAndQueuedTaskIsRecovered() throws Exception {
        TaskRepository repo1 = new JdbcTaskRepository(dataDir);
        TaskStore store1 = new TaskStore(repo1);
        TaskPipeline p1 = new TaskPipeline(store1, props());

        AtomicInteger doneEffects = new AtomicInteger();
        CountDownLatch blockStarted = new CountDownLatch(1);
        CountDownLatch releaseBlock = new CountDownLatch(1);
        p1.registerHandler("done", task -> doneEffects.incrementAndGet());
        p1.registerHandler("block", task -> {
            blockStarted.countDown();
            while (true) {
                try {
                    if (releaseBlock.await(1, TimeUnit.SECONDS)) {
                        break;
                    }
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
        });
        AtomicInteger queuedEffects1 = new AtomicInteger();
        p1.registerHandler("queued", task -> queuedEffects1.incrementAndGet());

        Task done = p1.submit("done-key", "done", null, null).task();
        await("done-key SUCCEEDED", () -> done.getStatus() == TaskStatus.SUCCEEDED);
        assertEquals(1, doneEffects.get());

        p1.submit("block-key", "block", null, null);
        await("block-key RUNNING", () -> blockStarted.getCount() == 0);
        // 单线程：block 占住 worker，queued 一直排队、attempts=0
        Task queued = p1.submit("queued-key", "queued", null, null).task();
        await("queued-key persisted as PENDING",
                () -> repo1.find("queued-key").map(t -> t.getStatus() == TaskStatus.PENDING).orElse(false));
        assertEquals(0, queued.getAttempts());

        p1.abort();
        assertEquals(TaskStatus.RUNNING, repo1.find("block-key").orElseThrow().getStatus());
        assertEquals(TaskStatus.PENDING, repo1.find("queued-key").orElseThrow().getStatus());
        assertEquals(TaskStatus.SUCCEEDED, repo1.find("done-key").orElseThrow().getStatus());
        log.info("[test] 强杀后：done=SUCCEEDED block=RUNNING queued=PENDING，均未丢失");

        // 重启
        TaskRepository repo2 = new JdbcTaskRepository(dataDir);
        TaskStore store2 = new TaskStore(repo2);
        TaskPipeline p2 = new TaskPipeline(store2, props());
        AtomicInteger doneEffects2 = new AtomicInteger();
        AtomicInteger queuedEffects2 = new AtomicInteger();
        p2.registerHandler("done", task -> doneEffects2.incrementAndGet());
        p2.registerHandler("block", task -> { /* 恢复后立即完成 */ });
        p2.registerHandler("queued", task -> queuedEffects2.incrementAndGet());

        // 对已成功键的再次提交：必须识别为重复，拿到的是原任务
        SubmitResult repeat = p2.submit("done-key", "done", null, null);
        assertTrue(repeat.duplicate(), "重启后同一幂等键再次提交必须识别为重复投递");
        assertSame(repeat.task(), store2.find("done-key").orElseThrow());
        assertEquals(TaskStatus.SUCCEEDED, repeat.task().getStatus());

        await("queued-key recovered and SUCCEEDED",
                () -> repo2.find("queued-key").map(t -> t.getStatus() == TaskStatus.SUCCEEDED).orElse(false));
        await("block-key recovered and SUCCEEDED",
                () -> repo2.find("block-key").map(t -> t.getStatus() == TaskStatus.SUCCEEDED).orElse(false));

        long graceDeadline = System.currentTimeMillis() + 500;
        while (System.currentTimeMillis() < graceDeadline) {
            Thread.sleep(20);
        }
        assertEquals(0, doneEffects2.get(), "已成功的任务重启后不得再执行一遍");
        assertEquals(1, queuedEffects2.get(), "强杀时排队的任务重启后执行且只执行一次");
        assertEquals(1, doneEffects.get(), "成功业务效果自始至终只有一次");
        log.info("[test] 重启后：done 未重跑、queued 恢复执行一次、重复提交被识别");

        releaseBlock.countDown();
        p2.close();
        p1.abort();
    }

    /** 仓储层 fencing token 条件更新的确定性校验：过期令牌一律写不进去 */
    @Test
    void repositoryEpochCasRejectsStaleToken() {
        TaskRepository repo = new JdbcTaskRepository(dataDir);
        assertTrue(repo.insertIfAbsent(new Task("f-key", "t", null, 3)).isEmpty());

        assertEquals(1, repo.claimForRun("f-key", 0), "领取成功返回新令牌 epoch+1");
        assertEquals(-1, repo.claimForRun("f-key", 0), "PENDING 已变 RUNNING，旧令牌不能再领取");
        assertEquals(-1, repo.claimForRun("f-key", 1), "RUNNING 不能重复领取");

        assertFalse(repo.completeSuccess("f-key", 999), "过期令牌不得写 SUCCEEDED");
        assertFalse(repo.completeSuccess("f-key", 0), "更早的令牌不得写 SUCCEEDED");
        assertTrue(repo.completeSuccess("f-key", 1), "当前令牌写成功");
        Task succeeded = repo.find("f-key").orElseThrow();
        assertEquals(TaskStatus.SUCCEEDED, succeeded.getStatus());
        assertEquals(2, succeeded.getEpoch());

        assertFalse(repo.completeSuccess("f-key", 1), "终态后旧令牌不能再写");
        assertFalse(repo.scheduleRetry("f-key", 1, "RETRYABLE", "x", System.currentTimeMillis()),
                "终态不得回退");
        log.info("[test] 仓储 epoch CAS 全部符合预期，终态 epoch=2");
    }
}
