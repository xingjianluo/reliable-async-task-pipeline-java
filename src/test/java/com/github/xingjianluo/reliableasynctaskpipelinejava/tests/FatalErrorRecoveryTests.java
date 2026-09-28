package com.github.xingjianluo.reliableasynctaskpipelinejava.tests;

import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.*;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.persist.JdbcTaskRepository;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.persist.TaskRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 处理器抛出严重错误（Error 级别）后的持久化与重启恢复语义。
 *
 * <p>使用 JUnit {@link TempDir} 提供的临时目录承载嵌入式 H2 文件库：
 * 不在项目工作目录生成或改动任何数据文件，每次运行都是全新数据，可本地反复执行。
 */
class FatalErrorRecoveryTests {

    @TempDir
    Path dataDir;

    private PipelineProperties props() {
        return props(20);
    }

    private PipelineProperties props(long backoffBaseMillis) {
        PipelineProperties props = new PipelineProperties();
        props.setConcurrency(1);
        props.setMaxBacklog(100);
        props.setDefaultMaxAttempts(3);
        props.setBackoffBaseMillis(backoffBaseMillis);
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
     * 严重错误重试耗尽进入死信后，结果必须落盘；用同一数据目录启动新进程视角的流水线：
     * 死信任务不重跑、同键再提交识别为重复，其它新键任务在重启后照常处理。
     */
    @Test
    void fatalDeadLetterSurvivesRestartAndIsNeverReRun() throws Exception {
        TaskRepository repo1 = new JdbcTaskRepository(dataDir);
        TaskStore store1 = new TaskStore(repo1);
        TaskPipeline p1 = new TaskPipeline(store1, props());
        AtomicInteger oomExecutions = new AtomicInteger();
        p1.registerHandler("oom", task -> {
            oomExecutions.incrementAndGet();
            throw new OutOfMemoryError("simulated heap exhaustion");
        });

        Task bad = p1.submit("oom-restart-key", "oom", null, 3).task();
        await("DEAD_LETTER persisted",
                () -> repo1.find("oom-restart-key").map(t -> t.getStatus() == TaskStatus.DEAD_LETTER).orElse(false));
        assertEquals(3, oomExecutions.get());
        String persistedReason = repo1.find("oom-restart-key").orElseThrow().getFailureReason();
        assertTrue(persistedReason.contains("OutOfMemoryError"), "死信原因需落盘: " + persistedReason);
        p1.close();

        // 模拟重启：新仓储 / 新流水线接管同一临时数据目录
        TaskRepository repo2 = new JdbcTaskRepository(dataDir);
        TaskStore store2 = new TaskStore(repo2);
        AtomicInteger executionsAfterRestart = new AtomicInteger();
        TaskPipeline p2 = new TaskPipeline(store2, props());
        p2.registerHandler("oom", task -> executionsAfterRestart.incrementAndGet());
        AtomicInteger freshExecutions = new AtomicInteger();
        p2.registerHandler("ok", task -> freshExecutions.incrementAndGet());

        Task loaded = repo2.find("oom-restart-key").orElseThrow();
        assertEquals(TaskStatus.DEAD_LETTER, loaded.getStatus(), "重启后死信终态不变");
        assertEquals(FailureKind.RETRY_EXHAUSTED, loaded.getFailureKind());
        assertTrue(loaded.getFailureReason().contains("OutOfMemoryError"));

        // 同键再提交：必须命中已有死信任务，不产生任何新执行
        SubmitResult repeat = p2.submit("oom-restart-key", "oom", null, 3);
        assertTrue(repeat.duplicate());
        assertSame(store2.find("oom-restart-key").orElseThrow(), repeat.task(),
                "重启后同键提交必须返回新存储中已加载的那条死信任务，不得新建");

        // 重启后的流水线必须正常干活：其它键的任务可以成功
        Task fresh = p2.submit("fresh-after-restart", "ok", null, null).task();
        await("fresh SUCCEEDED",
                () -> repo2.find("fresh-after-restart").map(t -> t.getStatus() == TaskStatus.SUCCEEDED).orElse(false));

        long grace = System.currentTimeMillis() + 300;
        while (System.currentTimeMillis() < grace) {
            Thread.sleep(20);
        }
        assertEquals(0, executionsAfterRestart.get(), "死信任务重启后不得重跑");
        assertEquals(1, freshExecutions.get());
        p2.close();
        // 引用 bad 仅为断言同一任务视图
        assertEquals(3, bad.getAttempts());
    }

    /**
     * 首次执行抛出严重错误后任务处于退避重试（PENDING）期间，进程被强杀（abort）：
     * 重启后必须自动恢复，等退避时间到达后续跑成功，且业务效果只产生一次。
     */
    @Test
    void fatalErrorDuringRetryBackoffResumesAfterHardRestartWithoutDuplicateEffect() throws Exception {
        // 退避 800ms：足够在“回到 PENDING 后、下次重试前”完成强杀；重启后等待也不超过 10s
        TaskRepository repo1 = new JdbcTaskRepository(dataDir);
        TaskStore store1 = new TaskStore(repo1);
        TaskPipeline p1 = new TaskPipeline(store1, props(800));
        AtomicInteger firstAttempts = new AtomicInteger();
        p1.registerHandler("oom", task -> {
            firstAttempts.incrementAndGet();
            throw new OutOfMemoryError("simulated heap exhaustion");
        });

        p1.submit("oom-backoff-key", "oom", null, 3);
        await("first OOM recorded, task back to PENDING",
                () -> repo1.find("oom-backoff-key")
                        .map(t -> t.getStatus() == TaskStatus.PENDING && t.getAttempts() == 1).orElse(false));
        String reasonBeforeKill = repo1.find("oom-backoff-key").orElseThrow().getFailureReason();
        assertTrue(reasonBeforeKill.contains("OutOfMemoryError"));

        p1.abort(); // 退避中强杀：PENDING（含 OOM 失败原因、未到的退避时间）原样落盘
        assertEquals(TaskStatus.PENDING, repo1.find("oom-backoff-key").orElseThrow().getStatus(),
                "强杀时排队退避中的任务不得丢失");

        // 重启：新处理器恢复正常，任务被接管，退避时间到达后续跑成功一次
        TaskRepository repo2 = new JdbcTaskRepository(dataDir);
        TaskStore store2 = new TaskStore(repo2);
        AtomicInteger effects = new AtomicInteger();
        TaskPipeline p2 = new TaskPipeline(store2, props(800));
        p2.registerHandler("oom", task -> effects.incrementAndGet());

        await("SUCCEEDED after restart",
                () -> repo2.find("oom-backoff-key").map(t -> t.getStatus() == TaskStatus.SUCCEEDED).orElse(false));
        Task done = repo2.find("oom-backoff-key").orElseThrow();
        assertEquals(2, done.getAttempts(), "首次严重错误的尝试保留，重启后再执行一次");
        assertEquals(1, effects.get(), "恢复后业务效果只产生一次，不得重复生效");
        p2.close();
        p1.abort();
    }
}
