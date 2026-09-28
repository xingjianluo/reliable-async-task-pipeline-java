package com.github.xingjianluo.reliableasynctaskpipelinejava.tests;

import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 处理器抛出 Error 级别严重错误（如 OutOfMemoryError）时的收敛语义测试。
 * 全部使用纯内存 TaskStore，本地可反复执行，不在工作目录产生任何数据文件。
 */
class FatalErrorHandlingTests {

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
        props.setConcurrency(2);
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

    /**
     * 处理器持续抛出 OutOfMemoryError：任务不得卡在 RUNNING，必须按失败语义在重试耗尽后
     * 进入死信，失败原因里要能看到严重错误的类型与信息。
     */
    @Test
    void fatalErrorFromHandlerConvergesToDeadLetterAndDoesNotStayRunning() throws Exception {
        TaskPipeline p = newPipeline(fastProps(), new TaskStore());
        AtomicInteger executions = new AtomicInteger();
        p.registerHandler("oom", task -> {
            executions.incrementAndGet();
            throw new OutOfMemoryError("simulated heap exhaustion");
        });

        Task task = p.submit("oom-key", "oom", null, 3).task();
        await("DEAD_LETTER", () -> task.getStatus() == TaskStatus.DEAD_LETTER);

        assertEquals(3, task.getAttempts(), "严重错误也要按重试规则尝试 maxAttempts 次");
        assertEquals(3, executions.get());
        assertEquals(FailureKind.RETRY_EXHAUSTED, task.getFailureKind(), "重试耗尽后进入死信");
        assertNotNull(task.getFailureReason());
        assertTrue(task.getFailureReason().contains("OutOfMemoryError"),
                "查询必须能看到严重错误类型: " + task.getFailureReason());
        assertTrue(task.getFailureReason().contains("simulated heap exhaustion"),
                "查询必须能看到严重错误信息: " + task.getFailureReason());
    }

    /**
     * 严重错误不得打死工作线程、不得停止消费：单 worker 下，先让一条任务反复抛
     * OutOfMemoryError 进死信，之后提交的其它键任务必须照常执行成功；
     * 流水线全程存活（未重启）。
     */
    @Test
    void fatalErrorDoesNotStopPipelineFromProcessingLaterTasks() throws Exception {
        PipelineProperties props = fastProps();
        props.setConcurrency(1); // 唯一 worker 被打死时最容易暴露“流水线假活”
        TaskPipeline p = newPipeline(props, new TaskStore());

        AtomicInteger oomExecutions = new AtomicInteger();
        p.registerHandler("oom", task -> {
            oomExecutions.incrementAndGet();
            throw new OutOfMemoryError("simulated heap exhaustion");
        });
        AtomicInteger laterExecutions = new AtomicInteger();
        p.registerHandler("later", task -> laterExecutions.incrementAndGet());

        Task oomTask = p.submit("oom-bad", "oom", null, 3).task();
        await("oom-bad DEAD_LETTER", () -> oomTask.getStatus() == TaskStatus.DEAD_LETTER);
        assertEquals(3, oomExecutions.get());

        // 严重错误之后，进程未重启：后续不同键的任务必须仍然被受理并执行
        Task later1 = p.submit("after-1", "later", null, null).task();
        Task later2 = p.submit("after-2", "later", null, null).task();
        await("after-1 SUCCEEDED", () -> later1.getStatus() == TaskStatus.SUCCEEDED);
        await("after-2 SUCCEEDED", () -> later2.getStatus() == TaskStatus.SUCCEEDED);
        assertEquals(2, laterExecutions.get(), "严重错误之后后续任务必须照常被处理");
    }

    /**
     * 同键连续较劲：第一条因严重错误重试耗尽进死信后，用同一个幂等键再提交必须识别为
     * 重复（返回原死信任务，duplicate=true），不产生新执行；换一个键则立刻能被处理。
     */
    @Test
    void sameKeyAfterFatalDeadLetterIsDeduplicatedAndOtherKeysStillFlow() throws Exception {
        PipelineProperties props = fastProps();
        props.setConcurrency(1);
        TaskPipeline p = newPipeline(props, new TaskStore());

        AtomicInteger oomExecutions = new AtomicInteger();
        p.registerHandler("oom", task -> {
            oomExecutions.incrementAndGet();
            throw new OutOfMemoryError("boom");
        });
        AtomicInteger otherExecutions = new AtomicInteger();
        p.registerHandler("ok", task -> otherExecutions.incrementAndGet());

        Task bad = p.submit("dead-key", "oom", null, 2).task();
        await("dead-key DEAD_LETTER", () -> bad.getStatus() == TaskStatus.DEAD_LETTER);

        SubmitResult repeat = p.submit("dead-key", "oom", null, 2);
        assertTrue(repeat.duplicate(), "同键重复提交命中已收敛的死信任务，必须识别为重复");
        assertSame(bad, repeat.task(), "同一幂等键不得新建第二条任务");

        Task fresh = p.submit("fresh-key", "ok", null, null).task();
        await("fresh-key SUCCEEDED", () -> fresh.getStatus() == TaskStatus.SUCCEEDED);
        assertEquals(2, oomExecutions.get(), "重复投递死信键不得再执行");
        assertEquals(1, otherExecutions.get());
    }

    /**
     * 严重错误只出现一次、之后处理器恢复正常：任务应退避重试并最终成功，
     * 业务效果只产生一次，终态 SUCCEEDED 而不是卡死或误判死信。
     */
    @Test
    void singleFatalErrorFollowedByRecoveryRetriesAndSucceedsOnce() throws Exception {
        TaskPipeline p = newPipeline(fastProps(), new TaskStore());
        AtomicInteger executions = new AtomicInteger();
        AtomicInteger effects = new AtomicInteger();
        p.registerHandler("flaky-oom", task -> {
            int attempt = executions.incrementAndGet();
            if (attempt == 1) {
                throw new OutOfMemoryError("transient exhaustion");
            }
            effects.incrementAndGet();
        });

        Task task = p.submit("flaky-key", "flaky-oom", null, 3).task();
        await("SUCCEEDED", () -> task.getStatus() == TaskStatus.SUCCEEDED);
        assertEquals(2, task.getAttempts(), "首次严重错误后应重试一次并成功");
        assertEquals(1, effects.get(), "重试成功只产生一次业务效果");
    }

    /**
     * 极端情况：严重错误甚至不是处理器抛的，而是从常规收敛路径之外（这里注入 store.tryStart）
     * 逃逸到 worker 主循环。worker 安全网必须兜住——线程不死，后续任务仍然被处理。
     */
    @Test
    void workerSurvivesFatalErrorEscapingOutsideExecute() throws Exception {
        PipelineProperties props = fastProps();
        props.setConcurrency(1);
        // 自定义 store：对特定键在领取执行权时抛 Error（模拟收敛路径之外的致命故障）
        TaskStore store = new TaskStore() {
            @Override
            public long tryStart(String idempotencyKey) {
                if ("escape-key".equals(idempotencyKey)) {
                    throw new OutOfMemoryError("error from infrastructure, not handler");
                }
                return super.tryStart(idempotencyKey);
            }
        };
        TaskPipeline p = newPipeline(props, store);
        AtomicInteger okExecutions = new AtomicInteger();
        p.registerHandler("ok", task -> okExecutions.incrementAndGet());

        Task before = p.submit("before-key", "ok", null, null).task();
        await("before SUCCEEDED", () -> before.getStatus() == TaskStatus.SUCCEEDED);

        // 这个键每次领取都会从 execute 之外抛 Error；验证 worker 不被打死
        p.submit("escape-key", "ok", null, null);
        Thread.sleep(200); // 给 worker 几次轮询机会触发逃逸错误

        Task after = p.submit("after-key", "ok", null, null).task();
        await("after SUCCEEDED despite escaped fatal", () -> after.getStatus() == TaskStatus.SUCCEEDED);
        assertEquals(2, okExecutions.get(), "逃逸出 execute 的严重错误不得停止流水线消费");
    }
}
