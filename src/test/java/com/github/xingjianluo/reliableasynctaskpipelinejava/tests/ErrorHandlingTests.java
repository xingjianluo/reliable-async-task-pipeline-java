package com.github.xingjianluo.reliableasynctaskpipelinejava.tests;

import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.*;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.persist.JdbcTaskRepository;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.persist.TaskRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 处理器抛出严重错误（Error 级别，如 {@link OutOfMemoryError}）时的收敛语义测试。
 *
 * <p>覆盖：
 * <ul>
 *   <li>严重错误按失败语义收敛：任务不卡在 RUNNING，重试耗尽进死信，失败原因可查询；
 *       首次错误若后续成功，则正常 SUCCEEDED 且只生效一次；</li>
 *   <li>单条任务的严重错误不会杀死工作线程、不会带停流水线：之后提交的任务照常处理；</li>
 *   <li>严重错误后退避等待中的任务经强杀/重启恢复，不丢失、不重复生效。</li>
 * </ul>
 *
 * <p>全部本地运行：内存用例不落盘；持久化用例使用 JUnit {@link TempDir} 临时目录，
 * 不读取、不改动项目工作目录中的数据文件，每次运行相互隔离，可反复执行。
 */
class ErrorHandlingTests {

    private static final Logger log = LoggerFactory.getLogger(ErrorHandlingTests.class);

    private TaskPipeline pipeline;

    private PipelineProperties props() {
        PipelineProperties props = new PipelineProperties();
        props.setConcurrency(1);
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
     * 处理器持续抛出 OutOfMemoryError：任务必须按重试规则尝试 maxAttempts 次后进入死信，
     * 不能一直停在 RUNNING，查询能看到 RETRY_EXHAUSTED 与含 OutOfMemoryError 的失败原因。
     */
    @Test
    void outOfMemoryErrorIsRetriedAndConvergesToDeadLetterInsteadOfStuckRunning() throws Exception {
        pipeline = new TaskPipeline(new TaskStore(), props());
        AtomicInteger executions = new AtomicInteger();
        pipeline.registerHandler("t", task -> {
            int attempt = executions.incrementAndGet();
            log.info("[test] idempotencyKey=oom-key attempt={} 主动抛出 OutOfMemoryError", attempt);
            throw new OutOfMemoryError("simulated heap exhaustion");
        });

        Task task = pipeline.submit("oom-key", "t", null, 2).task();

        // 中途不应观察到“卡死”：最终收敛到 DEAD_LETTER（若卡在 RUNNING 这里会超时失败）
        await("DEAD_LETTER after severe errors", () -> task.getStatus() == TaskStatus.DEAD_LETTER);
        log.info("[test] idempotencyKey=oom-key 终态={} attempts={} failureKind={} reason={}",
                task.getStatus(), task.getAttempts(), task.getFailureKind(), task.getFailureReason());

        assertEquals(2, task.getAttempts(), "严重错误也必须按重试规则尝试到上限");
        assertEquals(2, executions.get());
        assertEquals(FailureKind.RETRY_EXHAUSTED, task.getFailureKind(), "重试耗尽需保留可区分的失败分类");
        assertNotNull(task.getFailureReason(), "严重错误的失败原因必须可查询");
        assertTrue(task.getFailureReason().contains("OutOfMemoryError"),
                "失败原因需体现严重错误类型，实际: " + task.getFailureReason());
        assertTrue(task.getFailureReason().contains("simulated heap exhaustion"),
                "失败原因需保留原始错误信息，实际: " + task.getFailureReason());
    }

    /**
     * 单条任务抛出严重错误后，流水线必须继续消费：随后提交的其他键任务正常执行成功。
     * 单工作线程场景最能暴露“worker 被 Error 杀死、队列静默停摆”的问题。
     */
    @Test
    void severeErrorInOneTaskDoesNotStopProcessingOfLaterTasks() throws Exception {
        pipeline = new TaskPipeline(new TaskStore(), props());
        pipeline.registerHandler("boom", task -> {
            log.info("[test] idempotencyKey=boom-key 处理器抛出 OutOfMemoryError，maxAttempts=1");
            throw new OutOfMemoryError("simulated heap exhaustion");
        });
        AtomicInteger laterEffects = new AtomicInteger();
        pipeline.registerHandler("ok", task -> {
            laterEffects.incrementAndGet();
            log.info("[test] idempotencyKey={} 在严重错误之后正常执行", task.getIdempotencyKey());
        });

        Task boom = pipeline.submit("boom-key", "boom", null, 1).task();
        await("boom-key DEAD_LETTER", () -> boom.getStatus() == TaskStatus.DEAD_LETTER);
        assertEquals(TaskStatus.DEAD_LETTER, boom.getStatus(), "严重错误任务自身必须先收敛到明确终态");

        // 严重错误之后再提交别的键：不能停在 PENDING 没人处理
        Task later = pipeline.submit("later-key", "ok", null, null).task();
        await("later-key SUCCEEDED", () -> later.getStatus() == TaskStatus.SUCCEEDED);
        log.info("[test] 严重错误后 later-key 终态={} effects={}", later.getStatus(), laterEffects.get());
        assertEquals(1, laterEffects.get(), "流水线在严重错误后必须继续正常消费后续任务");

        // 再来一个任务确认消费稳定（不是只侥幸处理了一条）
        Task another = pipeline.submit("another-key", "ok", null, null).task();
        await("another-key SUCCEEDED", () -> another.getStatus() == TaskStatus.SUCCEEDED);
        assertEquals(2, laterEffects.get());
    }

    /**
     * 首次执行抛 OutOfMemoryError、退避后的重试成功：任务 SUCCEEDED，业务效果恰好一次。
     */
    @Test
    void severeErrorOnFirstAttemptCanBeRetriedToSuccessWithSingleEffect() throws Exception {
        pipeline = new TaskPipeline(new TaskStore(), props());
        AtomicInteger executions = new AtomicInteger();
        AtomicInteger effects = new AtomicInteger();
        pipeline.registerHandler("t", task -> {
            int attempt = executions.incrementAndGet();
            if (attempt == 1) {
                log.info("[test] idempotencyKey=recover-key attempt=1 抛出 OutOfMemoryError");
                throw new OutOfMemoryError("simulated heap exhaustion");
            }
            effects.incrementAndGet();
            log.info("[test] idempotencyKey=recover-key attempt={} 成功生效 effects={}", attempt, effects.get());
        });

        Task task = pipeline.submit("recover-key", "t", null, 3).task();
        await("SUCCEEDED", () -> task.getStatus() == TaskStatus.SUCCEEDED);
        log.info("[test] idempotencyKey=recover-key 终态={} attempts={} effects={}",
                task.getStatus(), task.getAttempts(), effects.get());

        assertEquals(2, task.getAttempts(), "第一次按严重错误失败、重试成功");
        assertEquals(2, executions.get());
        assertEquals(1, effects.get(), "失败尝试不产生业务效果，成功提交恰好一次");
    }

    /**
     * 任务首次执行抛出 OutOfMemoryError 后处于退避等待（PENDING）时进程被强杀（abort）：
     * 重启后任务不丢、恢复续跑成功，业务效果只产生一次，失败历史仍可从状态演进中看出。
     */
    @Test
    void severeFailurePendingRetrySurvivesHardRestartWithoutDuplicateEffect(@TempDir Path dataDir) throws Exception {
        // 退避基数设大：确保 abort 发生在“严重错误已落盘、重试尚未执行”的窗口内，避免竞态
        PipelineProperties props = props();
        props.setBackoffBaseMillis(5_000);

        TaskRepository repo1 = new JdbcTaskRepository(dataDir);
        TaskStore store1 = new TaskStore(repo1);
        TaskPipeline p1 = new TaskPipeline(store1, props);

        AtomicInteger firstAttempts = new AtomicInteger();
        p1.registerHandler("t", task -> {
            firstAttempts.incrementAndGet();
            log.info("[test] idempotencyKey=crash-key attempt=1 抛出 OutOfMemoryError 后等待退避");
            throw new OutOfMemoryError("simulated heap exhaustion");
        });

        Task task = p1.submit("crash-key", "t", null, 3).task();
        await("PENDING retry after severe error persisted",
                () -> repo1.find("crash-key").map(t -> t.getStatus() == TaskStatus.PENDING
                        && t.getAttempts() == 1).orElse(false));
        Task afterError = repo1.find("crash-key").orElseThrow();
        assertEquals(FailureKind.RETRYABLE, afterError.getFailureKind());
        assertTrue(afterError.getFailureReason().contains("OutOfMemoryError"));
        log.info("[test] 强杀前 crash-key status=PENDING attempts=1 reason={}", afterError.getFailureReason());

        p1.abort(); // 模拟 kill -9：不做任何在途回收

        // 重启：新仓储/新流水线接管同一临时数据目录
        TaskRepository repo2 = new JdbcTaskRepository(dataDir);
        TaskStore store2 = new TaskStore(repo2);
        TaskPipeline p2 = new TaskPipeline(store2, props());
        AtomicInteger effects = new AtomicInteger();
        p2.registerHandler("t", task2 -> {
            effects.incrementAndGet();
            log.info("[test] idempotencyKey=crash-key 重启后续跑成功 effects={}", effects.get());
        });

        await("SUCCEEDED after restart",
                () -> repo2.find("crash-key").map(t -> t.getStatus() == TaskStatus.SUCCEEDED).orElse(false));
        Task recoveredView = repo2.find("crash-key").orElseThrow();
        log.info("[test] 重启后 crash-key 终态={} attempts={} effects={}",
                recoveredView.getStatus(), recoveredView.getAttempts(), effects.get());
        assertEquals(1, effects.get(), "重启恢复后业务效果只允许产生一次");
        assertTrue(recoveredView.getAttempts() >= 2, "严重错误的首次尝试与恢复后的成功尝试都要计入 attempts");
        assertEquals(TaskStatus.SUCCEEDED, recoveredView.getStatus(), "同一幂等键最终收敛到可解释的明确状态");

        p2.close();
        p1.abort();
    }
}
