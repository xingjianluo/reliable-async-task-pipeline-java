package com.github.xingjianluo.reliableasynctaskpipelinejava.tests;

import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 存储层租约纪元（fencing epoch）与中断回收测试。纯内存，不依赖外部服务。
 */
class TaskStoreTests {

    private static final Logger log = LoggerFactory.getLogger(TaskStoreTests.class);

    @Test
    void staleEpochCannotWriteTerminalState() {
        TaskStore store = new TaskStore();
        store.createIfAbsent(new Task("fence-key", "t", null, 3));

        int epoch = store.tryStart("fence-key");
        assertEquals(1, epoch);
        assertFalse(store.markSucceeded("fence-key", epoch - 1), "旧纪元（0）的成功回写必须被拒绝");
        assertFalse(store.markDeadLetter("fence-key", epoch - 1, FailureKind.NON_RETRYABLE, "x"),
                "旧纪元的死信回写必须被拒绝");
        assertTrue(store.markSucceeded("fence-key", epoch), "当前纪元可以写入终态");
        assertEquals(TaskStatus.SUCCEEDED, store.find("fence-key").orElseThrow().getStatus());
        log.info("[test] idempotencyKey=fence-key 旧纪元回写被拒绝，当前纪元写入成功");
    }

    @Test
    void reclaimBumpsEpochAndOldLeaseIsInvalid() {
        TaskStore store = new TaskStore();
        store.createIfAbsent(new Task("reclaim-key", "t", null, 3));
        int oldEpoch = store.tryStart("reclaim-key"); // 模拟执行已经开始
        assertEquals(1, oldEpoch);

        int reclaimed = store.reclaimInterruptedTasks(); // 模拟崩溃回收：RUNNING -> PENDING
        assertEquals(1, reclaimed);
        Task task = store.find("reclaim-key").orElseThrow();
        assertEquals(TaskStatus.PENDING, task.getStatus(), "中断任务需回收为 PENDING");
        int newEpoch = store.tryStart("reclaim-key"); // 回收 bump 一次，重新执行再 bump 一次
        assertTrue(newEpoch > oldEpoch, "重新获得执行权应拿到更大的新纪元");

        // 旧执行（epoch=1）此刻才醒来尝试回写：必须失败，不能覆盖新执行
        assertFalse(store.markSucceeded("reclaim-key", oldEpoch), "被回收的旧执行不得覆盖新状态");
        assertFalse(store.markRetryPending("reclaim-key", oldEpoch, FailureKind.RETRYABLE, "late", 0),
                "被回收的旧执行不得把任务退回 PENDING");
        assertTrue(store.markSucceeded("reclaim-key", newEpoch), "新执行可以正常写入成功");
        assertEquals(TaskStatus.SUCCEEDED, task.getStatus());
        log.info("[test] idempotencyKey=reclaim-key 回收后纪元 {} -> {}，旧执行回写被拒绝，终态={}",
                oldEpoch, newEpoch, task.getStatus());
    }

    @Test
    void walReopenRestoresTerminalStatesAndReclaimsRunningWithNewEpoch(@TempDir Path dir) {
        Path file = dir.resolve("store.log");
        try (FileTaskJournal journal = new FileTaskJournal(file)) {
            TaskStore store = new TaskStore(journal);
            store.createIfAbsent(new Task("done", "t", "p", 3));
            int e1 = store.tryStart("done");
            assertTrue(store.markSucceeded("done", e1));

            store.createIfAbsent(new Task("stuck", "t", null, 2));
            int e2 = store.tryStart("stuck"); // 崩溃点：RUNNING
            assertEquals(1, e2);
        }

        // 重新打开日志（模拟新进程）：重放重建，RUNNING 被回收并 bump 纪元
        try (FileTaskJournal reopened = new FileTaskJournal(file)) {
            TaskStore restored = new TaskStore(reopened);
            assertEquals(TaskStatus.SUCCEEDED, restored.find("done").orElseThrow().getStatus(),
                    "已成功任务必须从日志还原，重启后不会重跑");
            Task stuck = restored.find("stuck").orElseThrow();
            assertEquals(TaskStatus.PENDING, stuck.getStatus(), "崩溃时 RUNNING 的任务回收为 PENDING");
            assertEquals(1, stuck.getAttempts(), "attempts 从日志还原");
            int newEpoch = restored.tryStart("stuck");
            assertTrue(newEpoch > 1, "回收后纪元必须继续递增，旧纪元(1)失效");
            assertTrue(restored.markSucceeded("stuck", newEpoch));
            assertFalse(restored.markSucceeded("stuck", 1), "旧纪元回写必须被拒绝");
            log.info("[test] WAL 重开：SUCCEEDED 保持、RUNNING 回收并 bump 纪元，旧纪元(1)回写被拒，新纪元={}",
                    newEpoch);
        }
    }
}
