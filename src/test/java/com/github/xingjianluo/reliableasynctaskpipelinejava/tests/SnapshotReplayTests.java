package com.github.xingjianluo.reliableasynctaskpipelinejava.tests;

import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 快照压缩后的日志重放：终态与退避等待状态必须等价还原（SUBMITTED 之后直接跟状态事件）。
 */
class SnapshotReplayTests {

    private static final Logger log = LoggerFactory.getLogger(SnapshotReplayTests.class);

    @Test
    void compactedSnapshotsReplayEquivalentState(@TempDir Path dir) {
        Path file = dir.resolve("p.log");
        long future = System.currentTimeMillis() + 60_000;
        try (FileTaskJournal j = new FileTaskJournal(file)) {
            j.compact(List.of(
                    new TaskJournal.Event.Submitted("ok", "t", "x", 3),
                    new TaskJournal.Event.Succeeded("ok", 1),
                    new TaskJournal.Event.Submitted("dead", "t", null, 2),
                    new TaskJournal.Event.DeadLetter("dead", 1, FailureKind.NON_RETRYABLE, "bad"),
                    new TaskJournal.Event.Submitted("waiting", "t", null, 3),
                    new TaskJournal.Event.RetryPending("waiting", 2, FailureKind.RETRYABLE, "transient", future)));
        }
        try (FileTaskJournal j2 = new FileTaskJournal(file)) {
            TaskStore s = new TaskStore(j2);
            assertEquals(TaskStatus.SUCCEEDED, s.find("ok").orElseThrow().getStatus(),
                    "压缩后的成功终态必须还原，不能退回 PENDING 重跑");
            assertEquals(1, s.find("ok").orElseThrow().getAttempts());

            Task dead = s.find("dead").orElseThrow();
            assertEquals(TaskStatus.DEAD_LETTER, dead.getStatus());
            assertEquals(FailureKind.NON_RETRYABLE, dead.getFailureKind());
            assertEquals("bad", dead.getFailureReason());

            Task waiting = s.find("waiting").orElseThrow();
            assertEquals(TaskStatus.PENDING, waiting.getStatus(), "等待退避的任务保持 PENDING");
            assertEquals(future, waiting.getNextAttemptAt().toEpochMilli(), "下次执行时间必须还原");

            // 终态任务不会被恢复执行：没有任何 PENDING 入队候选是 ok/dead
            assertTrue(s.pendingKeys().contains("waiting"));
            assertFalse(s.pendingKeys().contains("ok"));
            assertFalse(s.pendingKeys().contains("dead"));
            log.info("[test] 压缩快照重放：SUCCEEDED/DEAD_LETTER/退避PENDING 均等价还原");
        }
    }
}
