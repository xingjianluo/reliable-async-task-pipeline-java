package com.github.xingjianluo.reliableasynctaskpipelinejava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 文件 WAL 测试：追加持久化、重放、崩溃半行截断、快照压缩。使用临时目录，可重复跑。
 */
class FileTaskJournalTests {

    private static final Logger log = LoggerFactory.getLogger(FileTaskJournalTests.class);

    @TempDir
    Path dir;

    @Test
    void eventsArePersistedAndReplayedInOrder() throws Exception {
        Path file = dir.resolve("journal.log");
        try (FileTaskJournal journal = new FileTaskJournal(file)) {
            journal.submitted("k1", "t", "p1", 3);
            journal.started("k1", 1, 1);
            journal.submitted("k2", "t", "p2", 2);
            journal.succeeded("k1", 1);
            journal.deadLetter("k2", 1, FailureKind.NON_RETRYABLE, "bad");
        }
        assertTrue(Files.size(file) > 0, "事件必须落盘");

        try (FileTaskJournal reopened = new FileTaskJournal(file)) {
            List<TaskJournal.Event> events = reopened.replay();
            assertEquals(5, events.size());
            assertInstanceOf(TaskJournal.Event.Submitted.class, events.get(0));
            assertEquals("k1", events.get(0).key());
            assertInstanceOf(TaskJournal.Event.Succeeded.class, events.get(3));
            assertInstanceOf(TaskJournal.Event.DeadLetter.class, events.get(4));
            log.info("[test] 重放 {} 条事件，顺序与内容正确", events.size());
        }
    }

    @Test
    void tornTrailingLineFromCrashIsTruncatedAndValidEventsSurvive() throws IOException {
        Path file = dir.resolve("torn.log");
        try (FileTaskJournal journal = new FileTaskJournal(file)) {
            journal.submitted("survive", "t", null, 1);
            journal.started("survive", 1, 1);
        }
        // 模拟崩溃时写了一半的行（无结尾换行、且不是合法 JSON）
        Files.writeString(file, "{\"op\":\"STARTED\",\"key\":\"su",
                StandardCharsets.UTF_8, java.nio.file.StandardOpenOption.APPEND);

        try (FileTaskJournal reopened = new FileTaskJournal(file)) {
            List<TaskJournal.Event> events = reopened.replay();
            assertEquals(2, events.size(), "损坏半行之前的完整事件必须全部保留");
            List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
            assertEquals(2, lines.size(), "重开时损坏尾部应被截断");
            log.info("[test] 崩溃残留半行已截断，保留 {} 条完整事件", events.size());
        }
    }

    @Test
    void compactRewritesSnapshotEquivalentState() throws IOException {
        Path file = dir.resolve("compact.log");
        try (FileTaskJournal journal = new FileTaskJournal(file)) {
            journal.submitted("k", "t", "payload-x", 5);
            journal.started("k", 1, 1);
            journal.retryPending("k", 1, FailureKind.RETRYABLE, "r1", 123L);
            journal.started("k", 2, 2);
            journal.succeeded("k", 2);
            // 压缩为每键一条 SUBMITTED + 一条最新状态
            journal.compact(List.of(
                    new TaskJournal.Event.Submitted("k", "t", "payload-x", 5),
                    new TaskJournal.Event.Succeeded("k", 2)));
        }
        assertEquals(2, Files.readAllLines(file, StandardCharsets.UTF_8).size(), "快照应只有 2 行");

        try (FileTaskJournal reopened = new FileTaskJournal(file)) {
            List<TaskJournal.Event> events = reopened.replay();
            assertEquals(2, events.size());
            TaskJournal.Event.Submitted submitted = (TaskJournal.Event.Submitted) events.get(0);
            assertEquals("payload-x", submitted.payload());
            assertEquals(5, submitted.maxAttempts());
            assertInstanceOf(TaskJournal.Event.Succeeded.class, events.get(1));
            // 压缩后追加仍正常（锁与通道已在新文件上重开）
            reopened.removed("other");
            assertEquals(3, reopened.replay().size());
            log.info("[test] 快照压缩后状态等价，且追加通道工作正常");
        }
    }

    @Test
    void secondInstanceOnSameFileIsRejected() throws IOException {
        Path file = dir.resolve("locked.log");
        try (FileTaskJournal first = new FileTaskJournal(file)) {
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> new FileTaskJournal(file, new ObjectMapper()));
            assertTrue(ex.getMessage().contains("journal lock"));
            log.info("[test] 同文件第二个实例被文件锁拒绝: {}", ex.getMessage());
        }
    }
}
