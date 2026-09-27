package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 基于本地文件的追加式任务日志（WAL）：
 * <ul>
 *   <li>每次状态流转追加一行 JSON 并 {@code force(true)}（含目录 fsync 的最佳努力），
 *       进程被强制结束（kill -9 / 机器重启）后已确认受理与已终态的记录不丢；</li>
 *   <li>{@link #replay()} 顺序重放全部事件重建状态；读到损坏的半行（崩溃点）即截断，
 *       之前完整事件全部有效；</li>
 *   <li>启动重放后将当前状态压缩成快照重写日志，避免文件无限增长；</li>
 *   <li>用 {@link FileLock} 阻止两个流水线实例同时写同一日志文件。</li>
 * </ul>
 */
public class FileTaskJournal implements TaskJournal {
    private static final Logger log = LoggerFactory.getLogger(FileTaskJournal.class);

    private final Path file;
    private final ObjectMapper mapper;
    /** 保持通道打开以持有排他文件锁，并用于每次追加后的 force */
    private FileChannel channel;
    private FileLock lock;

    public FileTaskJournal(Path path) {
        this(path, new ObjectMapper());
    }

    public FileTaskJournal(Path path, ObjectMapper mapper) {
        try {
            this.file = path;
            this.mapper = mapper;
            Path parent = file.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            this.channel = FileChannel.open(file,
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            FileLock acquired;
            try {
                acquired = channel.tryLock();
            } catch (OverlappingFileLockException e) {
                acquired = null; // 同一 JVM 内已有实例持有该文件锁
            }
            this.lock = acquired;
            if (this.lock == null) {
                channel.close();
                throw new IllegalStateException("another pipeline instance holds the journal lock: " + file);
            }
            long truncated = truncateTrailingCorruption();
            if (truncated > 0) {
                log.warn("[journal] {} 截断崩溃残留的不完整日志 {} 字节", file, truncated);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot open task journal: " + path, e);
        }
    }

    /** 检查文件末尾是否存在不完整（无法解析为 JSON）的行，截断到最后一个换行符 */
    private long truncateTrailingCorruption() throws IOException {
        long size = channel.size();
        if (size == 0) {
            return 0;
        }
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        if (lines.isEmpty()) {
            return 0;
        }
        // 最后一行无换行（崩溃时写了一半）或无法解析：视为损坏尾部
        byte[] all = Files.readAllBytes(file);
        boolean endsWithNewline = all[all.length - 1] == '\n';
        String last = lines.get(lines.size() - 1);
        boolean parseable = parseLine(last) != null;
        if (endsWithNewline && parseable) {
            return 0;
        }
        long validBytes = 0;
        for (int i = 0; i < lines.size() - 1; i++) {
            validBytes += lines.get(i).getBytes(StandardCharsets.UTF_8).length + 1;
        }
        channel.truncate(validBytes);
        channel.force(true);
        return size - validBytes;
    }

    private synchronized void append(JsonNode node) {
        try {
            byte[] bytes = mapper.writeValueAsBytes(node);
            byte[] line = new byte[bytes.length + 1];
            System.arraycopy(bytes, 0, line, 0, bytes.length);
            line[bytes.length] = '\n';
            channel.position(channel.size());
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(line);
            while (buf.hasRemaining()) {
                channel.write(buf);
            }
            channel.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to append journal event: " + file, e);
        }
    }

    private com.fasterxml.jackson.databind.node.ObjectNode node(String op, String key) {
        var n = mapper.createObjectNode();
        n.put("op", op);
        n.put("key", key);
        return n;
    }

    @Override
    public void submitted(String key, String type, String payload, int maxAttempts) {
        var n = node("SUBMITTED", key);
        n.put("type", type);
        n.put("payload", payload);
        n.put("maxAttempts", maxAttempts);
        append(n);
    }

    @Override
    public void started(String key, int attempts, int epoch) {
        var n = node("STARTED", key);
        n.put("attempts", attempts);
        n.put("epoch", epoch);
        append(n);
    }

    @Override
    public void succeeded(String key, int epoch) {
        var n = node("SUCCEEDED", key);
        n.put("epoch", epoch);
        append(n);
    }

    @Override
    public void retryPending(String key, int epoch, FailureKind kind, String reason, long nextAttemptAtMillis) {
        var n = node("RETRY_PENDING", key);
        n.put("epoch", epoch);
        n.put("kind", kind.name());
        n.put("reason", reason);
        n.put("nextAttemptAt", nextAttemptAtMillis);
        append(n);
    }

    @Override
    public void deadLetter(String key, int epoch, FailureKind kind, String reason) {
        var n = node("DEAD_LETTER", key);
        n.put("epoch", epoch);
        n.put("kind", kind.name());
        n.put("reason", reason);
        append(n);
    }

    @Override
    public void reclaimed(String key, int attempts, int epoch, String reason) {
        var n = node("RECLAIMED", key);
        n.put("attempts", attempts);
        n.put("epoch", epoch);
        n.put("reason", reason);
        append(n);
    }

    @Override
    public void removed(String key) {
        append(node("REMOVED", key));
    }

    @Override
    public synchronized List<Event> replay() {
        List<Event> events = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            int lineNo = 0;
            while ((line = reader.readLine()) != null) {
                lineNo++;
                if (line.isBlank()) {
                    continue;
                }
                Event event = parseLine(line);
                if (event == null) {
                    log.warn("[journal] {} 第 {} 行无法解析，已忽略（重放以可解析行为准）", file, lineNo);
                    continue;
                }
                events.add(event);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to replay journal: " + file, e);
        }
        return events;
    }

    /** 用当前状态快照（每键一条 SUBMITTED + 一条最新状态事件）重写日志，避免无限增长 */
    public synchronized void compact(List<Event> snapshot) {
        try {
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            try (java.io.OutputStream out = Files.newOutputStream(tmp,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
                for (Event event : snapshot) {
                    out.write(mapper.writeValueAsBytes(toNode(event)));
                    out.write('\n');
                }
                out.flush();
            }
            // 先释放旧文件的通道与锁，再原子替换，然后在新文件上重新加锁
            lock.release();
            channel.close();
            try {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
            }
            channel = FileChannel.open(file,
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            channel.force(true);
            lock = channel.tryLock();
            if (lock == null) {
                throw new IllegalStateException("lost journal lock after compaction: " + file);
            }
            log.info("[journal] {} 快照压缩完成，保留事件 {} 条", file, snapshot.size());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to compact journal: " + file, e);
        }
    }

    @Override
    public void close() {
        try {
            if (channel == null || !channel.isOpen()) {
                return;
            }
            if (lock != null && lock.isValid()) {
                lock.release();
            }
            channel.force(true);
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to close journal: " + file, e);
        }
    }

    private JsonNode toNode(Event event) {        return switch (event) {
            case Event.Submitted s -> {
                var n = node("SUBMITTED", s.key());
                n.put("type", s.type());
                n.put("payload", s.payload());
                n.put("maxAttempts", s.maxAttempts());
                yield n;
            }
            case Event.Started s -> {
                var n = node("STARTED", s.key());
                n.put("attempts", s.attempts());
                n.put("epoch", s.epoch());
                yield n;
            }
            case Event.Succeeded s -> {
                var n = node("SUCCEEDED", s.key());
                n.put("epoch", s.epoch());
                yield n;
            }
            case Event.RetryPending r -> {
                var n = node("RETRY_PENDING", r.key());
                n.put("epoch", r.epoch());
                n.put("kind", r.kind().name());
                n.put("reason", r.reason());
                n.put("nextAttemptAt", r.nextAttemptAtMillis());
                yield n;
            }
            case Event.DeadLetter d -> {
                var n = node("DEAD_LETTER", d.key());
                n.put("epoch", d.epoch());
                n.put("kind", d.kind().name());
                n.put("reason", d.reason());
                yield n;
            }
            case Event.Reclaimed r -> {
                var n = node("RECLAIMED", r.key());
                n.put("attempts", r.attempts());
                n.put("epoch", r.epoch());
                n.put("reason", r.reason());
                yield n;
            }
            case Event.Removed r -> node("REMOVED", r.key());
        };
    }

    private Event parseLine(String line) {
        try {
            JsonNode n = mapper.readTree(line);
            String op = n.path("op").asText();
            String key = n.path("key").asText();
            return switch (op) {
                case "SUBMITTED" -> new Event.Submitted(key, n.path("type").asText(null),
                        n.path("payload").asText(null), n.path("maxAttempts").asInt(1));
                case "STARTED" -> new Event.Started(key, n.path("attempts").asInt(), n.path("epoch").asInt());
                case "SUCCEEDED" -> new Event.Succeeded(key, n.path("epoch").asInt());
                case "RETRY_PENDING" -> new Event.RetryPending(key, n.path("epoch").asInt(),
                        FailureKind.valueOf(n.path("kind").asText(FailureKind.RETRYABLE.name())),
                        n.path("reason").asText(null), n.path("nextAttemptAt").asLong(0));
                case "DEAD_LETTER" -> new Event.DeadLetter(key, n.path("epoch").asInt(),
                        FailureKind.valueOf(n.path("kind").asText(FailureKind.NON_RETRYABLE.name())),
                        n.path("reason").asText(null));
                case "RECLAIMED" -> new Event.Reclaimed(key, n.path("attempts").asInt(),
                        n.path("epoch").asInt(), n.path("reason").asText(null));
                case "REMOVED" -> new Event.Removed(key);
                default -> null;
            };
        } catch (Exception e) {
            return null;
        }
    }
}
