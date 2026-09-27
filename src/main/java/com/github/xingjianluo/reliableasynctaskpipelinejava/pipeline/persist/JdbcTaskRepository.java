package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.persist;

import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.FailureKind;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.Task;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 基于嵌入式 H2 文件库的任务仓储。
 *
 * <p>数据库文件位于 {@code <dataDir>/tasks.mv.db}，随应用进程嵌入式启动，无需任何外部服务；
 * {@code WRITE_DELAY=0} 使每次提交同步落盘，进程被直接 kill -9 / 机器断电时，
 * 已确认受理（插入）与已完成（终态流转）的记录都不会丢。
 *
 * <p>所有流转都是带 epoch（fencing token）的条件 UPDATE：存储中 epoch 必须等于执行者
 * 领取任务时拿到的令牌才会生效并自增。重启恢复会把遗留 RUNNING 记录 bump 回 PENDING，
 * 因此中断后才醒来的旧执行拿过期令牌写终态时影响行数为 0，无法覆盖更新后的结果。
 */
public class JdbcTaskRepository implements TaskRepository {

    private static final Logger log = LoggerFactory.getLogger(JdbcTaskRepository.class);
    private static final int REASON_MAX_LENGTH = 2000;
    /** H2 唯一约束冲突 SQLState */
    private static final String UNIQUE_VIOLATION_STATE = "23505";

    private final String jdbcUrl;

    public JdbcTaskRepository(Path dataDir) {
        try {
            Files.createDirectories(dataDir);
        } catch (Exception e) {
            throw new IllegalStateException("cannot create data dir: " + dataDir, e);
        }
        this.jdbcUrl = "jdbc:h2:file:" + dataDir.resolve("tasks").toAbsolutePath()
                + ";WRITE_DELAY=0;LOCK_TIMEOUT=10000";
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("H2 driver not available", e);
        }
        initSchema();
        log.info("[persist] 任务持久化已启用，数据文件目录: {}", dataDir.toAbsolutePath());
    }

    private Connection connect() {
        try {
            return DriverManager.getConnection(jdbcUrl, "sa", "");
        } catch (SQLException e) {
            throw new PersistenceException("cannot connect to " + jdbcUrl, e);
        }
    }

    private void initSchema() {
        String ddl = """
                CREATE TABLE IF NOT EXISTS task (
                    idempotency_key VARCHAR(255) PRIMARY KEY,
                    type            VARCHAR(255) NOT NULL,
                    payload         CLOB,
                    max_attempts    INT NOT NULL,
                    status          VARCHAR(32) NOT NULL,
                    attempts        INT NOT NULL,
                    failure_kind    VARCHAR(64),
                    failure_reason  VARCHAR(2000),
                    next_attempt_at BIGINT NOT NULL,
                    epoch           BIGINT NOT NULL
                )""";
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute(ddl);
        } catch (SQLException e) {
            throw new PersistenceException("schema init failed", e);
        }
    }

    @Override
    public Optional<Task> insertIfAbsent(Task task) {
        String sql = """
                INSERT INTO task (idempotency_key, type, payload, max_attempts, status, attempts,
                                  failure_kind, failure_reason, next_attempt_at, epoch)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""";
        try (Connection c = connect(); PreparedStatement ps = c.prepareStatement(sql)) {
            bind(ps, task);
            ps.executeUpdate();
            return Optional.empty();
        } catch (SQLException e) {
            if (UNIQUE_VIOLATION_STATE.equals(e.getSQLState())) {
                // 键已存在：可能来自上一个（被强杀的）进程，返回持久化中的任务
                return find(task.getIdempotencyKey());
            }
            throw new PersistenceException("insert failed for key=" + task.getIdempotencyKey(), e);
        }
    }

    private void bind(PreparedStatement ps, Task t) throws SQLException {
        ps.setString(1, t.getIdempotencyKey());
        ps.setString(2, t.getType());
        ps.setString(3, t.getPayload());
        ps.setInt(4, t.getMaxAttempts());
        ps.setString(5, t.getStatus().name());
        ps.setInt(6, t.getAttempts());
        ps.setString(7, t.getFailureKind() == null ? null : t.getFailureKind().name());
        ps.setString(8, truncate(t.getFailureReason()));
        ps.setLong(9, t.getNextAttemptAt().toEpochMilli());
        ps.setLong(10, t.getEpoch());
    }

    private static String truncate(String reason) {
        if (reason == null || reason.length() <= REASON_MAX_LENGTH) {
            return reason;
        }
        return reason.substring(0, REASON_MAX_LENGTH);
    }

    @Override
    public Optional<Task> find(String idempotencyKey) {
        String sql = "SELECT * FROM task WHERE idempotency_key = ?";
        try (Connection c = connect(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, idempotencyKey);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(map(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new PersistenceException("find failed for key=" + idempotencyKey, e);
        }
    }

    @Override
    public List<Task> findAll() {
        List<Task> result = new ArrayList<>();
        try (Connection c = connect();
             PreparedStatement ps = c.prepareStatement("SELECT * FROM task");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                result.add(map(rs));
            }
        } catch (SQLException e) {
            throw new PersistenceException("findAll failed", e);
        }
        return result;
    }

    private Task map(ResultSet rs) throws SQLException {
        String kind = rs.getString("failure_kind");
        return new Task(
                rs.getString("idempotency_key"),
                rs.getString("type"),
                rs.getString("payload"),
                rs.getInt("max_attempts"),
                TaskStatus.valueOf(rs.getString("status")),
                rs.getInt("attempts"),
                kind == null ? null : FailureKind.valueOf(kind),
                rs.getString("failure_reason"),
                Instant.ofEpochMilli(rs.getLong("next_attempt_at")),
                rs.getLong("epoch"));
    }

    @Override
    public long claimForRun(String idempotencyKey, long expectedEpoch) {
        // 仅 PENDING、令牌匹配、且已到可执行时间才能领取；领取即 bump epoch（新令牌）
        String sql = """
                UPDATE task SET status = 'RUNNING', attempts = attempts + 1, epoch = epoch + 1
                WHERE idempotency_key = ? AND status = 'PENDING'
                  AND epoch = ? AND next_attempt_at <= ?""";
        try (Connection c = connect(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, idempotencyKey);
            ps.setLong(2, expectedEpoch);
            ps.setLong(3, System.currentTimeMillis());
            int rows = ps.executeUpdate();
            return rows == 1 ? expectedEpoch + 1 : -1;
        } catch (SQLException e) {
            throw new PersistenceException("claim failed for key=" + idempotencyKey, e);
        }
    }

    @Override
    public boolean completeSuccess(String idempotencyKey, long expectedEpoch) {
        String sql = "UPDATE task SET status = 'SUCCEEDED', epoch = epoch + 1 "
                + "WHERE idempotency_key = ? AND status = 'RUNNING' AND epoch = ?";
        return conditionalUpdate(sql, idempotencyKey, expectedEpoch, "completeSuccess");
    }

    @Override
    public boolean scheduleRetry(String idempotencyKey, long expectedEpoch,
                                 String failureKind, String failureReason, long nextAttemptAtMillis) {
        String sql = """
                UPDATE task SET status = 'PENDING', failure_kind = ?, failure_reason = ?,
                                next_attempt_at = ?, epoch = epoch + 1
                WHERE idempotency_key = ? AND status = 'RUNNING' AND epoch = ?""";
        try (Connection c = connect(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, failureKind);
            ps.setString(2, truncate(failureReason));
            ps.setLong(3, nextAttemptAtMillis);
            ps.setString(4, idempotencyKey);
            ps.setLong(5, expectedEpoch);
            return ps.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new PersistenceException("scheduleRetry failed for key=" + idempotencyKey, e);
        }
    }

    @Override
    public boolean moveToDeadLetter(String idempotencyKey, long expectedEpoch,
                                    String failureKind, String failureReason) {
        String sql = """
                UPDATE task SET status = 'DEAD_LETTER', failure_kind = ?, failure_reason = ?,
                                epoch = epoch + 1
                WHERE idempotency_key = ? AND status = 'RUNNING' AND epoch = ?""";
        try (Connection c = connect(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, failureKind);
            ps.setString(2, truncate(failureReason));
            ps.setString(3, idempotencyKey);
            ps.setLong(4, expectedEpoch);
            return ps.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new PersistenceException("moveToDeadLetter failed for key=" + idempotencyKey, e);
        }
    }

    private boolean conditionalUpdate(String sql, String idempotencyKey,
                                      long expectedEpoch, String op) {
        try (Connection c = connect(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, idempotencyKey);
            ps.setLong(2, expectedEpoch);
            return ps.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new PersistenceException(op + " failed for key=" + idempotencyKey, e);
        }
    }

    @Override
    public boolean deleteIfPending(String idempotencyKey) {
        try (Connection c = connect();
             PreparedStatement ps = c.prepareStatement(
                     "DELETE FROM task WHERE idempotency_key = ? AND status = 'PENDING'")) {
            ps.setString(1, idempotencyKey);
            return ps.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new PersistenceException("delete failed for key=" + idempotencyKey, e);
        }
    }
}
