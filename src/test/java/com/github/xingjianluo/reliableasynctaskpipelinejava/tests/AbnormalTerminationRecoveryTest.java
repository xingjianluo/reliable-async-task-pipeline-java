package com.github.xingjianluo.reliableasynctaskpipelinejava.tests;

import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.FailureClassification;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.JsonFileTaskStore;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.TaskRecord;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.TaskStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 模拟进程异常终止（RUNNING 状态下直接丢弃内存态、只留快照文件）后，
 * 新进程基于同一存储目录能把在途任务恢复为可重试状态。
 */
class AbnormalTerminationRecoveryTest {

    @TempDir
    Path tempDir;

    @Test
    void runningTaskLeftByDeadProcessIsResetToPendingOnNextStartup() {
        String dir = tempDir.toString();

        // 旧进程：任务被认领为 RUNNING 并已持久化，随后“崩溃”（对象被 GC，无优雅停机）
        JsonFileTaskStore oldStore = new JsonFileTaskStore(dir);
        TaskRecord record = new TaskRecord();
        record.setIdempotencyKey("crashed-key");
        record.setPayload("payload");
        record.setStatus(TaskStatus.PENDING);
        record.setAttempts(2);
        record.setCreatedAt(1L);
        record.setUpdatedAt(1L);
        oldStore.putIfAbsent(record);
        TaskRecord claimed = oldStore.claimForExecution("crashed-key");
        assertThat(claimed.getStatus()).isEqualTo(TaskStatus.RUNNING);
        assertThat(claimed.getExecutionEpoch()).isEqualTo(1L);

        // 新进程：从同一快照文件构造存储
        JsonFileTaskStore newStore = new JsonFileTaskStore(dir);
        assertThat(newStore.find("crashed-key").getStatus())
                .as("快照文件中确实遗留 RUNNING")
                .isEqualTo(TaskStatus.RUNNING);

        List<String> reset = newStore.resetRunningToPending("recovered after abnormal termination");

        assertThat(reset).containsExactly("crashed-key");
        TaskRecord recovered = newStore.find("crashed-key");
        assertThat(recovered.getStatus()).isEqualTo(TaskStatus.PENDING);
        assertThat(recovered.getAttempts()).as("已消耗的尝试次数保留").isEqualTo(2);
        assertThat(recovered.getLastFailureClassification()).isEqualTo(FailureClassification.RETRYABLE);

        // 再开一个进程验证状态已经被持久化，不会再次恢复
        JsonFileTaskStore thirdStore = new JsonFileTaskStore(dir);
        assertThat(thirdStore.resetRunningToPending("again")).isEmpty();
        assertThat(thirdStore.find("crashed-key").getStatus()).isEqualTo(TaskStatus.PENDING);
    }
}
