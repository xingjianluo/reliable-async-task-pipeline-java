package com.github.xingjianluo.reliableasynctaskpipelinejava.tests;

import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.IllegalStateTransitionException;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.JsonFileTaskStore;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.StateTransition;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.TaskRecord;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.TaskStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 合法流转路径与非法回退防护的单测。
 */
class StateTransitionTest {

    @Test
    void legalTransitionsAreAllowed() {
        assertThat(StateTransition.isAllowed(TaskStatus.PENDING, TaskStatus.RUNNING)).isTrue();
        assertThat(StateTransition.isAllowed(TaskStatus.RUNNING, TaskStatus.SUCCEEDED)).isTrue();
        assertThat(StateTransition.isAllowed(TaskStatus.RUNNING, TaskStatus.RETRYING)).isTrue();
        assertThat(StateTransition.isAllowed(TaskStatus.RUNNING, TaskStatus.DEAD)).isTrue();
        assertThat(StateTransition.isAllowed(TaskStatus.RUNNING, TaskStatus.PENDING)).isTrue();
        assertThat(StateTransition.isAllowed(TaskStatus.RETRYING, TaskStatus.PENDING)).isTrue();
    }

    @Test
    void illegalRollbacksAreRejected() {
        // 终态不得回退
        assertThat(StateTransition.isAllowed(TaskStatus.SUCCEEDED, TaskStatus.RUNNING)).isFalse();
        assertThat(StateTransition.isAllowed(TaskStatus.DEAD, TaskStatus.PENDING)).isFalse();
        assertThat(StateTransition.isAllowed(TaskStatus.CANCELED, TaskStatus.RUNNING)).isFalse();
        // 不得跳过 RUNNING
        assertThat(StateTransition.isAllowed(TaskStatus.PENDING, TaskStatus.SUCCEEDED)).isFalse();
        // 待执行任务不能直接进死信
        assertThat(StateTransition.isAllowed(TaskStatus.PENDING, TaskStatus.DEAD)).isFalse();
    }

    @Test
    void storeRejectsIllegalTransitionWithException(@TempDir Path dir) {
        JsonFileTaskStore store = new JsonFileTaskStore(dir.toString());
        TaskRecord record = new TaskRecord();
        record.setIdempotencyKey("k-legal");
        record.setStatus(TaskStatus.SUCCEEDED);
        record.setCreatedAt(1L);
        record.setUpdatedAt(1L);
        store.putIfAbsent(record);

        assertThatThrownBy(() -> store.compareAndUpdateStatus("k-legal", TaskStatus.SUCCEEDED, TaskStatus.RUNNING))
                .isInstanceOf(IllegalStateTransitionException.class)
                .hasMessageContaining("k-legal")
                .hasMessageContaining("SUCCEEDED -> RUNNING");
    }
}
