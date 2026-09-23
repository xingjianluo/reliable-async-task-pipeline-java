package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * 合法状态流转规则：任何状态修改都必须通过这里的校验，非法回退一律拒绝。
 *
 * <pre>
 * PENDING   -> RUNNING | CANCELED
 * RETRYING  -> PENDING | CANCELED      (退避结束后回到待执行)
 * RUNNING   -> SUCCEEDED | RETRYING | DEAD | PENDING
 *              (RUNNING -> PENDING 仅用于停机中断后的安全恢复)
 * SUCCEEDED / DEAD / CANCELED 为终态，不再流转
 * </pre>
 */
public final class StateTransition {

    private static final Map<TaskStatus, Set<TaskStatus>> ALLOWED;

    static {
        Map<TaskStatus, Set<TaskStatus>> map = new EnumMap<>(TaskStatus.class);
        map.put(TaskStatus.PENDING, EnumSet.of(TaskStatus.RUNNING, TaskStatus.CANCELED));
        map.put(TaskStatus.RETRYING, EnumSet.of(TaskStatus.PENDING, TaskStatus.CANCELED));
        map.put(TaskStatus.RUNNING, EnumSet.of(
                TaskStatus.SUCCEEDED, TaskStatus.RETRYING, TaskStatus.DEAD, TaskStatus.PENDING));
        map.put(TaskStatus.SUCCEEDED, EnumSet.noneOf(TaskStatus.class));
        map.put(TaskStatus.DEAD, EnumSet.noneOf(TaskStatus.class));
        map.put(TaskStatus.CANCELED, EnumSet.noneOf(TaskStatus.class));
        ALLOWED = Map.copyOf(map);
    }

    private StateTransition() {
    }

    public static boolean isAllowed(TaskStatus from, TaskStatus to) {
        if (from == to) {
            return true;
        }
        return ALLOWED.getOrDefault(from, EnumSet.noneOf(TaskStatus.class)).contains(to);
    }

    public static boolean isTerminal(TaskStatus status) {
        return status == TaskStatus.SUCCEEDED || status == TaskStatus.DEAD || status == TaskStatus.CANCELED;
    }
}
