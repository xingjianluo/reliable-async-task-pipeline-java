package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

/**
 * 背压拒绝：队列积压超过配置阈值。与业务失败可区分（提交阶段即被拒绝，任务未入队）。
 */
public class QueueFullException extends RuntimeException {

    public QueueFullException(String message) {
        super(message);
    }
}
