package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.persist;

/**
 * 持久化层不可恢复异常（落盘失败等）。
 */
public class PersistenceException extends RuntimeException {

    public PersistenceException(String message, Throwable cause) {
        super(message, cause);
    }
}
