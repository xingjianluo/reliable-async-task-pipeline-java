package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

/**
 * 将执行异常分类为可重试 / 不可重试。
 */
@FunctionalInterface
public interface FailureClassifier {
    FailureClassification classify(Throwable error);
}
