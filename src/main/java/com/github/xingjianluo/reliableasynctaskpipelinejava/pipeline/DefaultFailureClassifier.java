package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

/**
 * 默认失败分类规则：
 * <ul>
 *     <li>{@link NonRetryableTaskException} 或其包装异常 -> 不可重试，直接进死信；</li>
 *     <li>{@link InterruptedException} / {@link Error}（OOM 等）-> 视为中断，按可重试处理，
 *     由停机恢复逻辑保证不丢任务；</li>
 *     <li>其余异常 -> 可重试，受最大尝试次数限制。</li>
 * </ul>
 */
public class DefaultFailureClassifier implements FailureClassifier {

    @Override
    public FailureClassification classify(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof NonRetryableTaskException) {
                return FailureClassification.NON_RETRYABLE;
            }
            current = current.getCause();
        }
        return FailureClassification.RETRYABLE;
    }
}
