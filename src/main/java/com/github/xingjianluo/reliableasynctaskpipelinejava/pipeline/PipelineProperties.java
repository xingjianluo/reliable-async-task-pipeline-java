package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 流水线配置项（骨架）。
 */
@ConfigurationProperties(prefix = "pipeline")
public class PipelineProperties {

    /** 并发执行上限 */
    private int concurrency = 4;
    /** 队列积压阈值，超过则拒绝提交 */
    private int maxBacklog = 100;
    /** 默认最大尝试次数（含首次） */
    private int defaultMaxAttempts = 3;
    /** 重试退避基数（毫秒），按 attempt 指数增长 */
    private long backoffBaseMillis = 200;
    /** 优雅停机等待在途任务完成的限定时间（毫秒） */
    private long shutdownTimeoutMillis = 5000;
    /** 持久化日志（WAL）文件路径；空表示纯内存模式，重启不恢复 */
    private String journalPath = "data/task-journal.log";

    public int getConcurrency() { return concurrency; }
    public void setConcurrency(int concurrency) { this.concurrency = concurrency; }
    public int getMaxBacklog() { return maxBacklog; }
    public void setMaxBacklog(int maxBacklog) { this.maxBacklog = maxBacklog; }
    public int getDefaultMaxAttempts() { return defaultMaxAttempts; }
    public void setDefaultMaxAttempts(int defaultMaxAttempts) { this.defaultMaxAttempts = defaultMaxAttempts; }
    public long getBackoffBaseMillis() { return backoffBaseMillis; }
    public void setBackoffBaseMillis(long backoffBaseMillis) { this.backoffBaseMillis = backoffBaseMillis; }
    public long getShutdownTimeoutMillis() { return shutdownTimeoutMillis; }
    public void setShutdownTimeoutMillis(long shutdownTimeoutMillis) { this.shutdownTimeoutMillis = shutdownTimeoutMillis; }
    public String getJournalPath() { return journalPath; }
    public void setJournalPath(String journalPath) { this.journalPath = journalPath; }
}
