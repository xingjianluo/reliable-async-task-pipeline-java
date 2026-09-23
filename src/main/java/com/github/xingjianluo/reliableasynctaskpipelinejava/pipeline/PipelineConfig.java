package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

/**
 * 流水线配置参数。
 */
public class PipelineConfig {
    private int workerThreads = 2;
    private int queueCapacity = 100;
    private int maxAttempts = 3;
    private long backoffBaseMillis = 50L;
    private long backoffMaxMillis = 2_000L;
    private long shutdownWaitMillis = 5_000L;
    private String storeDir = "target/task-pipeline-store";

    public int getWorkerThreads() { return workerThreads; }
    public void setWorkerThreads(int workerThreads) { this.workerThreads = workerThreads; }
    public int getQueueCapacity() { return queueCapacity; }
    public void setQueueCapacity(int queueCapacity) { this.queueCapacity = queueCapacity; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public long getBackoffBaseMillis() { return backoffBaseMillis; }
    public void setBackoffBaseMillis(long backoffBaseMillis) { this.backoffBaseMillis = backoffBaseMillis; }
    public long getBackoffMaxMillis() { return backoffMaxMillis; }
    public void setBackoffMaxMillis(long backoffMaxMillis) { this.backoffMaxMillis = backoffMaxMillis; }
    public long getShutdownWaitMillis() { return shutdownWaitMillis; }
    public void setShutdownWaitMillis(long shutdownWaitMillis) { this.shutdownWaitMillis = shutdownWaitMillis; }
    public String getStoreDir() { return storeDir; }
    public void setStoreDir(String storeDir) { this.storeDir = storeDir; }
}
