package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

/**
 * 流水线 Bean 装配。
 */
@Configuration
@EnableConfigurationProperties(PipelineProperties.class)
public class PipelineConfig {

    @Bean(destroyMethod = "close")
    public TaskJournal taskJournal(PipelineProperties properties) {
        String path = properties.getJournalPath();
        if (path == null || path.isBlank()) {
            org.slf4j.LoggerFactory.getLogger(PipelineConfig.class)
                    .warn("[config] pipeline.journal-path 为空，使用纯内存模式：重启后任务不恢复");
            return TaskJournal.NOOP;
        }
        return new FileTaskJournal(Path.of(path));
    }

    @Bean
    public TaskStore taskStore(TaskJournal taskJournal) {
        return new TaskStore(taskJournal);
    }

    @Bean(destroyMethod = "close")
    public TaskPipeline taskPipeline(TaskStore taskStore, PipelineProperties properties) {
        TaskPipeline pipeline = new TaskPipeline(taskStore, properties);
        // 演示处理器：本地验证用，仅记录日志
        pipeline.registerHandler("echo", task ->
                org.slf4j.LoggerFactory.getLogger("demo.echo")
                        .info("echo handled, idempotencyKey={} payload={}",
                                task.getIdempotencyKey(), task.getPayload()));
        return pipeline;
    }
}