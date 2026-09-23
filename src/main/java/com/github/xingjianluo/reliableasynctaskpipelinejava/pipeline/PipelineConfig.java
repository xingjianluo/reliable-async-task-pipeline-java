package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 流水线 Bean 装配。
 */
@Configuration
@EnableConfigurationProperties(PipelineProperties.class)
public class PipelineConfig {

    @Bean
    public TaskStore taskStore() {
        return new TaskStore();
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