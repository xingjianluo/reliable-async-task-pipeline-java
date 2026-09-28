package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.persist.JdbcTaskRepository;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.persist.TaskRepository;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Paths;

/**
 * 流水线 Bean 装配。
 */
@Configuration
@EnableConfigurationProperties(PipelineProperties.class)
public class PipelineConfig {

    /** 嵌入式 H2 文件仓储：任务落盘，进程强杀/机器重启后重启可恢复 */
    @Bean(destroyMethod = "")
    public TaskRepository taskRepository(PipelineProperties properties) {
        return new JdbcTaskRepository(Paths.get(properties.getDataDir()));
    }

    @Bean
    public TaskStore taskStore(TaskRepository taskRepository) {
        return new TaskStore(taskRepository);
    }

    @Bean(destroyMethod = "close")
    public TaskPipeline taskPipeline(TaskStore taskStore, PipelineProperties properties) {
        TaskPipeline pipeline = new TaskPipeline(taskStore, properties);
        // 演示处理器：本地验证用，仅记录日志
        pipeline.registerHandler("echo", task ->
                org.slf4j.LoggerFactory.getLogger("demo.echo")
                        .info("echo handled, idempotencyKey={} payload={}",
                                task.getIdempotencyKey(), task.getPayload()));
        // 演示处理器：本地复现“处理器抛出严重错误（Error 级别）”用，
        // 主动抛出 OutOfMemoryError，便于观察任务按失败语义收敛、流水线继续消费。
        pipeline.registerHandler("oom", task -> {
            throw new OutOfMemoryError("demo: simulated heap exhaustion from handler");
        });
        return pipeline;
    }
}