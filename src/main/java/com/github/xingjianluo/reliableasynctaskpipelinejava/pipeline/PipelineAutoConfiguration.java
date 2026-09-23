package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

import jakarta.annotation.PreDestroy;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 将流水线注册为 Spring Bean，配置前缀 task.pipeline。
 * 容器中的 {@link TaskHandler} Bean 作为业务处理器；没有则使用空操作处理器。
 */
@Configuration
public class PipelineAutoConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "task.pipeline")
    public PipelineConfig pipelineConfig() {
        return new PipelineConfig();
    }

    @Bean
    @ConditionalOnMissingBean(TaskHandler.class)
    public TaskHandler defaultTaskHandler() {
        return context -> { };
    }

    @Bean
    public ManagedTaskPipeline managedTaskPipeline(PipelineConfig config, TaskHandler handler) {
        return new ManagedTaskPipeline(new TaskPipelineService(config, handler));
    }

    @Bean
    public TaskPipelineService taskPipelineService(ManagedTaskPipeline managed) {
        return managed.service;
    }

    /** 随容器启动/关闭，保证优雅停机被执行。 */
    static class ManagedTaskPipeline {
        private final TaskPipelineService service;

        ManagedTaskPipeline(TaskPipelineService service) {
            this.service = service;
        }

        @jakarta.annotation.PostConstruct
        public void start() {
            service.start();
        }

        @PreDestroy
        public void stop() {
            service.stop();
        }
    }
}
