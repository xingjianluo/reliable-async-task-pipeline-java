package com.github.xingjianluo.reliableasynctaskpipelinejava.tests;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Path;

/**
 * Spring 上下文冒烟测试。
 *
 * <p>持久化数据目录指向 JUnit 临时目录：测试不会在项目工作目录下生成或改动数据文件，
 * 每次运行使用全新目录，不复用上一次运行遗留的数据。
 */
@SpringBootTest
public class ApplicationTests {

    @TempDir
    static Path tempDataDir;

    @DynamicPropertySource
    static void pipelineProperties(DynamicPropertyRegistry registry) {
        registry.add("pipeline.data-dir", () -> tempDataDir.toString());
    }

    @Test
    public void contentLoad() {
    }
}
