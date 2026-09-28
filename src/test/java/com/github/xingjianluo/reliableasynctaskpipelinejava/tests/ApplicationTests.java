package com.github.xingjianluo.reliableasynctaskpipelinejava.tests;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Path;

/**
 * Spring 上下文装配冒烟测试。
 *
 * <p>持久化数据目录通过 {@link DynamicPropertySource} 重定向到 JUnit 临时目录：
 * 不在项目工作目录生成或改动数据文件，也不会复用上次运行遗留的数据。
 */
@SpringBootTest
public class ApplicationTests {

    @TempDir
    static Path tempDataDir;

    @DynamicPropertySource
    static void dataDir(DynamicPropertyRegistry registry) {
        registry.add("pipeline.data-dir", () -> tempDataDir.toString());
    }

    @Test
    public void contentLoad() {
    }
}
