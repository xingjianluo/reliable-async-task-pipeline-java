package com.github.xingjianluo.reliableasynctaskpipelinejava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 提交结果区分的接口测试：202=新受理，200=重复投递，响应体 {@code duplicate} 字段明确标记。
 * 纯内存 + 独立 MockMvc，不启动 Web 容器、不依赖外部服务。
 */
class TaskControllerTests {

    private TaskPipeline pipeline;
    private MockMvc mvc;
    private final ObjectMapper json = new ObjectMapper();

    private void start(PipelineProperties props) {
        TaskStore store = new TaskStore();
        pipeline = new TaskPipeline(store, props);
        mvc = MockMvcBuilders.standaloneSetup(new TaskController(pipeline)).build();
    }

    @AfterEach
    void tearDown() {
        if (pipeline != null) {
            pipeline.close();
        }
    }

    private PipelineProperties props() {
        PipelineProperties props = new PipelineProperties();
        props.setConcurrency(1);
        return props;
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("timeout waiting for: " + what);
            }
            Thread.sleep(10);
        }
    }

    private String submitBody(String key) {
        return "{\"idempotencyKey\":\"" + key + "\",\"type\":\"t\",\"payload\":null}";
    }

    @Test
    void queuedDuplicateReturns200AndDuplicateFlagBeforeAnyAttempt() throws Exception {
        PipelineProperties props = props();
        start(props);

        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        pipeline.registerHandler("t", task -> {
            started.countDown();
            release.await(5, TimeUnit.SECONDS);
        });

        // 第一条：占住唯一 worker
        mvc.perform(post("/tasks").contentType(MediaType.APPLICATION_JSON).content(submitBody("k-occupied")))
                .andExpect(status().isAccepted());
        await("occupied running", () -> started.getCount() == 0);

        // 第二条进入队列，尚未开始执行（attempts 仍为 0）
        MvcResult first = mvc.perform(post("/tasks").contentType(MediaType.APPLICATION_JSON).content(submitBody("k-dup")))
                .andExpect(status().isAccepted())
                .andReturn();
        JsonNode firstBody = json.readTree(first.getResponse().getContentAsString());
        assertFalse(firstBody.get("duplicate").asBoolean(), "首次提交 duplicate=false");
        assertEquals("PENDING", firstBody.get("status").asText());
        assertEquals(0, firstBody.get("attempts").asInt(), "仍在排队，attempts=0");

        // 同一键立刻重复提交：即使还排队、attempts=0，也必须识别为重复
        MvcResult second = mvc.perform(post("/tasks").contentType(MediaType.APPLICATION_JSON).content(submitBody("k-dup")))
                .andExpect(status().isOk())
                .andReturn();
        JsonNode secondBody = json.readTree(second.getResponse().getContentAsString());
        assertTrue(secondBody.get("duplicate").asBoolean(), "排队中的重复投递 duplicate 必须为 true");
        assertEquals("PENDING", secondBody.get("status").asText());
        assertEquals(0, secondBody.get("attempts").asInt(), "重复投递不得增加执行次数");

        release.countDown();
        await("k-dup done", () -> pipeline.query("k-dup")
                .map(t -> t.getStatus() == TaskStatus.SUCCEEDED).orElse(false));

        // 终态后重复提交仍是 200 + duplicate
        mvc.perform(post("/tasks").contentType(MediaType.APPLICATION_JSON).content(submitBody("k-dup")))
                .andExpect(status().isOk())
                .andExpect(result -> assertTrue(json.readTree(result.getResponse().getContentAsString())
                        .get("duplicate").asBoolean()));
    }

    @Test
    void queryUnknownKeyReturns404() throws Exception {
        start(props());
        mvc.perform(get("/tasks/missing")).andExpect(status().isNotFound());
    }

    @Test
    void invalidRequestReturns400() throws Exception {
        start(props());
        mvc.perform(post("/tasks").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"\",\"type\":\"t\"}"))
                .andExpect(status().isBadRequest());
    }
}
