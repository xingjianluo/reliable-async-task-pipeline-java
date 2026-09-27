package com.github.xingjianluo.reliableasynctaskpipelinejava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.*;

import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

/**
 * REST 提交语义测试：202=新受理，200=重复投递（响应体 duplicate=true，即使任务仍排队、attempts=0）。
 * 测试环境使用纯内存 WAL（见 src/test/resources/application.properties）。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TaskControllerTests {

    @LocalServerPort
    int port;

    @Autowired
    TaskPipeline pipeline;

    @Autowired
    PipelineProperties properties;

    private final ObjectMapper mapper = new ObjectMapper();

    private ResponseEntity<String> post(String key, String type) {
        TestRestTemplate rest = new TestRestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> body = new HttpEntity<>(
                "{\"idempotencyKey\":\"" + key + "\",\"type\":\"" + type + "\",\"payload\":\"hi\"}", headers);
        return rest.exchange("http://localhost:" + port + "/tasks", HttpMethod.POST, body, String.class);
    }

    @Test
    void http202ForNewTaskAndHttp200WithDuplicateFlagForRepeatSubmit() throws Exception {
        String key = "rest-dup-" + System.nanoTime();

        ResponseEntity<String> first = post(key, "echo");
        assertEquals(HttpStatus.ACCEPTED, first.getStatusCode(), "新任务应返回 202");
        JsonNode firstJson = mapper.readTree(first.getBody());
        assertFalse(firstJson.path("duplicate").asBoolean(), "新受理 duplicate=false");
        assertTrue(firstJson.path("accepted").asBoolean());
        assertEquals(key, firstJson.path("task").path("idempotencyKey").asText());

        Thread.sleep(150); // echo 处理器快速完成
        ResponseEntity<String> second = post(key, "echo");
        assertEquals(HttpStatus.OK, second.getStatusCode(), "重复投递应返回 200");
        JsonNode secondJson = mapper.readTree(second.getBody());
        assertTrue(secondJson.path("duplicate").asBoolean(), "重复投递必须显式 duplicate=true");
        assertFalse(secondJson.path("accepted").asBoolean());
        assertEquals("SUCCEEDED", secondJson.path("task").path("status").asText());
        assertEquals(1, secondJson.path("task").path("attempts").asInt());
    }

    @Test
    void queuedDuplicateWithZeroAttemptsStillReturnsHttp200Duplicate() throws Exception {
        // 占住全部工作线程，保证目标任务稳定停留在 PENDING（排队、attempts=0）
        int concurrency = properties.getConcurrency();
        CountDownLatch allHoldersEntered = new CountDownLatch(concurrency);
        CountDownLatch releaseHolders = new CountDownLatch(1);
        String holderType = "blocking-holder-" + System.nanoTime();
        pipeline.registerHandler(holderType, task -> {
            allHoldersEntered.countDown();
            try {
                releaseHolders.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        for (int i = 0; i < concurrency; i++) {
            pipeline.submit("rest-holder-" + System.nanoTime() + "-" + i, holderType, null, null);
        }
        allHoldersEntered.await();

        String type = "blocking-rest-" + System.nanoTime();
        pipeline.registerHandler(type, task -> { });

        String key = "rest-queued-" + System.nanoTime();
        ResponseEntity<String> first = post(key, type);
        assertEquals(HttpStatus.ACCEPTED, first.getStatusCode());

        ResponseEntity<String> second = post(key, type);
        assertEquals(HttpStatus.OK, second.getStatusCode(), "排队中重复提交也必须是 200");
        JsonNode json = mapper.readTree(second.getBody());
        assertTrue(json.path("duplicate").asBoolean(), "必须显式告知是重复投递");
        assertFalse(json.path("accepted").asBoolean());
        assertEquals("PENDING", json.path("task").path("status").asText());
        assertEquals(0, json.path("task").path("attempts").asInt(), "尚未开始执行、attempts=0 也认得出重复");

        releaseHolders.countDown();
    }
}
