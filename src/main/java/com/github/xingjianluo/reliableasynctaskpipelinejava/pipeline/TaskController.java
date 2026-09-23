package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 任务提交与状态查询的最小 HTTP 入口，用于本地手工验证。
 */
@RestController
@RequestMapping("/tasks")
public class TaskController {

    private final TaskPipelineService service;

    public TaskController(TaskPipelineService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> submit(@RequestBody Map<String, String> request) {
        String key = request.get("idempotencyKey");
        String payload = request.get("payload");
        SubmissionResult result = service.submit(key, payload);

        Map<String, Object> body = new LinkedHashMap<>();
        if (result.isAccepted()) {
            body.put("outcome", "ACCEPTED");
            body.put("task", toView(result.getTask()));
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(body);
        }
        if (result.isDuplicate()) {
            body.put("outcome", "DUPLICATE");
            body.put("task", toView(result.getTask()));
            return ResponseEntity.ok(body);
        }
        // 背压 / 停机拒绝：使用 503 且拒绝原因与业务失败可区分。
        body.put("outcome", "REJECTED");
        body.put("rejectReason", result.getRejectReason());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(body);
    }

    @GetMapping("/{idempotencyKey}")
    public ResponseEntity<Map<String, Object>> status(@PathVariable String idempotencyKey) {
        TaskRecord record = service.getStatus(idempotencyKey);
        if (record == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(toView(record));
    }

    private Map<String, Object> toView(TaskRecord r) {
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("idempotencyKey", r.getIdempotencyKey());
        view.put("status", r.getStatus());
        view.put("attempts", r.getAttempts());
        view.put("createdAt", r.getCreatedAt());
        view.put("updatedAt", r.getUpdatedAt());
        view.put("nextRunAt", r.getNextRunAt());
        view.put("lastFailureReason", r.getLastFailureReason());
        view.put("lastFailureClassification", r.getLastFailureClassification());
        return view;
    }
}
