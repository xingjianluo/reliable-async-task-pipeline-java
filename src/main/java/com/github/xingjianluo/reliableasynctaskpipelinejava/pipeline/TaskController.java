package com.github.xingjianluo.reliableasynctaskpipelinejava.pipeline;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * 任务提交与状态查询 REST 接口。
 */
@RestController
@RequestMapping("/tasks")
public class TaskController {

    private final TaskPipeline pipeline;

    public TaskController(TaskPipeline pipeline) {
        this.pipeline = pipeline;
    }

    public record SubmitRequest(String idempotencyKey, String type, String payload, Integer maxAttempts) {}

    public record TaskView(String idempotencyKey, String type, TaskStatus status, int attempts,
                           FailureKind failureKind, String failureReason) {
        static TaskView of(Task t) {
            return new TaskView(t.getIdempotencyKey(), t.getType(), t.getStatus(), t.getAttempts(),
                    t.getFailureKind(), t.getFailureReason());
        }
    }

    /** 提交响应：在任务视图之外显式告知调用方是否命中重复投递 */
    public record SubmitResultView(TaskView task, boolean duplicate, boolean accepted) {}

    /** 拒绝原因与业务失败可区分：error=QUEUE_FULL / PIPELINE_CLOSING / BAD_REQUEST */
    public record ErrorView(String error, String reason) {}

    @PostMapping
    public ResponseEntity<?> submit(@RequestBody SubmitRequest request) {
        if (request.idempotencyKey() == null || request.idempotencyKey().isBlank()
                || request.type() == null || request.type().isBlank()) {
            return ResponseEntity.badRequest()
                    .body(new ErrorView("BAD_REQUEST", "idempotencyKey and type are required"));
        }
        try {
            SubmitOutcome outcome = pipeline.submit(request.idempotencyKey(), request.type(),
                    request.payload(), request.maxAttempts());
            SubmitResultView body = new SubmitResultView(TaskView.of(outcome.task()),
                    outcome.duplicate(), !outcome.duplicate());
            // 重复投递返回 200（幂等命中，拿到的是原来那条任务），新任务返回 202（已受理）
            HttpStatus status = outcome.duplicate() ? HttpStatus.OK : HttpStatus.ACCEPTED;
            return ResponseEntity.status(status).body(body);
        } catch (QueueFullException e) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .body(new ErrorView("QUEUE_FULL", e.getMessage()));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(new ErrorView("PIPELINE_CLOSING", e.getMessage()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(new ErrorView("BAD_REQUEST", e.getMessage()));
        }
    }

    @GetMapping("/{idempotencyKey}")
    public ResponseEntity<?> query(@PathVariable String idempotencyKey) {
        return pipeline.query(idempotencyKey)
                .<ResponseEntity<?>>map(t -> ResponseEntity.ok(TaskView.of(t)))
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(new ErrorView("NOT_FOUND", "no task for idempotencyKey=" + idempotencyKey)));
    }
}
