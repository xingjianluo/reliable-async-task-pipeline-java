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
            Task task = pipeline.submit(request.idempotencyKey(), request.type(),
                    request.payload(), request.maxAttempts());
            // 重复提交返回 200（幂等命中），新任务返回 202（已受理）
            HttpStatus status = task.getAttempts() > 0 || task.getStatus() != TaskStatus.PENDING
                    ? HttpStatus.OK : HttpStatus.ACCEPTED;
            return ResponseEntity.status(status).body(TaskView.of(task));
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
