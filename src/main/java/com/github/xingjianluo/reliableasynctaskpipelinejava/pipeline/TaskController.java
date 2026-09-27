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
                           FailureKind failureKind, String failureReason, boolean duplicate) {
        static TaskView of(Task t) {
            return of(t, false);
        }

        static TaskView of(Task t, boolean duplicate) {
            return new TaskView(t.getIdempotencyKey(), t.getType(), t.getStatus(), t.getAttempts(),
                    t.getFailureKind(), t.getFailureReason(), duplicate);
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
            SubmitResult result = pipeline.submit(request.idempotencyKey(), request.type(),
                    request.payload(), request.maxAttempts());
            // 202：新任务已受理；200：重复投递命中已有任务（即使其仍在排队、attempts=0）
            HttpStatus status = result.duplicate() ? HttpStatus.OK : HttpStatus.ACCEPTED;
            return ResponseEntity.status(status).body(TaskView.of(result.task(), result.duplicate()));
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
