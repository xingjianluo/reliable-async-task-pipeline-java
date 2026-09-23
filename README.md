# reliable-async-task-pipeline-java

本地异步任务处理流水线：在并发提交、重复投递、执行失败与服务停止等情况下，
为任务提供确定且可解释的处理结果。纯内存实现，不依赖任何外部服务。

## 投递语义

- **至少一次执行，至多一次成功提交**：任务可能被尝试执行多次（重试、停机恢复），
  但 `RUNNING -> SUCCEEDED` 的流转只允许发生一次，业务效果的成功提交至多一次。
- **幂等去重**：提交必须携带 `idempotencyKey`。同一键重复提交会被识别，
  返回已存在的任务（HTTP 200），不会产生新的业务效果；新任务返回 HTTP 202。
- **背压拒绝**：当积压（PENDING）任务数超过 `pipeline.max-backlog` 时，
  提交被拒绝并抛出 `QueueFullException`（HTTP 429，`error=QUEUE_FULL`），
  与业务失败（任务进入 DEAD_LETTER）完全可区分。被拒绝的任务不会入队。
- 流水线关闭后提交被拒绝（HTTP 503，`error=PIPELINE_CLOSING`）。

## 幂等与顺序保证

- 同一幂等键的任务在任一时刻**至多被一个线程执行**：状态流转按键串行化
  （对任务对象加锁的 CAS 状态机），`PENDING -> RUNNING` 只能由一个执行者获得。
- 状态机合法路径（非法流转一律被拒绝，不会出现回退或并发不一致）：

  ```
  <none>  -> PENDING                 提交（幂等去重）
  PENDING -> RUNNING                 获得唯一执行权，attempts +1
  RUNNING -> SUCCEEDED               执行成功（终态）
  RUNNING -> PENDING                 可重试失败 / 停机回收（带退避的下次执行时间）
  RUNNING -> DEAD_LETTER             不可重试失败 / 重试耗尽（终态）
  ```

- **顺序**：不同任务之间不保证执行顺序（并发处理）；同一任务的重试严格按
  尝试次数顺序进行，重试之间按退避间隔调度。

## 重试与死信规则

- 处理器通过抛出 `TaskFailedException(kind, reason)` 声明失败分类：
  - `NON_RETRYABLE`：不可重试，**立即进入死信**，不再尝试。
  - `RETRYABLE`：可重试，按指数退避 `backoffBaseMillis * 2^(attempt-1)`
    （封顶 30s）调度重试。
  - 处理器抛出的其他未分类异常（含中断）一律按 `RETRYABLE` 处理。
- 当 `attempts >= maxAttempts` 时，可重试失败转为 `RETRY_EXHAUSTED` 进入死信。
- 死信任务保留可区分的失败原因：`failureKind ∈ {NON_RETRYABLE, RETRY_EXHAUSTED}`
  与 `failureReason`，可通过查询接口获取。
- 每次失败分类都会在日志中打印依据（幂等键、尝试次数、分类、退避时长）。

## 优雅停机

- 停机时停止接收新任务；在途（RUNNING）任务在 `pipeline.shutdown-timeout-millis`
  内等待完成。
- 超时未完成：中断执行线程，已退出线程遗留的 RUNNING 任务被安全转回
  `PENDING`（可重试状态），不丢失任务。
- 若执行线程仍存活（忽略中断），任务保持 RUNNING 并由该线程自行完成终态流转，
  避免同一键被并发执行产生重复业务效果。
- 新流水线实例在同一 `TaskStore` 上启动时，会自动恢复所有遗留 PENDING 任务。
- 注意：业务处理器实现需保证幂等（同一幂等键的业务效果只应产生一次），
  流水线保证“成功提交”记录至多一次。

## 配置项（application.properties）

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `pipeline.concurrency` | 4 | 并发执行上限（工作线程数） |
| `pipeline.max-backlog` | 100 | 积压阈值，超过则拒绝提交（背压） |
| `pipeline.default-max-attempts` | 3 | 默认最大尝试次数（含首次） |
| `pipeline.backoff-base-millis` | 200 | 重试退避基数（毫秒），指数增长封顶 30s |
| `pipeline.shutdown-timeout-millis` | 5000 | 停机时等待在途任务完成的限定时间（毫秒） |

## REST 接口

- `POST /tasks`：提交任务。Body：`{"idempotencyKey","type","payload","maxAttempts?"}`。
  - 202：新任务已受理；200：重复提交，返回已存在任务；
  - 429 `QUEUE_FULL`：背压拒绝；503 `PIPELINE_CLOSING`：停机中；400 `BAD_REQUEST`。
- `GET /tasks/{idempotencyKey}`：查询状态，返回
  `{idempotencyKey,type,status,attempts,failureKind,failureReason}`。

## 本地验证

```bash
# 运行全部测试（纯内存，不依赖外部服务，可重复验证）
mvn test

# 启动服务并手动验证
mvn spring-boot:run
curl -X POST localhost:8080/tasks -H 'Content-Type: application/json' \
     -d '{"idempotencyKey":"k1","type":"echo","payload":"hello"}'
curl localhost:8080/tasks/k1
```

测试覆盖：重复提交幂等去重、可重试失败重试耗尽进死信、不可重试失败直接进死信、
同一幂等键的并发提交与并发执行互斥、背压拒绝、停机在途任务回收与重启恢复、
非法状态流转拒绝。单测日志打印幂等键、尝试次数、状态流转与失败分类依据。
