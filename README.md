# reliable-async-task-pipeline-java

本地异步任务处理流水线：在并发提交、重复投递、执行失败、**进程强制结束后重启**
与服务停止等情况下，为任务提供确定且可解释的处理结果。
默认使用本地文件 WAL 持久化，不依赖任何外部服务（数据库/MQ 均不需要）。

## 投递语义

- **至少一次执行，至多一次成功提交**：任务可能被尝试执行多次（重试、停机/崩溃恢复），
  但 `RUNNING -> SUCCEEDED` 的流转只允许发生一次，业务效果的成功提交至多一次。
- **幂等去重（结果可区分）**：提交必须携带 `idempotencyKey`。提交结果
  `SubmitOutcome{task, duplicate}` 明确区分：
  - `duplicate=false`：本次请求首次受理该键（HTTP 202）；
  - `duplicate=true`：重复投递，返回的是**原来那条任务**（HTTP 200）。
    即使它还排在队列里没开始跑、`attempts=0`，也认得出是重复；同一个键永远只有一条任务。
- **背压拒绝**：当积压（PENDING）任务数超过 `pipeline.max-backlog` 时，
  提交被拒绝并抛出 `QueueFullException`（HTTP 429，`error=QUEUE_FULL`），
  与业务失败（任务进入 DEAD_LETTER）完全可区分。被拒绝的任务不会入队。
- 流水线关闭后提交被拒绝（HTTP 503，`error=PIPELINE_CLOSING`）。

## 持久化与重启恢复

- 每一次状态流转（受理、开始执行、成功、重试、死信、回收、移除）都会先以
  **追加式 WAL（JSON Lines）写入 `pipeline.journal-path` 并 fsync**，再更新内存。
- 启动（`TaskStore` 构造）时重放日志重建全部任务：
  - **SUCCEEDED / DEAD_LETTER**：终态原样恢复，**不会再被执行**，
    因此已成功任务重启后不会多出一次业务效果；同键再次提交仍是重复投递；
  - **PENDING**（含等待退避的重试）：重新入队，按各自的下次执行时间继续调度；
  - **RUNNING**：只可能是上一个进程被强制结束（kill -9、机器重启、掉电）时
    来不及走完的执行，一律安全回收为 PENDING 并**立即重新调度**，任务不丢失。
- 重放完成后会把当前状态压缩成快照重写日志（每键一条提交记录 + 一条最新状态），
  控制文件体积；崩溃瞬间写了一半的损坏行会在打开时被截断，之前的完整记录都有效。
- 同一日志文件有文件锁保护，禁止两个流水线实例同时写同一文件。
- 纯内存模式：把 `pipeline.journal-path` 置空即退化为不持久化（重启不恢复），
  其余语义不变。

### 旧执行不能覆盖新状态（fencing epoch / 租约纪元）

- 每次 `PENDING -> RUNNING` 都会给该键分配一个**单调递增的租约纪元 epoch**，
  并随 STARTED 事件落盘；成功/重试/死信回写都必须携带与当前相等的 epoch。
- 崩溃恢复把遗留 RUNNING 回收为 PENDING 时会再次 bump epoch，旧租约立即失效。
- 因此：被中断、稍后才“醒来”的旧执行（例如忽略中断、在进程被杀时仍在跑的线程），
  它的终态回写会因为 epoch 过旧而被拒绝，**不可能覆盖已经往前走的新结果**；
  同一个键对外最终状态以最新一次执行为准，且从头到尾只有一次业务效果。
  该保证在并发较劲、同 JVM 僵尸旧执行场景下同样成立。

## 幂等与顺序保证

- 同一幂等键的任务在任一时刻**至多被一个线程执行**：状态流转按键串行化
  （对任务对象加锁的 CAS 状态机），`PENDING -> RUNNING` 只能由一个执行者获得。
- 状态机合法路径（非法流转、旧纪元回写一律被拒绝，不会出现回退或并发不一致）：

  ```
  <none>  -> PENDING                 提交（幂等去重 + WAL）
  PENDING -> RUNNING(epoch=N)        获得唯一执行权，attempts +1，纪元 +1
  RUNNING -> SUCCEEDED               执行成功（终态，需携带当前 epoch）
  RUNNING -> PENDING                 可重试失败（带退避）/ 停机回收 / 崩溃恢复（bump epoch）
  RUNNING -> DEAD_LETTER             不可重试失败 / 重试耗尽（终态，需携带当前 epoch）
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
- 每次失败分类都会在日志中打印依据（幂等键、尝试次数、纪元、分类、退避时长）。

## 优雅停机

- 停机时停止接收新任务；在途（RUNNING）任务在 `pipeline.shutdown-timeout-millis`
  内等待完成。
- 超时未完成：中断执行线程，已退出线程遗留的 RUNNING 任务被安全转回
  `PENDING`（bump 纪元、可重试状态），不丢失任务。
- 若执行线程仍存活（忽略中断），任务保持 RUNNING 并由该线程自行完成终态流转；
  一旦发生崩溃恢复，bump 纪元会让它的旧租约失效，避免旧执行覆盖新状态。
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
| `pipeline.journal-path` | `data/task-journal.log` | 持久化 WAL 文件路径；置空为纯内存模式 |

## REST 接口

- `POST /tasks`：提交任务。Body：`{"idempotencyKey","type","payload","maxAttempts?"}`。
  - 202：新任务已受理，响应体 `{"task":{...},"duplicate":false,"accepted":true}`；
  - 200：重复提交，响应体 `{"task":{...原来那条...},"duplicate":true,"accepted":false}`
    （任务可能仍在 PENDING 排队、attempts=0）；
  - 429 `QUEUE_FULL`：背压拒绝；503 `PIPELINE_CLOSING`：停机中；400 `BAD_REQUEST`。
- `GET /tasks/{idempotencyKey}`：查询状态，返回
  `{idempotencyKey,type,status,attempts,failureKind,failureReason}`。

## 与之前版本的兼容范围

- 配置项、REST 路径、查询响应结构、状态机、重试/死信/背压/停机行为均保持兼容。
- 提交响应体由“直接返回任务视图”改为包了一层
  `{task, duplicate, accepted}`，并改用显式 `duplicate` 标志区分重复
  （旧实现靠状态/attempts 猜测，任务尚在排队时会误判为新任务）；
  200/202 状态码语义不变。
- Java SDK 侧提交返回类型由 `Task` 改为 `SubmitOutcome`（内含同一个 `Task`）。
- 存储从“纯内存”升级为“默认本地文件 WAL”，不引入任何外部依赖；
  需要旧行为时把 `pipeline.journal-path` 留空即可。

## 本地验证

```bash
# 运行全部测试（本地临时文件/内存，不依赖外部服务，可重复验证）
mvn test

# 启动服务并手动验证（默认在 ./data/task-journal.log 持久化）
mvn spring-boot:run
curl -X POST localhost:8080/tasks -H 'Content-Type: application/json' \
     -d '{"idempotencyKey":"k1","type":"echo","payload":"hello"}'
curl localhost:8080/tasks/k1
# 再次提交同键：HTTP 200，响应体 duplicate=true
curl -i -X POST localhost:8080/tasks -H 'Content-Type: application/json' \
     -d '{"idempotencyKey":"k1","type":"echo","payload":"hello"}'
# kill -9 后重新启动：k1 保持 SUCCEEDED 不重跑；未完成任务自动续跑
```

测试覆盖（共 23 个用例）：重复提交幂等去重（含排队中 attempts=0 与并发提交）、
可重试失败重试耗尽进死信、不可重试失败直接进死信、同一幂等键并发执行互斥、
背压拒绝、优雅停机在途回收、非法状态流转与旧纪元回写拒绝、
**WAL 事件持久化/重放/崩溃半行截断/快照压缩/文件锁、
强制结束（不走优雅停机）后重启不丢任务且业务效果恰好一次、
已成功任务重启不重跑且重复投递被识别、被中断的旧执行晚醒不能覆盖新成功状态**、
REST 202/200 与 `duplicate` 标志区分。
