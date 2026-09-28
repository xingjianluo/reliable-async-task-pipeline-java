# reliable-async-task-pipeline-java

本地异步任务处理流水线：在并发提交、重复投递、执行失败、服务停止（含进程被强制结束）
等情况下，为任务提供确定且可解释的处理结果。任务状态持久化到**随应用嵌入式启动的本地
H2 文件库**，不依赖任何外部服务（数据库、消息队列等）。

## 重启与恢复约定（强杀 / 机器重启也成立）

- 每个任务在“受理插入”与“每次状态流转”时都同步落盘（H2 `WRITE_DELAY=0`，提交即刷盘）。
  数据文件默认在 `pipeline.data-dir`（`./data/tasks/tasks.mv.db`）。
- **未完成任务不丢**：启动时新流水线先扫描持久化状态，把所有遗留在 `RUNNING` 的任务
  （正常停机回收的、以及 `kill -9` / 掉电等没走停机流程的）原子重置为 `PENDING` 并立即
  重新入队；原本就 `PENDING`（含在退避等待）的任务也一并接管。
- **成功任务不重跑**：`SUCCEEDED` / `DEAD_LETTER` 是终态，重启后不会再被执行，
  不会因此多出一次业务效果。
- **旧执行不覆盖新结果（fencing token / epoch）**：每个任务带一个单调递增的世代号
  `epoch`。执行者领取任务（`PENDING -> RUNNING`）时拿到当时的 epoch 作为令牌，之后写
  终态必须携带它；恢复时会把遗留 `RUNNING` 任务的 epoch +1。于是被中断、稍后才“醒来”
  的旧执行拿着过期令牌写 `SUCCEEDED`/死信时会被存储层的条件更新拒绝（影响行数 0），
  内存副本随即以存储中的最新状态为准。同一幂等键对外可见的状态始终以最新一次执行为准，
  业务效果的成功提交自始至终至多一次。并发较劲时由数据库行上的 epoch 条件更新裁决。

### 重启恢复后的状态语义

无论任务是执行时抛出严重错误、执行中途被强制结束、还是赶上正常停机，重启后同一个
幂等键最终都收敛到三种可解释的状态之一：

- **成功（`SUCCEEDED`）**：恢复后续跑成功；失败尝试不产生业务效果，成功提交自始至终
  至多一次（终态流转带 fencing token，旧执行醒来写不进去）。
- **可重试（`PENDING`）**：抛出严重错误后已按退避落盘为 `PENDING` 的任务、以及遗留
  `RUNNING` 被回收的任务，重启后重新入队，继续遵循剩余尝试次数与退避时间。
- **死信（`DEAD_LETTER`）**：重启前已重试耗尽或被显式判定不可重试的任务是终态，
  重启后不再执行，`failureKind` / `failureReason`（含严重错误类型）原样保留。

## 投递语义

- **至少一次执行，至多一次成功提交**：任务可能被尝试执行多次（重试、停机恢复、强杀恢复），
  但 `RUNNING -> SUCCEEDED` 的流转只允许发生一次，业务效果的成功提交至多一次。
- **幂等去重，结果可区分**：提交必须携带 `idempotencyKey`。提交返回显式结果：
  - 新任务：HTTP **202**，响应体 `duplicate=false`；
  - 重复投递：HTTP **200**，响应体 `duplicate=true`，返回的是原来那条任务。
  - 即使原任务仍排在队列里、还没开始跑（`attempts=0`），重复投递也照样识别为 200，
    同一幂等键在存储中永远只有一条任务。判定依据是幂等键是否已存在（含重启前落盘的任务），
    不再由调用方根据状态/尝试次数猜测。
- **背压拒绝**：当积压（PENDING）任务数超过 `pipeline.max-backlog` 时，
  提交被拒绝并抛出 `QueueFullException`（HTTP 429，`error=QUEUE_FULL`），
  与业务失败（任务进入 DEAD_LETTER）完全可区分。被拒绝的任务不会入队，落盘记录也会删除。
- 流水线关闭后提交被拒绝（HTTP 503，`error=PIPELINE_CLOSING`）。

## 幂等与顺序保证

- 同一幂等键的任务在任一时刻**至多被一个线程执行**：状态流转按键串行化
  （对任务对象加锁的 CAS 状态机），`PENDING -> RUNNING` 只能由一个执行者获得；
  跨进程 / 重启恢复的竞争由持久化层的 epoch 条件更新裁决。
- 状态机合法路径（非法流转一律被拒绝，不会出现回退或并发不一致）：

  ```
  <none>  -> PENDING                 提交（幂等去重，落盘）
  PENDING -> RUNNING                 获得唯一执行权，attempts +1，epoch +1，拿到新 fencing token
  RUNNING -> SUCCEEDED               执行成功（终态，epoch +1，仅当前令牌可写）
  RUNNING -> PENDING                 可重试失败 / 停机或强杀回收（带退避，epoch +1）
  RUNNING -> DEAD_LETTER             不可重试失败 / 重试耗尽（终态，epoch +1）
  ```

- **顺序**：不同任务之间不保证执行顺序（并发处理）；同一任务的重试严格按
  尝试次数顺序进行，重试之间按退避间隔调度。

## 重试与死信规则

- 处理器通过抛出 `TaskFailedException(kind, reason)` 声明失败分类：
  - `NON_RETRYABLE`：不可重试，**立即进入死信**，不再尝试。
  - `RETRYABLE`：可重试，按指数退避 `backoffBaseMillis * 2^(attempt-1)`
    （封顶 30s）调度重试。
  - 处理器抛出的其他未分类异常（含中断）一律按 `RETRYABLE` 处理。
- **处理器抛出严重错误（`Error` 级别，如 `OutOfMemoryError`、`StackOverflowError`）的约定**：
  流水线会兜底捕获处理器抛出的任何 `Throwable`，与未分类异常一样按 `RETRYABLE`
  失败收敛——走退避重试，重试次数耗尽则以 `RETRY_EXHAUSTED` 进入死信；失败原因
  （形如 `severe error OutOfMemoryError: <message>`）写入 `failureReason`，可通过
  查询接口看到。严重错误**不会**让任务卡在 `RUNNING`，也**不会**杀死工作线程：
  工作线程主循环另有 `Throwable` 兜底保护，单条任务的严重错误之后，同键或其他键的
  后续任务照常被消费，无需重启即可恢复正常服务。
- 当 `attempts >= maxAttempts` 时，可重试失败转为 `RETRY_EXHAUSTED` 进入死信。
- 死信任务保留可区分的失败原因：`failureKind ∈ {NON_RETRYABLE, RETRY_EXHAUSTED}`
  与 `failureReason`，可通过查询接口获取。
- 每次失败分类都会在日志中打印依据（幂等键、尝试次数、分类、退避时长）。

## 优雅停机

- 停机时停止接收新任务；在途（RUNNING）任务在 `pipeline.shutdown-timeout-millis`
  内等待完成。
- 超时未完成：中断执行线程，已退出线程遗留的 RUNNING 任务被安全转回
  `PENDING`（可重试状态），不丢失任务。
- 若执行线程仍存活（忽略中断），任务保持 RUNNING 并由该线程自行完成终态流转；
  其 fencing token 一旦过期（例如已被恢复流程 bump），写回会被拒绝，
  避免同一键被并发执行产生重复业务效果。
- 新流水线实例在同一持久化数据目录上启动时，会先回收所有遗留 RUNNING 任务
  （正常停机遗留或被强杀遗留的都一样），再恢复所有 PENDING 任务。
- 注意：业务处理器实现需保证幂等（同一幂等键的业务效果只应产生一次），
  流水线保证“成功提交”记录至多一次；处理器可能因重试/恢复被**调用多次**。

## 持久化与兼容范围

- 持久化实现为嵌入式 H2 文件库（`pipeline.persist.JdbcTaskRepository`），
  随应用进程启动，无需安装或连接任何外部服务；删除数据目录即回到全新状态。
- 接口与行为向后兼容：`POST /tasks` 的状态码语义不变（202 新受理 / 200 重复 /
  429 背压 / 503 停机中 / 400 参数错误）；查询响应仅**新增** `duplicate` 字段，
  不改变已有字段。Java 侧提交返回类型由 `Task` 调整为 `SubmitResult(task, duplicate)`，
  重复判定不再依赖 `attempts/status` 推断。
- `TaskStore` 仍保留无参构造（内存模式，不落盘）供不需要跨进程持久化的场景与快速测试使用；
  Spring 装配默认使用 H2 文件仓储。

## 配置项（application.properties）

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `pipeline.concurrency` | 4 | 并发执行上限（工作线程数） |
| `pipeline.max-backlog` | 100 | 积压阈值，超过则拒绝提交（背压） |
| `pipeline.default-max-attempts` | 3 | 默认最大尝试次数（含首次） |
| `pipeline.backoff-base-millis` | 200 | 重试退避基数（毫秒），指数增长封顶 30s |
| `pipeline.shutdown-timeout-millis` | 5000 | 停机时等待在途任务完成的限定时间（毫秒） |
| `pipeline.data-dir` | `data/tasks` | 嵌入式 H2 数据文件目录；重启 / 强杀后据此恢复任务 |

## REST 接口

- `POST /tasks`：提交任务。Body：`{"idempotencyKey","type","payload","maxAttempts?"}`。
  - 202：新任务已受理，`duplicate=false`；200：重复提交（哪怕任务仍在排队、attempts=0），
    返回已存在任务且 `duplicate=true`；
  - 429 `QUEUE_FULL`：背压拒绝；503 `PIPELINE_CLOSING`：停机中；400 `BAD_REQUEST`。
- `GET /tasks/{idempotencyKey}`：查询状态，返回
  `{idempotencyKey,type,status,attempts,failureKind,failureReason,duplicate}`
  （查询场景 `duplicate` 恒为 false）。

## 本地验证

```bash
# 运行全部测试（嵌入式 H2 + 临时目录，纯本地、不依赖外部服务，可重复验证）
mvn test

# 启动服务并手动验证
mvn spring-boot:run
curl -X POST localhost:8080/tasks -H 'Content-Type: application/json' \
     -d '{"idempotencyKey":"k1","type":"echo","payload":"hello"}'   # 202 duplicate=false
curl -X POST localhost:8080/tasks -H 'Content-Type: application/json' \
     -d '{"idempotencyKey":"k1","type":"echo","payload":"hello"}'   # 200 duplicate=true
curl localhost:8080/tasks/k1

# 验证强杀恢复：运行中直接 kill -9 进程，再用同一数据目录启动，
# 已成功任务不重跑、未完成任务自动续跑、同键再提交返回 200
kill -9 <pid>
mvn spring-boot:run -Dspring-boot.run.arguments=--pipeline.data-dir=data/tasks

# 验证处理器抛出严重错误（OutOfMemoryError）的收敛：
#   mvn test -Dtest=ErrorHandlingTests
# 用例主动让处理器抛出 new OutOfMemoryError(...)，断言：
#   1. 任务按重试规则跑满次数后进入 DEAD_LETTER，不卡在 RUNNING，
#      failureReason 含 "severe error OutOfMemoryError"；
#   2. 严重错误之后提交的其他键任务仍能正常 SUCCEEDED（流水线未被带停）；
#   3. 首次抛错、退避重试成功时业务效果恰好一次；
#   4. 抛错后处于退避等待时 abort()（模拟 kill -9），重启后续跑成功且只生效一次。
```

测试覆盖（共 19 个用例，均可本地反复跑）：

- 重复提交幂等去重；**排队中（attempts=0）重复投递也返回 duplicate/200**（流水线层 + REST 层）；
- 可重试失败重试耗尽进死信、不可重试失败直接进死信；
- 同一幂等键的并发提交（仅 1 个受理、其余全部 duplicate）与并发执行互斥；
- 背压拒绝、停机在途任务回收与重启恢复、非法状态流转与过期 fencing token 拒绝；
- **强杀恢复**：执行中 `abort()`（模拟 kill -9）后用同一数据目录重启，RUNNING 任务被回收续跑、
  只成功一次；旧执行醒来持过期令牌写终态被拒绝，状态不被覆盖；
- **成功不重跑**：强杀重启后已 SUCCEEDED 任务不再执行，同键再次提交识别为重复；
  强杀时仍排队的 PENDING 任务不丢、恢复后执行一次；
- **严重错误（Error）收敛**：处理器抛出 `OutOfMemoryError` 时任务按重试规则重试、
  耗尽进死信且不卡在 RUNNING；严重错误之后流水线继续消费后续任务；
  首次抛错重试成功只生效一次；抛错后退避等待中强杀，重启恢复不丢、不重复生效；
- **仓储 epoch CAS**：过期令牌领取 / 写终态 / 终态回退全部被拒。
- 所有单测均使用内存仓储或 JUnit `@TempDir` 临时数据目录，不在项目工作目录生成或
  改动数据文件，每次运行数据全新、不遗留、不复用。
单测日志打印幂等键、尝试次数、世代号、状态流转、fencing 拒绝与失败分类依据。
