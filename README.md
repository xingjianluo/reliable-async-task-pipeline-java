# 本地异步任务处理流水线

一个**不依赖任何外部服务**（无需数据库 / MQ）的本地异步任务流水线，保证任务在并发提交、重复投递、
执行失败与服务停止等场景下仍有**确定且可解释**的处理结果。基于 Spring Boot 3 / Java 21。

## 1. 投递与处理语义

| 维度 | 保证 |
| --- | --- |
| 投递语义 | **至少一次（at-least-once）执行尝试** + **至多一次（at-most-once）业务效果** |
| 幂等 | 以 `idempotencyKey` 去重：首次提交持久化后，任何重复提交都返回 `DUPLICATE` 与同一记录，业务处理器对同一键最多被成功提交一次业务效果 |
| 同键并发 | 同一幂等键任意时刻最多一个执行实例：领取需原子完成 `PENDING -> RUNNING`（CAS + 执行代次 epoch），执行中的重复提交仍被识别为重复 |
| 跨键顺序 | **不同键之间不保证顺序**，按工作线程空闲情况并发执行；同一键的多次尝试严格按尝试次数串行 |
| 状态一致 | 所有流转经显式状态机校验，非法回退直接抛 `IllegalStateTransitionException` |

> 说明：框架保证同一键的业务处理器不会被并发调用，且成功结果只会被接受一次。
> 若业务自身存在外部副作用，建议在业务层也做幂等（框架提供的“单次成功 + 执行代次隔离”已避免重复提交结果，但无法感知业务提交瞬间的崩溃）。

## 2. 状态机

```
PENDING ──claim──> RUNNING ──成功──────────────> SUCCEEDED   (终态)
  ^                 │  │
  │                 │  ├──不可重试失败 / 重试耗尽──> DEAD       (终态)
  │                 │  └──可重试失败(仍有额度)────> RETRYING
  │                 └──停机中断恢复──────────────> PENDING
  └────退避到期 re-enqueue──── RETRYING
```

- `SUCCEEDED / DEAD / CANCELED` 为终态，不允许任何回退。
- 停机超时或进程异常终止时遗留的 `RUNNING`，在（重新）启动时原子重置为 `PENDING` 并持久化。
- 规则见 `StateTransition`，非法流转由 `JsonFileTaskStore` 在写盘前拒绝。

## 3. 重试与死信规则

- 处理器抛 `NonRetryableTaskException`（含被包装在 cause 链中）→ 分类为 **NON_RETRYABLE**，**立即进入死信**，不重试。
- 其它异常 → 分类为 **RETRYABLE**，按**指数退避** `min(base * 2^(attempt-1), max)` 等待，
  在 `maxAttempts`（含首次）内重试；达到上限仍失败 → 进入死信，分类依据记为 **RETRY_LIMIT_EXHAUSTED**。
- 死信记录保留可区分的失败信息：
  - `lastFailureClassification`：`RETRYABLE` / `NON_RETRYABLE`；
  - `lastFailureReason`：`NON_RETRYABLE: <异常类型: message>` 或
    `RETRY_LIMIT_EXHAUSTED: <异常类型: message>`；可重试中间失败记为 `RETRYABLE: ...`。
- 分类策略可通过实现 `FailureClassifier` 替换，退避可通过实现 `BackoffPolicy` 替换。

## 4. 并发、背压与优雅停机

- **并发执行**：`task.pipeline.worker-threads` 个工作线程从有界就绪队列领取不同任务并发处理；
  同一键由内存执行集合 + 存储 CAS + 执行代次三重保证不并发。
- **背压**：就绪队列容量为 `task.pipeline.queue-capacity`。提交时**先持久化、再入队**，
  队列满则回滚刚插入的记录并返回 `REJECTED / QUEUE_FULL`（HTTP 503）。
  拒绝原因与业务失败（任务进 `DEAD`）明确区分；停机中提交返回 `REJECTED / SHUTTING_DOWN`。
- **优雅停机**（`stop()` / Spring 容器关闭）：
  1. 置位停机标志，拒绝新提交，重试调度器退出；
  2. 在 `shutdown-wait-millis` 内等待在途任务自然结束（成功或落为 RETRYING 并持久化）；
  3. 超时仍为 `RUNNING` 的任务，**原子批量转为 `PENDING` 并 fsync 到快照文件**，再中断线程；
  4. 重启时 `start()` 恢复：`RUNNING -> PENDING`，已到期的 `RETRYING -> PENDING`，重新入队。
- **执行代次（epoch）**：每次认领 epoch +1。忽略中断的慢线程在停机后迟到的成功/失败写回，
  会因 epoch 不匹配被丢弃，绝不覆盖恢复后的状态，杜绝重复业务效果。

## 5. 持久化

`JsonFileTaskStore` 将全部任务记录以单个 JSON 快照保存在
`<store-dir>/tasks.json`，**每次状态变更在同一把锁内完成并立即持久化**
（写临时文件 + 原子 `move`）。进程崩溃只会停留在变更前或变更后两个一致状态之一，不会出现半条记录。
这是一个面向本地验证 / 单机部署的实现，不依赖外部服务；如需多实例部署可替换 `TaskStore` 实现。

## 6. 配置项（`application.properties`，前缀 `task.pipeline`）

| 键 | 默认值 | 说明 |
| --- | --- | --- |
| `worker-threads` | 2 | 并发工作线程数（同键仍串行） |
| `queue-capacity` | 100 | 就绪队列容量；满时新提交被拒绝（背压阈值） |
| `max-attempts` | 3 | 最大尝试次数（含首次） |
| `backoff-base-millis` | 100 | 指数退避基数 |
| `backoff-max-millis` | 2000 | 指数退避上限 |
| `shutdown-wait-millis` | 5000 | 优雅停机等待在途任务的上限 |
| `store-dir` | target/task-pipeline-store | JSON 快照存储目录 |

## 7. 使用方式

### 编程式

```java
PipelineConfig config = new PipelineConfig();
config.setWorkerThreads(4);
config.setQueueCapacity(200);
TaskPipelineService pipeline = new TaskPipelineService(config, ctx -> {
    // ctx.getIdempotencyKey() / getPayload() / getAttempt()
    // 抛 NonRetryableTaskException -> 立即死信；抛其它异常 -> 退避重试
});
pipeline.start();

SubmissionResult r = pipeline.submit("order-123", "{...}");
// r.isAccepted() / r.isDuplicate() / r.getRejectReason()
TaskRecord rec = pipeline.getStatus("order-123"); // PENDING/RUNNING/SUCCEEDED/RETRYING/DEAD

Runtime.getRuntime().addShutdownHook(new Thread(pipeline::stop));
```

### Spring Boot / HTTP

容器会自动装配流水线；只需声明一个 `TaskHandler` Bean 即可覆盖默认空处理器。

```bash
# 提交（202 ACCEPTED / 200 DUPLICATE / 503 REJECTED）
curl -X POST localhost:8080/tasks -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"order-123","payload":"hello"}'
# 查询
curl localhost:8080/tasks/order-123
```

## 8. 本地验证

```bash
mvn test
```

测试全部基于 JUnit 5 + 临时目录文件存储，不依赖外部服务，结果可重复。日志中会打印
**幂等键、尝试次数、状态流转、退避与失败分类依据**（见 stdout）。

| 测试 | 覆盖场景 |
| --- | --- |
| `StateTransitionTest` | 合法流转、非法回退拒绝、存储层非法流转抛异常 |
| `duplicateSubmissionRecognizedAndBusinessEffectAppliedOnce` | 重复提交识别 + 业务效果仅一次 + 终态不回退 |
| `retryableFailureExhaustsAttemptsAndGoesDeadWithReason` | 退避重试直至耗尽，进死信且原因可区分 |
| `nonRetryableFailureGoesDeadImmediately` | 不可重试失败立即死信，不消耗重试 |
| `concurrentSubmissionsWithSameKeyAreDeduplicatedAndSerialized` | 同键 16 线程并发提交：恰 1 个接受、执行 1 次 |
| `submissionRejectedWithQueueFullWhenBacklogExceedsThreshold` | 背压拒绝 `QUEUE_FULL`，拒绝原因与业务失败区分，回滚后可再提交 |
| `interruptedInFlightTaskIsRecoveredWithoutDuplicateEffect` | 停机中断 → RUNNING 持久化为 PENDING → 迟到结果被代次丢弃 → 重启恢复成功且不重复生效 |
| `differentTasksRunConcurrentlyUpToWorkerLimit` | 不同任务并发度达到 worker 上限 |

手工验证（启动 Web 服务）：

```bash
mvn spring-boot:run
# 另开终端用上面的 curl 命令提交 / 查询；Ctrl+C 停止可观察优雅停机日志
```
