# object-replica-idempotency-reconciler
协调对象副本重复传输、版本冲突和幂等提交

## 背景

对象复制任务在网络中断后会重试，此时可能出现三种棘手情况：

1. 目标副本上已经存在与源版本相同的内容，但清单（manifest）尚未提交；
2. 另一个版本正使用同一个对象键上传不同内容；
3. 提交租约（commit lease）失联，旧任务苏醒后仍试图提交最终清单。

`ReplicaIdempotencyCoordinator` 是一个无外部 I/O 的纯决策组件，输入源版本、
目标 ETag、传输状态、提交租约、清单、冲突版本和重试次数，对每个目标副本
输出四类动作之一：

- `REUSE`：内容一致，幂等复用已有副本，不重复传输；
- `RETRANSMIT`：副本缺失或残留中断数据，需要（重新）传输；
- `REJECT_CONFLICT`：与已提交版本或在途版本冲突，拒绝并给出原因；
- `COMMIT`：全部副本就绪且租约有效，提交最终清单。

兼容 Python 3.8.2（仅使用 `typing.Optional/List/dict` 与 `enum`，无 3.9+ 语法）。

## 决策规则

协调器按以下顺序短路裁决：

1. **已提交清单幂等**：清单状态为 `COMMITTED` 且已覆盖源版本的全部目标副本时，
   重复请求直接以 `REUSE` 完成，不产生新的清单版本（提交响应丢失后的重放安全）。
2. **逐副本内容裁决**：
   - 清单已记录该副本的相同内容，或目标 `COMPLETED` 且 ETag 等于源 ETag → `REUSE`；
   - 目标存在数据但 ETag 与源不同，且能匹配到冲突版本 → `REJECT_CONFLICT`
     （已提交版本给 `COMMITTED_VERSION_CONFLICT`，在途占用给
     `CONFLICTING_VERSION_PRESENT`），**不同内容永远不会覆盖已提交版本**；
   - 副本 `ABSENT` 或 `IN_PROGRESS` 残留且无冲突 → `RETRANSMIT`；
   - 需要重传但 `retry_count >= max_retries` → 以 `RETRY_LIMIT_EXCEEDED` 拒绝。
3. **汇总**：任一副本被拒绝则整体拒绝；任一副本需要重传则整体 `RETRANSMIT`，
   清单保持 `PENDING`；全部就绪才进入提交边界。
4. **租约 fencing（仅在提交边界生效）**：
   - 租约状态为 `LOST` → 以 `LEASE_LOST` 拒绝，**失联后的旧任务不能提交最终清单**；
   - 租约 epoch 小于本协调器已观察到的 epoch → 以 `LEASE_STALE_EPOCH` 拒绝；
   - 校验通过后记录 epoch，并把清单置为 `COMMITTED`，生成
     `manifest-<version_id>-<epoch>` 形式的清单版本号。

## 清单提交边界

清单是“哪些副本持有哪个对象版本”的唯一权威记录，其提交边界有明确约定：

- **边界之内（数据面）**：`REUSE` 与 `RETRANSMIT` 只影响目标副本上的数据，
  清单始终保持 `PENDING`，中途崩溃不会留下“已声明但不存在”的副本记录。
- **边界动作（原子提交）**：只有当所有目标副本都被裁决为 `REUSE`（内容已确认
  一致）且提交租约 fencing 通过时，协调器才一次性写入全部清单条目并把状态翻转为
  `COMMITTED`。提交是幂等的：已提交清单覆盖相同源版本与副本集合时，重试/重放
  请求得到相同结果，不生成新版本。
- **边界之外（不可变）**：清单一旦 `COMMITTED`，对应对象版本即视为已提交版本；
  同一对象键上的不同内容只能作为新版本走冲突检查，禁止覆盖已提交版本。
- **租约是边界的准入凭证**：数据传输阶段不强制租约，旧任务仍可安全重传幂等
  数据；但跨入提交边界时必须持有 `HELD` 且 epoch 不旧的租约，失联或被新 epoch
  取代的旧任务一律拒绝提交。

## 使用示例

```python
from replica_coordinator import (
    CommitLease, LeaseState, Manifest, ManifestState,
    ReplicaIdempotencyCoordinator, SourceVersion, TargetReplicaState, TransferState,
)

coordinator = ReplicaIdempotencyCoordinator(max_retries=3)
source = SourceVersion("objects/photo.jpg", "v1", "etag-v1")
replicas = [
    TargetReplicaState("replica-a", TransferState.COMPLETED, etag="etag-v1"),
    TargetReplicaState("replica-b", TransferState.ABSENT),
]
lease = CommitLease("task-1", epoch=1, state=LeaseState.HELD)
manifest = Manifest("task-1", ManifestState.PENDING)

decision = coordinator.reconcile(
    "task-1", source, replicas, lease, manifest,
    conflict_versions=[], retry_count=0,
)
decision.action             # ReplicaAction.RETRANSMIT（B 缺失，A 复用）
decision.replica_actions    # {"replica-a": REUSE, "replica-b": RETRANSMIT}
decision.manifest.state     # ManifestState.PENDING
```

## 测试

```bash
python -m unittest
```

测试覆盖：重复传输的幂等复用与已提交清单重放、同键不同内容的冲突拒绝、
提交中断与租约失联/旧 epoch fencing、重试上限，以及多目标副本的部分复用、
整单拒绝和统一提交。
