"""副本幂等协调器。

协调器在任务重试时根据源版本、目标 ETag、传输状态、提交租约、清单状态、
冲突版本与剩余重试次数，对每个目标副本做出幂等裁决，并在全部就绪且租约
有效时完成清单提交。

决策顺序（任一前置条件不满足立即短路拒绝）：

1. 已提交清单幂等检查：清单已覆盖源版本的全部目标副本时直接幂等完成。
2. 逐副本裁决：内容一致复用，缺失/残留重新传输，冲突版本拒绝。
3. 汇总：存在拒绝则整体拒绝；仍需重传则挂起提交，清单保持 PENDING。
4. 租约 fencing（仅提交边界）：失联租约或过期 epoch 的旧任务不得提交，
   通过后一次性把清单置为 COMMITTED。
"""

import copy

from typing import Optional

from .models import (
    CommitLease,
    ConflictVersion,
    CoordinatorDecision,
    LeaseState,
    Manifest,
    ManifestEntry,
    ManifestState,
    RejectReason,
    ReplicaAction,
    SourceVersion,
    TargetReplicaState,
    TransferState,
)


class ReplicaIdempotencyCoordinator:
    """无外部 I/O 的纯决策协调器，便于单元测试与嵌入实际复制流程。"""

    def __init__(self, max_retries=3, manifest_version_prefix="manifest"):
        # type: (int, str) -> None
        self.max_retries = max_retries
        self.manifest_version_prefix = manifest_version_prefix
        self._observed_epochs = {}  # type: dict

    def reconcile(
        self,
        task_id,
        source,
        replica_states,
        lease,
        manifest,
        conflict_versions=None,
        retry_count=0,
    ):
        # type: (str, SourceVersion, list, CommitLease, Manifest, list, int) -> CoordinatorDecision
        """对一次复制任务重试/提交请求做出裁决。

        :param task_id: 复制任务标识。
        :param source: 源对象版本（对象键、版本号、ETag）。
        :param replica_states: 各目标副本的 :class:`TargetReplicaState`。
        :param lease: 当前提交租约。
        :param manifest: 当前清单（PENDING 或 COMMITTED）。
        :param conflict_versions: 同一对象键上的其他版本，可为空。
        :param retry_count: 已经发生的重试次数。
        """
        conflict_versions = conflict_versions or []
        replica_ids = [state.replica_id for state in replica_states]

        # 1. 已提交清单幂等：内容与全部目标副本均已提交，重复请求直接完成。
        if (
            manifest.state == ManifestState.COMMITTED
            and manifest.covers(source, replica_ids)
        ):
            return CoordinatorDecision(
                action=ReplicaAction.REUSE,
                replica_actions={
                    replica_id: ReplicaAction.REUSE for replica_id in replica_ids
                },
                manifest=copy.deepcopy(manifest),
                lease_valid=True,
                message="manifest already committed for source version; idempotent no-op",
            )

        # 逐副本裁决，收集分项动作与拒绝原因。
        replica_actions = {}
        rejection = None
        for state in replica_states:
            replica_id, action, reason, message = self._evaluate_target(
                source, state, manifest, conflict_versions, retry_count
            )
            replica_actions[replica_id] = action
            if action == ReplicaAction.REJECT_CONFLICT and rejection is None:
                rejection = (reason, message)

        if rejection is not None:
            reason, message = rejection
            return CoordinatorDecision(
                action=ReplicaAction.REJECT_CONFLICT,
                reason=reason,
                replica_actions=replica_actions,
                manifest=copy.deepcopy(manifest),
                lease_valid=self._lease_usable(lease),
                message=message,
            )

        needs_transfer = any(
            action == ReplicaAction.RETRANSMIT for action in replica_actions.values()
        )
        if needs_transfer:
            return CoordinatorDecision(
                action=ReplicaAction.RETRANSMIT,
                replica_actions=replica_actions,
                manifest=copy.deepcopy(manifest),
                lease_valid=self._lease_usable(lease),
                message="one or more replicas require (re)transmission before commit",
            )

        # 全部副本内容就绪，到达提交边界，租约 fencing 生效。
        lease_error = self._validate_lease_at_commit(task_id, lease)
        if lease_error is not None:
            reason, message = lease_error
            return CoordinatorDecision(
                action=ReplicaAction.REJECT_CONFLICT,
                reason=reason,
                replica_actions=replica_actions,
                manifest=copy.deepcopy(manifest),
                lease_valid=False,
                message=message,
            )

        committed = self._commit_manifest(task_id, source, replica_ids, manifest, lease)
        return CoordinatorDecision(
            action=ReplicaAction.COMMIT,
            replica_actions=replica_actions,
            manifest=committed,
            lease_valid=True,
            message="all replicas ready; manifest committed",
        )

    def _evaluate_target(
        self, source, state, manifest, conflict_versions, retry_count
    ):
        # type: (SourceVersion, TargetReplicaState, Manifest, list, int) -> tuple
        replica_id = state.replica_id

        # 清单中已存在该副本的相同内容记录：幂等复用，不重复传输。
        if manifest.contains(source, replica_id):
            return replica_id, ReplicaAction.REUSE, None, None

        # 目标上已有完整数据且 ETag 与源一致：重试时直接复用。
        if (
            state.transfer_state == TransferState.COMPLETED
            and state.etag == source.etag
        ):
            return replica_id, ReplicaAction.REUSE, None, None

        # 目标上存在内容，但 ETag 与源不同：必须先判定冲突，禁止静默覆盖。
        if state.etag is not None and state.etag != source.etag:
            conflict = self._find_conflict(source, conflict_versions)
            if conflict is not None and conflict.etag == state.etag:
                if conflict.committed:
                    return (
                        replica_id,
                        ReplicaAction.REJECT_CONFLICT,
                        RejectReason.COMMITTED_VERSION_CONFLICT,
                        "target holds committed version {} with different content; "
                        "overwrite is forbidden".format(conflict.version_id),
                    )
                return (
                    replica_id,
                    ReplicaAction.REJECT_CONFLICT,
                    RejectReason.CONFLICTING_VERSION_PRESENT,
                    "object key is occupied by in-flight version {} from task {}".format(
                        conflict.version_id, conflict.claimed_by
                    ),
                )

        # 缺失或传输中断残留：需要（重新）传输，先检查剩余重试次数。
        if retry_count >= self.max_retries:
            return (
                replica_id,
                ReplicaAction.REJECT_CONFLICT,
                RejectReason.RETRY_LIMIT_EXCEEDED,
                "retry count {} exhausted; cannot schedule another transmission".format(
                    retry_count
                ),
            )
        return (
            replica_id,
            ReplicaAction.RETRANSMIT,
            None,
            None,
        )

    @staticmethod
    def _find_conflict(source, conflict_versions):
        # type: (SourceVersion, list) -> ConflictVersion
        for conflict in conflict_versions:
            if conflict.object_key == source.object_key:
                return conflict
        return None

    def _lease_usable(self, lease):
        # type: (CommitLease) -> bool
        if lease is None or lease.state != LeaseState.HELD:
            return False
        observed = self._observed_epochs.get(lease.task_id)
        return observed is None or lease.epoch >= observed

    def _validate_lease_at_commit(self, task_id, lease):
        # type: (str, CommitLease) -> Optional[tuple]
        if lease is None or lease.state == LeaseState.LOST:
            return (
                RejectReason.LEASE_LOST,
                "commit lease lost; stale task cannot submit the final manifest",
            )
        observed = self._observed_epochs.get(task_id)
        if observed is not None and lease.epoch < observed:
            return (
                RejectReason.LEASE_STALE_EPOCH,
                "lease epoch {} is older than observed epoch {}".format(
                    lease.epoch, observed
                ),
            )
        self._observed_epochs[task_id] = lease.epoch
        return None

    def _commit_manifest(self, task_id, source, replica_ids, manifest, lease):
        # type: (str, SourceVersion, list, Manifest, CommitLease) -> Manifest
        committed = copy.deepcopy(manifest)
        committed.task_id = task_id
        for replica_id in replica_ids:
            committed.add(
                ManifestEntry(
                    source.object_key, source.version_id, source.etag, replica_id
                )
            )
        committed.state = ManifestState.COMMITTED
        committed.manifest_version = "{}-{}-{}".format(
            self.manifest_version_prefix, source.version_id, lease.epoch
        )
        committed.committed_epoch = lease.epoch
        return committed
