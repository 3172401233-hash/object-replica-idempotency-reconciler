"""协调器使用的领域模型（纯数据，Python 3.8.2 兼容）。"""

from enum import Enum
from typing import List, Optional


class TransferState(str, Enum):
    """目标副本当前的传输状态。"""

    ABSENT = "absent"            # 目标上还没有任何数据
    IN_PROGRESS = "in_progress"  # 传输中断，目标上残留了部分数据
    COMPLETED = "completed"      # 数据已完整落盘（ETag 可用）


class LeaseState(str, Enum):
    """提交租约的状态。"""

    HELD = "held"    # 租约有效
    LOST = "lost"    # 租约曾持有但已失联/过期


class ManifestState(str, Enum):
    """清单状态。"""

    PENDING = "pending"      # 尚未提交
    COMMITTED = "committed"  # 已提交，版本成为已提交版本，不可覆盖


class ReplicaAction(str, Enum):
    """针对单个目标副本的动作。"""

    REUSE = "reuse"                  # 内容一致，幂等复用已有副本
    RETRANSMIT = "retransmit"        # 内容缺失/不一致，重新传输
    REJECT_CONFLICT = "reject"       # 与已提交/占用版本冲突，拒绝
    COMMIT = "commit"                # 本副本就绪，允许提交清单


class RejectReason(str, Enum):
    """拒绝类决策的原因。"""

    COMMITTED_VERSION_CONFLICT = "committed_version_conflict"
    CONFLICTING_VERSION_PRESENT = "conflicting_version_present"
    LEASE_LOST = "lease_lost"
    LEASE_STALE_EPOCH = "lease_stale_epoch"
    RETRY_LIMIT_EXCEEDED = "retry_limit_exceeded"


class SourceVersion:
    """源端对象版本。

    ``object_key`` 相同、``version_id`` 不同代表同一对象键上的不同内容。
    """

    def __init__(self, object_key, version_id, etag):
        # type: (str, str, str) -> None
        self.object_key = object_key
        self.version_id = version_id
        self.etag = etag

    def __eq__(self, other):
        if not isinstance(other, SourceVersion):
            return NotImplemented
        return (
            self.object_key == other.object_key
            and self.version_id == other.version_id
            and self.etag == other.etag
        )

    def __repr__(self):
        return "SourceVersion(key={!r}, version={!r}, etag={!r})".format(
            self.object_key, self.version_id, self.etag
        )


class TargetReplicaState:
    """单个目标副本上的传输状态与 ETag。"""

    def __init__(self, replica_id, transfer_state, etag=None):
        # type: (str, TransferState, Optional[str]) -> None
        self.replica_id = replica_id
        self.transfer_state = transfer_state
        self.etag = etag

    def __repr__(self):
        return "TargetReplicaState(id={!r}, state={!r}, etag={!r})".format(
            self.replica_id, self.transfer_state, self.etag
        )


class ConflictVersion:
    """同一对象键上可能存在的另一个版本。

    ``committed`` 为 True 表示该版本已经由清单提交，任何不同内容都不得覆盖；
    为 False 表示只是另一个任务声明/正在传输的占用版本，同样需要拒绝。
    """

    def __init__(self, object_key, version_id, etag, committed=False, claimed_by=None):
        # type: (str, str, str, bool, Optional[str]) -> None
        self.object_key = object_key
        self.version_id = version_id
        self.etag = etag
        self.committed = committed
        self.claimed_by = claimed_by

    def __repr__(self):
        return (
            "ConflictVersion(key={!r}, version={!r}, etag={!r}, "
            "committed={!r}, claimed_by={!r})"
        ).format(
            self.object_key,
            self.version_id,
            self.etag,
            self.committed,
            self.claimed_by,
        )


class CommitLease:
    """提交租约（fencing 令牌）。

    ``epoch`` 单调递增，协调器只接受不小于已观察到的 epoch。
    失联后拿到的旧任务持有的是较小 epoch，提交会被拒绝。
    """

    def __init__(self, task_id, epoch, state):
        # type: (str, int, LeaseState) -> None
        self.task_id = task_id
        self.epoch = epoch
        self.state = state

    def __repr__(self):
        return "CommitLease(task={!r}, epoch={!r}, state={!r})".format(
            self.task_id, self.epoch, self.state
        )


class ManifestEntry:
    """清单中的一条副本记录。"""

    def __init__(self, object_key, version_id, etag, replica_id):
        # type: (str, str, str, str) -> None
        self.object_key = object_key
        self.version_id = version_id
        self.etag = etag
        self.replica_id = replica_id

    def matches(self, source, replica_id):
        # type: (SourceVersion, str) -> bool
        return (
            self.object_key == source.object_key
            and self.version_id == source.version_id
            and self.etag == source.etag
            and self.replica_id == replica_id
        )

    def __eq__(self, other):
        if not isinstance(other, ManifestEntry):
            return NotImplemented
        return (
            self.object_key == other.object_key
            and self.version_id == other.version_id
            and self.etag == other.etag
            and self.replica_id == other.replica_id
        )

    def __repr__(self):
        return "ManifestEntry(key={!r}, version={!r}, etag={!r}, replica={!r})".format(
            self.object_key, self.version_id, self.etag, self.replica_id
        )


class Manifest:
    """复制任务的提交清单。"""

    def __init__(self, task_id, state, entries=None, manifest_version=None, committed_epoch=None):
        # type: (str, ManifestState, Optional[List[ManifestEntry]], Optional[str], Optional[int]) -> None
        self.task_id = task_id
        self.state = state
        self.entries = list(entries) if entries else []
        self.manifest_version = manifest_version
        self.committed_epoch = committed_epoch

    def contains(self, source, replica_id):
        # type: (SourceVersion, str) -> bool
        return any(entry.matches(source, replica_id) for entry in self.entries)

    def covers(self, source, replica_ids):
        # type: (SourceVersion, List[str]) -> bool
        return all(self.contains(source, replica_id) for replica_id in replica_ids)

    def add(self, entry):
        # type: (ManifestEntry) -> None
        if not any(entry == existing for existing in self.entries):
            self.entries.append(entry)

    def __repr__(self):
        return "Manifest(task={!r}, state={!r}, entries={!r})".format(
            self.task_id, self.state, self.entries
        )


class CoordinatorDecision:
    """协调器对一次重试/提交请求的裁决。

    ``action`` 是整体动作；``replica_actions`` 给出每个目标副本的分项动作；
    拒绝时 ``reason`` 给出原因；提交成功时 ``manifest`` 为已提交的新清单。
    """

    def __init__(
        self,
        action,
        reason=None,
        replica_actions=None,
        manifest=None,
        lease_valid=None,
        message="",
    ):
        # type: (ReplicaAction, Optional[RejectReason], Optional[dict], Optional[Manifest], Optional[bool], str) -> None
        self.action = action
        self.reason = reason
        self.replica_actions = dict(replica_actions) if replica_actions else {}
        self.manifest = manifest
        self.lease_valid = lease_valid
        self.message = message

    @property
    def rejected(self):
        return self.action == ReplicaAction.REJECT_CONFLICT

    def __repr__(self):
        return "CoordinatorDecision(action={!r}, reason={!r})".format(
            self.action, self.reason
        )
