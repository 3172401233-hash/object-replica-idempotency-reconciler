"""副本幂等协调器单元测试。

覆盖：重复传输幂等复用、内容冲突拒绝、提交中断与租约失联、多目标副本、
重试上限以及已提交清单的重复请求。
"""

import unittest

from replica_coordinator import (
    CommitLease,
    ConflictVersion,
    CoordinatorDecision,
    LeaseState,
    Manifest,
    ManifestEntry,
    ManifestState,
    RejectReason,
    ReplicaAction,
    ReplicaIdempotencyCoordinator,
    SourceVersion,
    TargetReplicaState,
    TransferState,
)


def make_source(etag="etag-v1", version_id="v1"):
    return SourceVersion(object_key="objects/photo.jpg", version_id=version_id, etag=etag)


def make_lease(epoch=1, state=LeaseState.HELD, task_id="task-1"):
    return CommitLease(task_id=task_id, epoch=epoch, state=state)


def pending_manifest(task_id="task-1"):
    return Manifest(task_id=task_id, state=ManifestState.PENDING)


class DuplicateTransferTest(unittest.TestCase):
    def setUp(self):
        self.coordinator = ReplicaIdempotencyCoordinator(max_retries=3)
        self.source = make_source()

    def test_absent_target_schedules_transfer(self):
        state = TargetReplicaState("replica-a", TransferState.ABSENT)
        decision = self.coordinator.reconcile(
            "task-1",
            self.source,
            [state],
            make_lease(),
            pending_manifest(),
            retry_count=0,
        )
        self.assertEqual(decision.action, ReplicaAction.RETRANSMIT)
        self.assertEqual(
            decision.replica_actions["replica-a"], ReplicaAction.RETRANSMIT
        )

    def test_retry_with_same_completed_content_is_reused(self):
        # 重试时目标副本已存在相同内容，但清单尚未提交：必须幂等复用并完成提交。
        state = TargetReplicaState(
            "replica-a", TransferState.COMPLETED, etag=self.source.etag
        )
        decision = self.coordinator.reconcile(
            "task-1",
            self.source,
            [state],
            make_lease(epoch=1),
            pending_manifest(),
            retry_count=1,
        )
        self.assertEqual(decision.action, ReplicaAction.COMMIT)
        self.assertEqual(decision.replica_actions["replica-a"], ReplicaAction.REUSE)
        self.assertEqual(decision.manifest.state, ManifestState.COMMITTED)
        self.assertTrue(decision.manifest.contains(self.source, "replica-a"))
        self.assertEqual(decision.manifest.committed_epoch, 1)

    def test_manifest_entry_for_same_content_allows_reuse(self):
        manifest = pending_manifest()
        manifest.add(
            ManifestEntry(
                self.source.object_key,
                self.source.version_id,
                self.source.etag,
                "replica-a",
            )
        )
        # 即使目标仍显示传输中断，清单已记录相同内容，按幂等复用处理。
        state = TargetReplicaState(
            "replica-a", TransferState.IN_PROGRESS, etag=self.source.etag
        )
        decision = self.coordinator.reconcile(
            "task-1", self.source, [state], make_lease(), manifest, retry_count=2
        )
        self.assertEqual(
            decision.replica_actions["replica-a"], ReplicaAction.REUSE
        )
        self.assertEqual(decision.action, ReplicaAction.COMMIT)

    def test_committed_manifest_replay_is_idempotent_noop(self):
        state = TargetReplicaState(
            "replica-a", TransferState.COMPLETED, etag=self.source.etag
        )
        first = self.coordinator.reconcile(
            "task-1",
            self.source,
            [state],
            make_lease(epoch=1),
            pending_manifest(),
        )
        self.assertEqual(first.action, ReplicaAction.COMMIT)

        # 提交响应丢失后客户端重放：不再产生新提交，直接幂等完成。
        replay = self.coordinator.reconcile(
            "task-1",
            self.source,
            [state],
            make_lease(epoch=2),
            first.manifest,
        )
        self.assertEqual(replay.action, ReplicaAction.REUSE)
        self.assertEqual(
            replay.replica_actions["replica-a"], ReplicaAction.REUSE
        )
        self.assertEqual(replay.manifest.manifest_version, first.manifest.manifest_version)


class ContentConflictTest(unittest.TestCase):
    def setUp(self):
        self.coordinator = ReplicaIdempotencyCoordinator(max_retries=3)

    def test_different_content_against_committed_version_is_rejected(self):
        source = make_source(etag="etag-v2", version_id="v2")
        state = TargetReplicaState(
            "replica-a", TransferState.COMPLETED, etag="etag-v1"
        )
        conflict = ConflictVersion(
            object_key=source.object_key,
            version_id="v1",
            etag="etag-v1",
            committed=True,
        )
        decision = self.coordinator.reconcile(
            "task-9",
            source,
            [state],
            make_lease(task_id="task-9"),
            pending_manifest("task-9"),
            conflict_versions=[conflict],
        )
        self.assertTrue(decision.rejected)
        self.assertEqual(
            decision.reason, RejectReason.COMMITTED_VERSION_CONFLICT
        )
        self.assertEqual(
            decision.replica_actions["replica-a"], ReplicaAction.REJECT_CONFLICT
        )
        self.assertEqual(decision.manifest.state, ManifestState.PENDING)

    def test_in_flight_conflicting_version_blocks_same_key_upload(self):
        source = make_source(etag="etag-v2", version_id="v2")
        state = TargetReplicaState(
            "replica-a", TransferState.IN_PROGRESS, etag="etag-v3"
        )
        conflict = ConflictVersion(
            object_key=source.object_key,
            version_id="v3",
            etag="etag-v3",
            committed=False,
            claimed_by="task-other",
        )
        decision = self.coordinator.reconcile(
            "task-9",
            source,
            [state],
            make_lease(task_id="task-9"),
            pending_manifest("task-9"),
            conflict_versions=[conflict],
        )
        self.assertTrue(decision.rejected)
        self.assertEqual(
            decision.reason, RejectReason.CONFLICTING_VERSION_PRESENT
        )

    def test_interrupted_partial_upload_without_conflict_retransmits(self):
        # 同一任务此前中断留下的部分数据（无冲突版本记录）可以安全重传覆盖。
        source = make_source()
        state = TargetReplicaState(
            "replica-a", TransferState.IN_PROGRESS, etag="partial-etag"
        )
        decision = self.coordinator.reconcile(
            "task-1",
            source,
            [state],
            make_lease(),
            pending_manifest(),
            conflict_versions=[],
            retry_count=1,
        )
        self.assertEqual(decision.action, ReplicaAction.RETRANSMIT)

    def test_retry_limit_exhausted_rejects_further_transfer(self):
        coordinator = ReplicaIdempotencyCoordinator(max_retries=2)
        source = make_source()
        state = TargetReplicaState("replica-a", TransferState.ABSENT)
        decision = coordinator.reconcile(
            "task-1",
            source,
            [state],
            make_lease(),
            pending_manifest(),
            retry_count=2,
        )
        self.assertTrue(decision.rejected)
        self.assertEqual(decision.reason, RejectReason.RETRY_LIMIT_EXCEEDED)


class CommitInterruptionTest(unittest.TestCase):
    def setUp(self):
        self.coordinator = ReplicaIdempotencyCoordinator(max_retries=3)
        self.source = make_source()

    def test_lost_lease_cannot_commit_final_manifest(self):
        state = TargetReplicaState(
            "replica-a", TransferState.COMPLETED, etag=self.source.etag
        )
        lease = CommitLease(
            task_id="task-1", epoch=1, state=LeaseState.LOST
        )
        decision = self.coordinator.reconcile(
            "task-1",
            self.source,
            [state],
            lease,
            pending_manifest(),
        )
        self.assertTrue(decision.rejected)
        self.assertEqual(decision.reason, RejectReason.LEASE_LOST)
        self.assertFalse(decision.lease_valid)
        self.assertEqual(decision.manifest.state, ManifestState.PENDING)

    def test_stale_epoch_after_reacquisition_is_fenced(self):
        # 第一次协调以 epoch=5 成功提交。
        state = TargetReplicaState(
            "replica-a", TransferState.COMPLETED, etag=self.source.etag
        )
        first = self.coordinator.reconcile(
            "task-1",
            self.source,
            [state],
            make_lease(epoch=5),
            pending_manifest(),
        )
        self.assertEqual(first.action, ReplicaAction.COMMIT)

        # 失联的旧任务持 epoch=4 苏醒并尝试提交另一个未提交清单，必须被 fencing。
        stale = CommitLease(
            task_id="task-1", epoch=4, state=LeaseState.HELD
        )
        decision = self.coordinator.reconcile(
            "task-1",
            self.source,
            [
                TargetReplicaState(
                    "replica-b", TransferState.COMPLETED, etag=self.source.etag
                )
            ],
            stale,
            pending_manifest(),
        )
        self.assertTrue(decision.rejected)
        self.assertEqual(decision.reason, RejectReason.LEASE_STALE_EPOCH)

    def test_interrupted_commit_resumes_with_new_lease(self):
        # 首次协调：副本 A 完成、副本 B 中断，要求重新传输 B，清单保持 PENDING。
        states = [
            TargetReplicaState(
                "replica-a", TransferState.COMPLETED, etag=self.source.etag
            ),
            TargetReplicaState("replica-b", TransferState.IN_PROGRESS),
        ]
        first = self.coordinator.reconcile(
            "task-1",
            self.source,
            states,
            make_lease(epoch=1),
            pending_manifest(),
            retry_count=0,
        )
        self.assertEqual(first.action, ReplicaAction.RETRANSMIT)
        self.assertEqual(
            first.replica_actions["replica-a"], ReplicaAction.REUSE
        )
        self.assertEqual(
            first.replica_actions["replica-b"], ReplicaAction.RETRANSMIT
        )
        self.assertEqual(first.manifest.state, ManifestState.PENDING)

        # 重传完成后再次协调（新租约 epoch=2）：完成提交。
        states[1] = TargetReplicaState(
            "replica-b", TransferState.COMPLETED, etag=self.source.etag
        )
        second = self.coordinator.reconcile(
            "task-1",
            self.source,
            states,
            make_lease(epoch=2),
            first.manifest,
            retry_count=1,
        )
        self.assertEqual(second.action, ReplicaAction.COMMIT)
        self.assertEqual(second.manifest.committed_epoch, 2)
        self.assertTrue(second.manifest.covers(self.source, ["replica-a", "replica-b"]))


class MultiTargetReplicaTest(unittest.TestCase):
    def setUp(self):
        self.coordinator = ReplicaIdempotencyCoordinator(max_retries=3)
        self.source = make_source()

    def test_all_replicas_ready_commits_single_manifest(self):
        states = [
            TargetReplicaState(
                "replica-a", TransferState.COMPLETED, etag=self.source.etag
            ),
            TargetReplicaState(
                "replica-b", TransferState.COMPLETED, etag=self.source.etag
            ),
            TargetReplicaState(
                "replica-c", TransferState.COMPLETED, etag=self.source.etag
            ),
        ]
        decision = self.coordinator.reconcile(
            "task-1",
            self.source,
            states,
            make_lease(epoch=7),
            pending_manifest(),
        )
        self.assertEqual(decision.action, ReplicaAction.COMMIT)
        self.assertEqual(len(decision.manifest.entries), 3)
        self.assertTrue(
            decision.manifest.covers(
                self.source, ["replica-a", "replica-b", "replica-c"]
            )
        )

    def test_one_conflicting_replica_rejects_whole_commit(self):
        conflict = ConflictVersion(
            object_key=self.source.object_key,
            version_id="v0",
            etag="etag-old",
            committed=True,
        )
        states = [
            TargetReplicaState(
                "replica-a", TransferState.COMPLETED, etag=self.source.etag
            ),
            TargetReplicaState(
                "replica-b", TransferState.COMPLETED, etag="etag-old"
            ),
        ]
        decision = self.coordinator.reconcile(
            "task-1",
            self.source,
            states,
            make_lease(),
            pending_manifest(),
            conflict_versions=[conflict],
        )
        self.assertTrue(decision.rejected)
        self.assertEqual(
            decision.reason, RejectReason.COMMITTED_VERSION_CONFLICT
        )
        self.assertEqual(
            decision.replica_actions["replica-a"], ReplicaAction.REUSE
        )
        self.assertEqual(
            decision.replica_actions["replica-b"], ReplicaAction.REJECT_CONFLICT
        )
        self.assertEqual(decision.manifest.state, ManifestState.PENDING)

    def test_mixed_reuse_and_retransmit_defers_commit(self):
        states = [
            TargetReplicaState(
                "replica-a", TransferState.COMPLETED, etag=self.source.etag
            ),
            TargetReplicaState("replica-b", TransferState.ABSENT),
        ]
        decision = self.coordinator.reconcile(
            "task-1",
            self.source,
            states,
            make_lease(),
            pending_manifest(),
        )
        self.assertEqual(decision.action, ReplicaAction.RETRANSMIT)
        self.assertEqual(
            decision.replica_actions["replica-a"], ReplicaAction.REUSE
        )
        self.assertEqual(
            decision.replica_actions["replica-b"], ReplicaAction.RETRANSMIT
        )
        self.assertEqual(decision.manifest.state, ManifestState.PENDING)


if __name__ == "__main__":
    unittest.main()
