package com.sds.batchservice.cluster;

import com.sds.batchservice.cluster.consensus.RaftNode;
import com.sds.batchservice.cluster.consensus.RaftRole;
import com.sds.batchservice.cluster.consensus.RaftTimings;
import com.sds.batchservice.cluster.consensus.transport.Message;
import com.sds.batchservice.cluster.consensus.transport.NetworkTransport;
import com.sds.batchservice.cluster.fsm.ClusterStateMachine;
import com.sds.batchservice.cluster.fsm.Command;
import com.sds.batchservice.cluster.fsm.CommandResult;
import com.sds.batchservice.cluster.fsm.CommandType;
import com.sds.batchservice.cluster.fsm.LogEntry;
import com.sds.batchservice.cluster.mock.VirtualCluster;
import com.sds.batchservice.cluster.recovery.ExecutionStatusProvider.ExecutionStatus;
import com.sds.batchservice.cluster.recovery.RecoveryCoordinator;
import com.sds.batchservice.cluster.storage.RaftLogManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.sds.batchservice.cluster.ArchitectureVerificationTest.assertLogsConverged;
import static com.sds.batchservice.cluster.ArchitectureVerificationTest.awaitLeader;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 설계 검토에서 식별된 결함의 회귀 테스트 (5.3.2 공통 런타임 규약의 안전성 불변식).
 */
class RaftSafetyTest {

    @Test
    @DisplayName("리더 교체 후에도 로그 인덱스가 이어지고 락·큐 명령이 정상 커밋된다 (인덱스 0 재할당 결함 회귀)")
    void failoverKeepsLogIndicesConsistent(@TempDir Path dir) throws Exception {
        VirtualCluster cluster = new VirtualCluster(3, dir);
        cluster.start();
        try {
            RaftNode leader = awaitLeader(cluster);
            for (int i = 1; i <= 5; i++) {
                assertThat(cluster.getQueueService(leader.getNodeId()).offer("JOB-" + i)).isTrue();
            }
            String oldLeaderId = leader.getNodeId();
            cluster.crash(oldLeaderId);

            RaftNode newLeader = await().atMost(5, TimeUnit.SECONDS).until(cluster::getLeaderNode,
                    l -> l != null && !l.getNodeId().equals(oldLeaderId));
            long lastIndexBefore = newLeader.getRaftLog().lastIndex();
            long fenceToken = cluster.getLockExecutor(newLeader.getNodeId())
                    .acquire("lock-after-failover", newLeader.getNodeId() + ":t", 2_000L, 5_000L);

            assertThat(fenceToken).isGreaterThan(lastIndexBefore);
            assertThat(newLeader.getStateMachine().getQueue().size()).isEqualTo(5);
            assertThat(cluster.getQueueService(newLeader.getNodeId()).offer("JOB-6")).isTrue();

            cluster.restart(oldLeaderId);
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                assertLogsConverged(cluster);
                cluster.runningNodes().forEach(n -> assertThat(n.getStateMachine().getQueue().size()).isEqualTo(6));
            });
        } finally {
            cluster.shutdown();
        }
    }

    @Test
    @DisplayName("로그가 뒤처진 노드는 리더로 선출되지 않고 커밋된 엔트리가 보존된다 (로그 최신성 투표 검사)")
    void staleLogNodeCannotBecomeLeader(@TempDir Path dir) throws Exception {
        VirtualCluster cluster = new VirtualCluster(3, dir);
        cluster.start();
        try {
            RaftNode leader = awaitLeader(cluster);
            List<String> followers = cluster.getNodeIds().stream().filter(id -> !id.equals(leader.getNodeId())).toList();
            String stale = followers.get(0);
            String upToDate = followers.get(1);

            cluster.getNetworkRouter().isolateNode(stale);
            for (int i = 1; i <= 10; i++) {
                assertThat(cluster.getQueueService(leader.getNodeId()).offer("JOB-" + i)).isTrue();
            }
            await().atMost(2, TimeUnit.SECONDS).until(() ->
                    cluster.getNode(upToDate).getStateMachine().getQueue().size() == 10);
            long staleTermBefore = cluster.getNode(stale).getCurrentTerm();

            cluster.crash(leader.getNodeId());
            cluster.getNetworkRouter().reconnectNode(stale);

            RaftNode newLeader = await().atMost(5, TimeUnit.SECONDS).until(cluster::getLeaderNode, java.util.Objects::nonNull);
            assertThat(newLeader.getNodeId()).isEqualTo(upToDate);
            assertThat(staleTermBefore).as("PreVote prevents term inflation while isolated").isLessThanOrEqualTo(leader.getCurrentTerm());
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(cluster.getNode(stale).getStateMachine().getQueue().size()).isEqualTo(10);
                assertLogsConverged(cluster);
            });
        } finally {
            cluster.shutdown();
        }
    }

    @Test
    @DisplayName("재기동 노드는 WAL로 FSM을 복원한 뒤 로그를 따라잡고 멤버십에 재합류한다 (Scenario 3)")
    void restartedNodeRecoversFromWalAndRejoins(@TempDir Path dir) throws Exception {
        VirtualCluster cluster = new VirtualCluster(3, dir);
        cluster.start();
        try {
            RaftNode leader = awaitLeader(cluster);
            String follower = cluster.getNodeIds().stream().filter(id -> !id.equals(leader.getNodeId())).findFirst().orElseThrow();
            for (int i = 1; i <= 5; i++) {
                cluster.getQueueService(leader.getNodeId()).offer("JOB-" + i);
            }
            await().atMost(2, TimeUnit.SECONDS).until(() -> cluster.getNode(follower).getStateMachine().getQueue().size() == 5);
            Thread.sleep(1_200L); // commitIndex 주기적 영속화(1초) 이후 크래시

            cluster.crash(follower);
            await().atMost(3, TimeUnit.SECONDS).until(() -> !leader.getActiveMembers().contains(follower));
            for (int i = 6; i <= 10; i++) {
                cluster.getQueueService(leader.getNodeId()).offer("JOB-" + i);
            }

            cluster.restart(follower);
            RaftNode restarted = cluster.getNode(follower);
            assertThat(restarted.getStateMachine().getQueue().size()).isGreaterThanOrEqualTo(5); // WAL Replay 복원

            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(restarted.getStateMachine().getQueue().size()).isEqualTo(10);
                assertThat(leader.getActiveMembers()).contains(follower);
                assertThat(restarted.isReady()).isTrue();
            });
        } finally {
            cluster.shutdown();
        }
    }

    @Test
    @DisplayName("동일 requestId로 재시도한 명령은 한 번만 반영되고 동일 결과를 반환한다")
    void retriedRequestIsAppliedOnce(@TempDir Path dir) throws Exception {
        VirtualCluster cluster = new VirtualCluster(3, dir);
        cluster.start();
        try {
            RaftNode leader = awaitLeader(cluster);
            cluster.getQueueService(leader.getNodeId()).offer("JOB-A");
            cluster.getQueueService(leader.getNodeId()).offer("JOB-B");

            Command dequeue = Command.dequeue("worker-x", 30_000L);
            CommandResult first = leader.execute(dequeue, 2_000L);
            CommandResult retried = leader.execute(dequeue, 2_000L);

            assertThat(retried.getValue()).isEqualTo(first.getValue()).isEqualTo("JOB-A");
            assertThat(retried.getToken()).isEqualTo(first.getToken());
            assertThat(leader.getStateMachine().getQueue().waitingSnapshot()).containsExactly("JOB-B");
        } finally {
            cluster.shutdown();
        }
    }

    @Test
    @DisplayName("lease 만료 후 새 소유자에게 더 큰 fenceToken이 발급되고 구 토큰의 해제는 거부된다")
    void staleFenceTokenIsRejected(@TempDir Path dir) throws Exception {
        VirtualCluster cluster = new VirtualCluster(3, dir);
        cluster.start();
        try {
            RaftNode leader = awaitLeader(cluster);
            var executor = cluster.getLockExecutor(leader.getNodeId());
            long tokenA = executor.acquire("fence-lock", "worker:A", 1_000L, 300L);
            Thread.sleep(400L);
            long tokenB = executor.acquire("fence-lock", "worker:B", 2_000L, 5_000L);

            assertThat(tokenB).isGreaterThan(tokenA);
            assertThat(leader.execute(Command.lockRelease("fence-lock", tokenA), 2_000L).isOk()).isFalse();
            assertThat(leader.getStateMachine().getLock().get("fence-lock").owner()).isEqualTo("worker:B");
        } finally {
            cluster.shutdown();
        }
    }

    @Test
    @DisplayName("신규 리더의 시계가 60초 앞서도 유효한 락·작업 lease가 조기 만료되지 않는다 (노드 간 시계 동기화 비의존)")
    void leaseSurvivesLeaderChangeWithClockSkew(@TempDir Path dir) throws Exception {
        VirtualCluster cluster = new VirtualCluster(3, dir);
        cluster.start();
        try {
            RaftNode leader = awaitLeader(cluster);
            String oldLeaderId = leader.getNodeId();
            cluster.getNodeIds().stream().filter(id -> !id.equals(oldLeaderId))
                    .forEach(id -> cluster.setClockOffset(id, 60_000L));

            long fenceToken = leader.execute(Command.lockAcquire("skew-lock", "worker:A", 10_000L, 0L), 2_000L).getToken();
            assertThat(cluster.getQueueService(oldLeaderId).offer("JOB-SKEW")).isTrue();
            CommandResult lease = leader.execute(Command.dequeue("worker-x", 30_000L), 2_000L);
            assertThat(lease.getValue()).isEqualTo("JOB-SKEW");

            cluster.crash(oldLeaderId);
            RaftNode newLeader = await().atMost(5, TimeUnit.SECONDS).until(cluster::getLeaderNode,
                    l -> l != null && !l.getNodeId().equals(oldLeaderId));
            Thread.sleep(1_500L); // RecoveryCoordinator 스캔(500ms 주기) 수행 대기

            assertThat(cluster.getRecoveryActions()).isEmpty(); // lease 만료에 의한 회수·재적재 없음
            assertThat(newLeader.execute(Command.lockAcquire("skew-lock", "worker:B", 10_000L, 0L), 2_000L).isOk()).isFalse();
            assertThat(newLeader.getStateMachine().getLock().get("skew-lock").fenceToken()).isEqualTo(fenceToken);
            assertThat(newLeader.execute(Command.lockRenew("skew-lock", fenceToken, 10_000L), 2_000L).isOk()).isTrue();
            assertThat(newLeader.getStateMachine().getQueue().getInFlight("JOB-SKEW").leaseToken()).isEqualTo(lease.getToken());
        } finally {
            cluster.shutdown();
        }
    }

    @Test
    @DisplayName("재기동한 구 리더의 PreVote는 리더 생존 근거가 아니므로 다른 후보의 PreVote가 승인된다 (Pod 재생성 후 선출 교착 회귀)")
    void preVoteFromRestartedLeaderIsNotLeaderContact(@TempDir Path dir) throws Exception {
        List<Message> sent = new CopyOnWriteArrayList<>();
        NetworkTransport capture = new NetworkTransport() {
            @Override
            public void send(Message message) {
                sent.add(message);
            }

            @Override
            public void registerReceiver(String nodeId, Consumer<Message> receiver) {
            }

            @Override
            public void unregisterReceiver(String nodeId) {
            }
        };
        RaftTimings timings = new RaftTimings(100L, 300L, 300L, 2_000L, 2_000L, 16);
        RaftLogManager raftLog = new RaftLogManager(dir.resolve("node-2"), false);
        RaftNode follower = new RaftNode("node-2", List.of("node-1", "node-2", "node-3"), capture, raftLog,
                new ClusterStateMachine(), timings);
        follower.start();
        try {
            // node-1이 term 1 리더로 Heartbeat 전송
            follower.handleMessage(Message.appendEntries("node-1", "node-2", 1L, 0L, 0L, List.of(), 0L));
            assertThat(follower.getLeaderId()).isEqualTo("node-1");
            Thread.sleep(400L); // 최소 Election Timeout(300ms) 동안 리더 Heartbeat 없음

            // node-1 Pod가 재생성되어 Follower로 기동한 뒤 PreVote 전송 → 리더 생존 근거가 아님
            follower.handleMessage(Message.preVote("node-1", "node-2", 2L, 0L, 0L));
            follower.handleMessage(Message.preVote("node-3", "node-2", 2L, 0L, 0L));

            assertThat(sent)
                    .filteredOn(m -> m.getType() == Message.Type.VOTE_RESPONSE && m.isPreVote() && "node-3".equals(m.getReceiverId()))
                    .singleElement()
                    .extracting(Message::isSuccess)
                    .isEqualTo(true);
        } finally {
            follower.stop();
            raftLog.close();
        }
    }

    @Test
    @DisplayName("신규 리더는 선출 직후 통신 이력이 오래된 팔로워를 DEAD로 판정하지 않는다 (롤링 업데이트 중 멤버십 오탐 회귀)")
    void newLeaderDoesNotMarkQuietFollowerDeadRightAfterElection(@TempDir Path dir) throws Exception {
        CapturingTransport transport = new CapturingTransport();
        RaftTimings timings = new RaftTimings(100L, 300L, 300L, 1_000L, 2_000L, 16);
        RaftLogManager raftLog = new RaftLogManager(dir.resolve("node-1"), false);
        RaftNode node = new RaftNode("node-1", List.of("node-1", "node-2", "node-3"), transport, raftLog,
                new ClusterStateMachine(), timings);
        node.start();
        try {
            Thread.sleep(1_500L); // 팔로워 간에는 평소 통신하지 않으므로 node-3의 마지막 수신이 판정 시간(1초)보다 오래됨
            electWithGrantFrom(node, transport, "node-2");
            long readyAt = System.currentTimeMillis();
            // node-2는 계속 응답 (Check-Quorum 유지), node-3은 무응답
            ScheduledExecutorService node2 = Executors.newSingleThreadScheduledExecutor();
            node2.scheduleAtFixedRate(() -> node.handleMessage(Message.appendResponse("node-2", "node-1",
                    node.getCurrentTerm(), true, raftLog.lastIndex(), 0L)), 0L, 100L, TimeUnit.MILLISECONDS);
            try {
                Thread.sleep(500L);
                assertThat(memberStatusEntries(raftLog)).as("MEMBER_STATUS proposals right after election").isEmpty();

                // 리더 전환 시점부터 판정 시간(1초)이 지나도 응답이 없으면 DEAD로 제안 (장애 감지 유지)
                await().atMost(3, TimeUnit.SECONDS).pollInterval(20, TimeUnit.MILLISECONDS).until(() ->
                        memberStatusEntries(raftLog).stream().anyMatch(e -> "node-3".equals(e.getCommand().attr(Command.NODE))));
                assertThat(System.currentTimeMillis() - readyAt).isGreaterThanOrEqualTo(900L);
                assertThat(memberStatusEntries(raftLog)).noneMatch(e -> "node-2".equals(e.getCommand().attr(Command.NODE)));
            } finally {
                node2.shutdownNow();
            }
        } finally {
            node.stop();
            raftLog.close();
        }
    }

    @Test
    @DisplayName("DEAD로 기록된 노드가 리더가 되면 자신의 ALIVE를 커밋하여 멤버십을 복구한다")
    void leaderMarkedDeadRestoresOwnMembership(@TempDir Path dir) throws Exception {
        CapturingTransport transport = new CapturingTransport();
        RaftTimings timings = new RaftTimings(100L, 300L, 300L, 2_000L, 2_000L, 16);
        RaftLogManager raftLog = new RaftLogManager(dir.resolve("node-1"), false);
        RaftNode node = new RaftNode("node-1", List.of("node-1", "node-2", "node-3"), transport, raftLog,
                new ClusterStateMachine(), timings);
        node.start();
        try {
            // 이전 리더 node-2가 node-1 재기동 중 커밋한 MEMBER_STATUS(DEAD) 복제
            long now = System.currentTimeMillis();
            node.handleMessage(Message.appendEntries("node-2", "node-1", 1L, 0L, 0L, List.of(
                    new LogEntry(1L, 1L, now, Command.noOp()),
                    new LogEntry(2L, 1L, now, Command.memberStatus("node-1", false))), 2L));
            await().atMost(2, TimeUnit.SECONDS).until(() -> !node.getActiveMembers().contains("node-1"));

            electWithGrantFrom(node, transport, "node-2");
            await().atMost(2, TimeUnit.SECONDS).pollInterval(20, TimeUnit.MILLISECONDS).until(() ->
                    memberStatusEntries(raftLog).stream().anyMatch(e -> e.getIndex() > 2L
                            && "node-1".equals(e.getCommand().attr(Command.NODE)) && e.getCommand().boolAttr(Command.ALIVE)));

            node.handleMessage(Message.appendResponse("node-2", "node-1", node.getCurrentTerm(), true, raftLog.lastIndex(), 0L));
            await().atMost(2, TimeUnit.SECONDS).until(() -> node.getActiveMembers().contains("node-1"));
        } finally {
            node.stop();
            raftLog.close();
        }
    }

    @Test
    @DisplayName("DEAD로 기록된 리더의 RecoveryCoordinator는 자신이 추적 중인 작업을 고립 작업으로 회수하지 않는다")
    void recoveryDoesNotTreatLeaderItselfAsDead(@TempDir Path dir) throws Exception {
        CapturingTransport transport = new CapturingTransport();
        RaftTimings timings = new RaftTimings(100L, 300L, 300L, 2_000L, 2_000L, 16);
        RaftLogManager raftLog = new RaftLogManager(dir.resolve("node-1"), false);
        RaftNode node = new RaftNode("node-1", List.of("node-1", "node-2", "node-3"), transport, raftLog,
                new ClusterStateMachine(), timings);
        RecoveryCoordinator recovery = new RecoveryCoordinator(node, (executionId, jobId) -> ExecutionStatus.NOT_FOUND,
                60_000L, 60_000L);
        node.start();
        recovery.start();
        try {
            // node-1이 JOB-SELF를 인출해 추적 중인 상태에서 MEMBER_STATUS(DEAD)가 커밋됨
            long now = System.currentTimeMillis();
            node.handleMessage(Message.appendEntries("node-2", "node-1", 1L, 0L, 0L, List.of(
                    new LogEntry(1L, 1L, now, Command.noOp()),
                    new LogEntry(2L, 1L, now, Command.enqueue("JOB-SELF", false)),
                    new LogEntry(3L, 1L, now, Command.dequeue("node-1", 60_000L)),
                    new LogEntry(4L, 1L, now, Command.memberStatus("node-1", false))), 4L));
            await().atMost(2, TimeUnit.SECONDS).until(() -> node.getStateMachine().getQueue().getInFlight("JOB-SELF") != null
                    && !node.getActiveMembers().contains("node-1"));

            electWithGrantFrom(node, transport, "node-2");
            recovery.scan();
            Thread.sleep(500L);

            assertThat(entriesAfter(raftLog, 4L)).noneMatch(e -> e.getType() == CommandType.QUEUE_REQUEUE
                    || e.getType() == CommandType.QUEUE_REASSIGN || e.getType() == CommandType.JOURNAL_RECORD);
            assertThat(recovery.getActions()).isEmpty();
            assertThat(node.getStateMachine().getQueue().getInFlight("JOB-SELF").worker()).isEqualTo("node-1");
        } finally {
            recovery.stop();
            node.stop();
            raftLog.close();
        }
    }

    @Test
    @DisplayName("Split Vote로 과반을 얻지 못하면 Election Timeout을 기다리지 않고 150~300ms 난수 대기 후 재선출한다 (QAS-05 최악 조건)")
    void splitVoteRetriesAfterShortRandomBackoff(@TempDir Path dir) throws Exception {
        CapturingTransport transport = new CapturingTransport();
        RaftTimings timings = new RaftTimings(100L, 1_500L, 2_000L, 2_000L, 2_000L, 16);
        RaftLogManager raftLog = new RaftLogManager(dir.resolve("node-1"), false);
        RaftNode node = new RaftNode("node-1", List.of("node-1", "node-2", "node-3"), transport, raftLog,
                new ClusterStateMachine(), timings);
        node.start();
        try {
            becomeCandidate(node, transport, "node-2");
            // 리더 장애 후 생존 2노드가 동시에 출마: node-2는 같은 term에 자신에게 투표하여 거절, node-3은 장애로 무응답
            long rejectedAt = System.currentTimeMillis();
            node.handleMessage(Message.voteResponse("node-2", "node-1", 1L, false, false));

            await().atMost(1_500L, TimeUnit.MILLISECONDS).pollInterval(10, TimeUnit.MILLISECONDS).until(() -> transport.sent.stream()
                    .anyMatch(m -> m.getType() == Message.Type.PRE_VOTE && m.getTerm() == 2L));
            long retryElapsed = System.currentTimeMillis() - rejectedAt;
            assertThat(retryElapsed).as("retry before Election Timeout (1,500ms)").isLessThan(1_000L);
        } finally {
            node.stop();
            raftLog.close();
        }
    }

    @Test
    @DisplayName("거절 없이 투표 응답만 지연·유실되면 Election Timeout을 유지하여 선출을 반복하지 않는다 (고지연 환경)")
    void candidateWithoutRejectionKeepsElectionTimeout(@TempDir Path dir) throws Exception {
        CapturingTransport transport = new CapturingTransport();
        RaftTimings timings = new RaftTimings(100L, 1_500L, 2_000L, 2_000L, 2_000L, 16);
        RaftLogManager raftLog = new RaftLogManager(dir.resolve("node-1"), false);
        RaftNode node = new RaftNode("node-1", List.of("node-1", "node-2", "node-3"), transport, raftLog,
                new ClusterStateMachine(), timings);
        node.start();
        try {
            becomeCandidate(node, transport, "node-2");
            Thread.sleep(1_000L);

            assertThat(transport.sent).noneMatch(m -> m.getType() == Message.Type.PRE_VOTE && m.getTerm() == 2L);
            assertThat(node.getCurrentTerm()).isEqualTo(1L);
        } finally {
            node.stop();
            raftLog.close();
        }
    }

    // ─── 헬퍼 ─────────────────────────────────────────────────────────────────
    /** voter의 PreVote 승인을 주입하여 node를 term 1 후보(RequestVote 송신 완료)로 만듭니다. */
    private static void becomeCandidate(RaftNode node, CapturingTransport transport, String voter) {
        await().atMost(3, TimeUnit.SECONDS).pollInterval(10, TimeUnit.MILLISECONDS).until(() -> transport.sent.stream()
                .anyMatch(m -> m.getType() == Message.Type.PRE_VOTE && m.getTerm() == 1L));
        node.handleMessage(Message.voteResponse(voter, node.getNodeId(), 1L, true, true));
        await().atMost(1, TimeUnit.SECONDS).pollInterval(10, TimeUnit.MILLISECONDS).until(() ->
                node.getCurrentTerm() == 1L && node.getRole() == RaftRole.CANDIDATE);
    }

    /** 송신 메시지를 기록하는 전송 계층 (RaftNode 단독 시험용). */
    private static final class CapturingTransport implements NetworkTransport {
        private final List<Message> sent = new CopyOnWriteArrayList<>();

        @Override
        public void send(Message message) {
            sent.add(message);
        }

        @Override
        public void registerReceiver(String nodeId, Consumer<Message> receiver) {
        }

        @Override
        public void unregisterReceiver(String nodeId) {
        }
    }

    /** voter의 PreVote·RequestVote 승인과 NO_OP 복제 응답을 주입하여 node를 서비스 가능한 리더로 만듭니다. */
    private static void electWithGrantFrom(RaftNode node, CapturingTransport transport, String voter) {
        long term = node.getCurrentTerm() + 1;
        await().atMost(3, TimeUnit.SECONDS).pollInterval(10, TimeUnit.MILLISECONDS).until(() -> transport.sent.stream()
                .anyMatch(m -> m.getType() == Message.Type.PRE_VOTE && m.getTerm() == term));
        node.handleMessage(Message.voteResponse(voter, node.getNodeId(), term, true, true));
        await().atMost(3, TimeUnit.SECONDS).pollInterval(10, TimeUnit.MILLISECONDS).until(() -> node.getCurrentTerm() == term);
        node.handleMessage(Message.voteResponse(voter, node.getNodeId(), term, true, false));
        await().atMost(3, TimeUnit.SECONDS).pollInterval(10, TimeUnit.MILLISECONDS).until(node::isLeader);
        node.handleMessage(Message.appendResponse(voter, node.getNodeId(), term, true, node.getRaftLog().lastIndex(), 0L));
        await().atMost(3, TimeUnit.SECONDS).pollInterval(10, TimeUnit.MILLISECONDS).until(node::isLeaderReady);
    }

    private static List<LogEntry> memberStatusEntries(RaftLogManager raftLog) {
        return entriesAfter(raftLog, 0L).stream().filter(e -> e.getType() == CommandType.MEMBER_STATUS).toList();
    }

    private static List<LogEntry> entriesAfter(RaftLogManager raftLog, long index) {
        long last = raftLog.lastIndex();
        return last <= index ? List.of() : raftLog.slice(index + 1, last, Integer.MAX_VALUE);
    }
}
