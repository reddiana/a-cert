package com.sds.batchservice.cluster;

import com.sds.batchservice.cluster.consensus.RaftNode;
import com.sds.batchservice.cluster.fsm.Command;
import com.sds.batchservice.cluster.fsm.CommandResult;
import com.sds.batchservice.cluster.mock.VirtualCluster;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
}
