package com.sds.batchservice.cluster;

import com.sds.batchservice.cluster.api.DistributedLockExecutor;
import com.sds.batchservice.cluster.api.DistributedQueueService;
import com.sds.batchservice.cluster.api.QueueLease;
import com.sds.batchservice.cluster.api.TaskJournalService;
import com.sds.batchservice.cluster.bootstrap.StandaloneBootstrapEngine;
import com.sds.batchservice.cluster.consensus.ClusterUnavailableException;
import com.sds.batchservice.cluster.consensus.RaftNode;
import com.sds.batchservice.cluster.consensus.RaftTimings;
import com.sds.batchservice.cluster.fsm.Command;
import com.sds.batchservice.cluster.fsm.JournalStateMachine.TaskStatus;
import com.sds.batchservice.cluster.mock.DagStateManager;
import com.sds.batchservice.cluster.mock.MockDatabaseProxy;
import com.sds.batchservice.cluster.mock.VirtualCluster;
import com.sds.batchservice.cluster.recovery.ExecutionStatusProvider.ExecutionStatus;
import com.sds.batchservice.cluster.recovery.RecoveryCoordinator.ActionKind;
import com.sds.batchservice.cluster.recovery.RecoveryCoordinator.RecoveryAction;
import com.sds.batchservice.cluster.sync.WriteBehindSynchronizer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * 7.2 Test Cases (TC-QAS-01 ~ 09) — 3개 가상 노드, 노드별 독립 WAL/FSM, 실제 선출 기반.
 */
public class ArchitectureVerificationTest {
    private static final Logger log = LoggerFactory.getLogger(ArchitectureVerificationTest.class);

    @Test
    @DisplayName("TC-QAS-01: 동시 100개 트래픽 상황에서 DAG 워크플로우 원자적 오케스트레이션 검증")
    void testQAS01_DagConcurrencyConsistency(@TempDir Path dir) throws Exception {
        VirtualCluster cluster = new VirtualCluster(3, dir);
        cluster.start();
        try {
            awaitLeader(cluster);
            List<DistributedLockExecutor> executors = cluster.getNodeIds().stream().map(cluster::getLockExecutor).toList();
            DagStateManager dagStateManager = new DagStateManager();

            int threadCount = 100;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch readyLatch = new CountDownLatch(threadCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            AtomicInteger executionCount = new AtomicInteger();
            AtomicInteger acquiredCount = new AtomicInteger();
            List<Long> criticalSectionLatencies = new CopyOnWriteArrayList<>();

            for (int i = 0; i < threadCount; i++) {
                final int requestId = i;
                final DistributedLockExecutor lockExecutor = executors.get(i % executors.size()); // 3개 노드에 분산 요청
                executor.submit(() -> {
                    readyLatch.countDown();
                    try {
                        startLatch.await();
                        lockExecutor.execute("dag-job-workflow-001", "req-" + requestId, () -> {
                            long csStart = System.nanoTime();
                            acquiredCount.incrementAndGet();
                            if (dagStateManager.transitionToRunning("task-B")) {
                                executionCount.incrementAndGet();
                            }
                            criticalSectionLatencies.add((System.nanoTime() - csStart) / 1_000_000L);
                            return null;
                        });
                    } catch (Exception e) {
                        log.warn("request {} failed: {}", requestId, e.toString());
                    }
                });
            }
            readyLatch.await();
            long startTime = System.currentTimeMillis();
            startLatch.countDown();
            executor.shutdown();
            boolean finished = executor.awaitTermination(5, TimeUnit.SECONDS);
            long totalElapsed = System.currentTimeMillis() - startTime;
            double avgLatency = criticalSectionLatencies.stream().mapToLong(Long::longValue).average().orElse(0.0);
            log.info("TC-QAS-01: {} requests, acquired={}, triggers={}, total={}ms, avg critical-section={}ms",
                    threadCount, acquiredCount.get(), executionCount.get(), totalElapsed, String.format("%.2f", avgLatency));

            assertThat(finished).isTrue();
            assertThat(acquiredCount.get()).isEqualTo(threadCount); // 상호 배제 하에서 전원 순차 획득 (기아 없음)
            assertThat(executionCount.get()).isEqualTo(1);           // 후행 태스크 발화는 오직 1회
            assertThat(avgLatency).isLessThanOrEqualTo(10.0);
            assertThat(totalElapsed).isLessThanOrEqualTo(1000L);
            await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> cluster.runningNodes().forEach(n ->
                    assertThat(n.getStateMachine().getLock().get("dag-job-workflow-001")).isNull()));
        } finally {
            cluster.shutdown();
        }
    }

    @Test
    @DisplayName("TC-QAS-02: 피크 타임 대규모 분산 큐 작업 인출 응답 시간 보장 검증")
    void testQAS02_DistributedQueuePerformance(@TempDir Path dir) throws Exception {
        VirtualCluster cluster = new VirtualCluster(3, dir);
        cluster.start();
        try {
            RaftNode leader = awaitLeader(cluster);
            int taskCount = 200;
            int workerCount = 12;
            DistributedQueueService leaderQueue = cluster.getQueueService(leader.getNodeId());
            for (int i = 1; i <= taskCount; i++) {
                assertThat(leaderQueue.offer("TASK-BATCH-" + i)).isTrue();
            }
            await().atMost(3, TimeUnit.SECONDS).until(() ->
                    cluster.runningNodes().stream().allMatch(n -> n.getStateMachine().getQueue().size() == taskCount));

            List<DistributedQueueService> queues = cluster.getNodeIds().stream().map(cluster::getQueueService).toList();
            Set<String> dequeuedTasks = ConcurrentHashMap.newKeySet();
            List<Long> fetchLatencies = new CopyOnWriteArrayList<>();
            ExecutorService workers = Executors.newFixedThreadPool(workerCount);
            CountDownLatch completeLatch = new CountDownLatch(taskCount);

            long startAll = System.currentTimeMillis();
            for (int w = 0; w < workerCount; w++) {
                final DistributedQueueService queue = queues.get(w % queues.size()); // 노드당 4개 워커
                workers.submit(() -> {
                    while (!Thread.currentThread().isInterrupted() && completeLatch.getCount() > 0) {
                        long fetchStart = System.currentTimeMillis();
                        try {
                            String task = queue.poll(50, TimeUnit.MILLISECONDS);
                            if (task != null) {
                                fetchLatencies.add(System.currentTimeMillis() - fetchStart);
                                dequeuedTasks.add(task);
                                completeLatch.countDown();
                            }
                        } catch (InterruptedException e) {
                            return;
                        } catch (RuntimeException e) {
                            log.warn("poll failed: {}", e.toString());
                        }
                    }
                });
            }
            boolean allDequeued = completeLatch.await(1, TimeUnit.SECONDS);
            long totalElapsed = System.currentTimeMillis() - startAll;
            workers.shutdownNow();

            double avg = fetchLatencies.stream().mapToLong(Long::longValue).average().orElse(0.0);
            long max = fetchLatencies.stream().mapToLong(Long::longValue).max().orElse(0L);
            log.info("TC-QAS-02: dequeued={} in {}ms, avg={}ms, max={}ms", dequeuedTasks.size(), totalElapsed,
                    String.format("%.2f", avg), max);

            assertThat(allDequeued).isTrue();
            assertThat(dequeuedTasks).hasSize(taskCount);
            assertThat(fetchLatencies).hasSize(taskCount); // 중복 인출 0건
            assertThat(avg).isLessThanOrEqualTo(20.0);
            assertThat(max).isLessThanOrEqualTo(50L);
            assertThat(totalElapsed).isLessThanOrEqualTo(500L);
        } finally {
            cluster.shutdown();
        }
    }

    @Test
    @DisplayName("TC-QAS-03: 스플릿 브레인 상황 시 데이터 정합성 및 단일 리더 보장 검증")
    void testQAS03_SplitBrainSingleLeader(@TempDir Path dir) throws Exception {
        VirtualCluster cluster = new VirtualCluster(3, dir);
        cluster.start();
        try {
            RaftNode oldLeader = awaitLeader(cluster);
            String oldLeaderId = oldLeader.getNodeId();
            cluster.getQueueService(oldLeaderId).offer("JOB-PARTITION");
            await().atMost(2, TimeUnit.SECONDS).until(() ->
                    cluster.runningNodes().stream().allMatch(n -> n.getStateMachine().getQueue().size() == 1));

            // 리더를 소수파로 격리 (Partition A {old leader} / Partition B {나머지 2대})
            cluster.getNetworkRouter().isolateNode(oldLeaderId);
            assertThatThrownBy(() -> cluster.getQueueService(oldLeaderId).pollLease())
                    .isInstanceOf(ClusterUnavailableException.class); // 소수파 인출 불가

            RaftNode newLeader = await().atMost(5, TimeUnit.SECONDS).until(cluster::getLeaderNode,
                    l -> l != null && !l.getNodeId().equals(oldLeaderId));
            int totalActiveLeaders = cluster.getActiveLeaderCount();
            boolean oldLeaderStillLeader = oldLeader.isLeader();
            log.info("TC-QAS-03: new leader={} (term {}), active leaders={}, old leader still leader={}",
                    newLeader.getNodeId(), newLeader.getCurrentTerm(), totalActiveLeaders, oldLeaderStillLeader);

            assertThat(oldLeaderStillLeader).isFalse();          // Check-Quorum 강등
            assertThat(totalActiveLeaders).isLessThanOrEqualTo(1);
            assertThat(newLeader.getStateMachine().getQueue().waitingSnapshot()).containsExactly("JOB-PARTITION");

            // 다수파 정상 쓰기 후 네트워크 복구 → 재수렴
            assertThat(cluster.getQueueService(newLeader.getNodeId()).offer("JOB-301")).isTrue();
            cluster.getNetworkRouter().reconnectNode(oldLeaderId);
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(cluster.getActiveLeaderCount()).isEqualTo(1);
                assertLogsConverged(cluster);
            });
            assertThat(oldLeader.getStateMachine().getQueue().waitingSnapshot()).containsExactly("JOB-PARTITION", "JOB-301");
        } finally {
            cluster.shutdown();
        }
    }

    @Test
    @DisplayName("TC-QAS-04: 일반 멤버 노드 장애 감지 및 3초 이내 멤버십 자가 치유 검증")
    void testQAS04_FollowerFailureSelfHealing(@TempDir Path dir) throws Exception {
        VirtualCluster cluster = new VirtualCluster(3, dir);
        cluster.start();
        try {
            RaftNode leader = awaitLeader(cluster);
            List<String> followers = cluster.getNodeIds().stream().filter(id -> !id.equals(leader.getNodeId())).toList();
            String crashed = followers.get(0);
            String healthy = followers.get(1);
            assertThat(leader.getActiveMembers()).hasSize(3);

            long t0 = System.currentTimeMillis();
            cluster.crash(crashed);
            await().atMost(3, TimeUnit.SECONDS).until(() -> !leader.getActiveMembers().contains(crashed));
            long totalHealTime = System.currentTimeMillis() - t0;
            log.info("TC-QAS-04: [{}] removed from membership in {}ms", crashed, totalHealTime);

            assertThat(totalHealTime).isLessThanOrEqualTo(3000L);
            assertThat(leader.getActiveMembers()).contains(leader.getNodeId(), healthy); // 정상 노드 오탐 0건
            await().atMost(1, TimeUnit.SECONDS).until(() ->
                    !cluster.getNode(healthy).getActiveMembers().contains(crashed)); // 잔여 노드 동기화
        } finally {
            cluster.shutdown();
        }
    }

    @Test
    @DisplayName("TC-QAS-05: 리더 노드 장애 시 3초 이내 신규 리더 자동 선출 및 정상화 검증")
    void testQAS05_LeaderFailoverTime(@TempDir Path dir) throws Exception {
        VirtualCluster cluster = new VirtualCluster(3, dir);
        cluster.start();
        try {
            String originalLeader = awaitLeader(cluster).getNodeId();
            long t0 = System.currentTimeMillis();
            cluster.crash(originalLeader);

            RaftNode newLeader = await().atMost(3, TimeUnit.SECONDS).until(cluster::getLeaderNode,
                    l -> l != null && !l.getNodeId().equals(originalLeader));
            long totalFailoverTime = System.currentTimeMillis() - t0;
            log.info("TC-QAS-05: new leader [{}] ready (NO_OP committed) in {}ms", newLeader.getNodeId(), totalFailoverTime);

            assertThat(totalFailoverTime).isLessThanOrEqualTo(3000L);
            assertThat(cluster.getQueueService(newLeader.getNodeId()).offer("JOB-AFTER-FAILOVER")).isTrue(); // 서비스 정상화
        } finally {
            cluster.shutdown();
        }
    }

    @Test
    @DisplayName("TC-QAS-06: 예기치 못한 다운 시 상태 복원을 통한 데이터 무유실 복구 검증")
    void testQAS06_JournalStateRecovery(@TempDir Path dir) throws Exception {
        Map<String, ExecutionStatus> targetStatus = Map.of(
                "exec-1", ExecutionStatus.NOT_FOUND, "exec-2", ExecutionStatus.FAILED, "exec-3", ExecutionStatus.NOT_FOUND,
                "exec-4", ExecutionStatus.RUNNING, "exec-5", ExecutionStatus.COMPLETED);
        VirtualCluster cluster = new VirtualCluster(3, dir, new MockDatabaseProxy(),
                (executionId, jobId) -> targetStatus.getOrDefault(executionId, ExecutionStatus.UNKNOWN), RaftTimings.defaults());
        cluster.start();
        try {
            RaftNode leader = awaitLeader(cluster);
            String leaderId = leader.getNodeId();
            String followerId = cluster.getNodeIds().stream().filter(id -> !id.equals(leaderId)).findFirst().orElseThrow();
            DistributedQueueService leaderQueue = cluster.getQueueService(leaderId);
            TaskJournalService leaderJournal = cluster.getJournalService(leaderId);

            // 10개 태스크: 리더 노드가 추적 — 1~5 RUNNING, 6~10 COMPLETED
            long exec4Token = 0;
            for (int i = 1; i <= 10; i++) {
                leaderQueue.offer("JOB-" + i);
                QueueLease lease = leaderQueue.pollLease();
                if (i <= 5) {
                    leaderJournal.record("exec-" + i, lease.jobId(), TaskStatus.RUNNING, lease.leaseToken());
                    if (i == 4) exec4Token = lease.leaseToken();
                } else {
                    leaderJournal.record("exec-" + i, lease.jobId(), TaskStatus.COMPLETED, lease.leaseToken());
                    leaderQueue.ack(lease);
                }
            }
            // 생존 팔로워가 추적 중인 작업 (복구 대상 아님)
            DistributedQueueService followerQueue = cluster.getQueueService(followerId);
            followerQueue.offer("JOB-11");
            QueueLease lease11 = followerQueue.pollLease();
            cluster.getJournalService(followerId).record("exec-11", "JOB-11", TaskStatus.RUNNING, lease11.leaseToken());

            long t0 = System.currentTimeMillis();
            cluster.crash(leaderId);
            await().atMost(10, TimeUnit.SECONDS).until(() -> cluster.getRecoveryActions().stream()
                    .filter(a -> a.kind() != ActionKind.LOCK_RECLAIMED).count() == 5);
            long recoveryElapsed = System.currentTimeMillis() - t0;
            List<RecoveryAction> actions = cluster.getRecoveryActions();
            log.info("TC-QAS-06: recovered in {}ms, actions={}", recoveryElapsed, actions);

            RaftNode newLeader = cluster.getLeaderNode();
            var queue = newLeader.getStateMachine().getQueue();
            var journal = newLeader.getStateMachine().getJournal();
            assertThat(recoveryElapsed).isLessThanOrEqualTo(10_000L);
            assertThat(actions).noneMatch(a -> "JOB-11".equals(a.target()));                  // 생존 노드 작업 미조치
            assertThat(queue.waitingSnapshot()).containsExactlyInAnyOrder("JOB-1", "JOB-2", "JOB-3"); // 재적재
            assertThat(queue.getInFlight("JOB-4").worker()).isEqualTo(newLeader.getNodeId());  // 재실행 없이 인수
            assertThat(journal.get("exec-5").status()).isEqualTo(TaskStatus.COMPLETED);
            assertThat(queue.getInFlight("JOB-5")).isNull();
            assertThat(queue.getInFlight("JOB-11").worker()).isEqualTo(followerId);
            // 유실 0건: 11개 작업 모두 대기열/in-flight/완료 중 하나로 추적됨
            for (int i = 1; i <= 11; i++) {
                String job = "JOB-" + i;
                boolean tracked = queue.waitingSnapshot().contains(job) || queue.getInFlight(job) != null
                        || journal.latestForJob(job).status() == TaskStatus.COMPLETED;
                assertThat(tracked).as(job + " tracked").isTrue();
            }
            // 펜싱: 구 추적자의 뒤늦은 보고(구 leaseToken)는 거부
            assertThat(cluster.getJournalService(newLeader.getNodeId())
                    .record("exec-4", "JOB-4", TaskStatus.COMPLETED, exec4Token)).isFalse();
        } finally {
            cluster.shutdown();
        }
    }

    @Test
    @DisplayName("TC-QAS-07: 클러스터 노드당 자원(메모리/CPU) 점유율 최적화 검증")
    void testQAS07_ResourceFootprintOptimization(@TempDir Path dir) throws Exception {
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        System.gc();
        Thread.sleep(500);
        long initialHeapUsed = memoryBean.getHeapMemoryUsage().getUsed();
        long gcTimeBefore = totalGcTime();

        VirtualCluster cluster = new VirtualCluster(1, dir);
        cluster.start();
        try {
            RaftNode node = awaitLeader(cluster);
            DistributedQueueService queue = cluster.getQueueService(node.getNodeId());
            for (int i = 0; i < 1_000; i++) {
                queue.offer("JOB-" + i);
            }
            Thread.sleep(5000); // 백그라운드 합의 루프 가동

            System.gc();
            Thread.sleep(500);
            long additionalHeapMB = (memoryBean.getHeapMemoryUsage().getUsed() - initialHeapUsed) / (1024 * 1024);
            long gcPause = totalGcTime() - gcTimeBefore;
            log.info("TC-QAS-07: additional heap={}MB, GC time={}ms", additionalHeapMB, gcPause);

            assertThat(additionalHeapMB).isLessThanOrEqualTo(50L);
            assertThat(gcPause).isLessThanOrEqualTo(1000L);
        } finally {
            cluster.shutdown();
        }
    }

    @Test
    @DisplayName("TC-QAS-08: 외부 미들웨어 종속 없는 단독 부트스트랩 및 자동 구성 검증")
    void testQAS08_MiddlewareFreeBootstrap(@TempDir Path dir) throws Exception {
        long startBootstrap = System.currentTimeMillis();
        StandaloneBootstrapEngine engine = new StandaloneBootstrapEngine(3, dir);
        engine.bootstrap();
        try {
            await().atMost(2, TimeUnit.MINUTES).until(engine::isClusterReady);
            long totalBootstrapTime = System.currentTimeMillis() - startBootstrap;

            // 실제 TCP 소켓 경로로 복제·커밋 확인
            RaftNode leader = engine.getLeader();
            assertThat(leader.execute(Command.enqueue("JOB-SOCKET", false), 5_000L).isOk()).isTrue();
            await().atMost(3, TimeUnit.SECONDS).until(() ->
                    engine.getNodes().stream().allMatch(n -> n.getStateMachine().getQueue().size() == 1));
            log.info("TC-QAS-08: 3-node socket cluster ready in {}ms", totalBootstrapTime);

            assertThat(engine.getRequiredExternalProcessCount()).isZero();
            assertThat(totalBootstrapTime).isLessThanOrEqualTo(120_000L);
        } finally {
            engine.stop();
        }
    }

    @Test
    @DisplayName("TC-QAS-09: 메타 DB 장기 장애 중 분산 락·큐 무중단 운영 및 DB 복구 후 정합성 복원 검증")
    void testQAS09_DatabaseOutageResilience(@TempDir Path dir) throws Exception {
        MockDatabaseProxy dbProxy = new MockDatabaseProxy();
        VirtualCluster cluster = new VirtualCluster(3, dir, dbProxy, (e, j) -> ExecutionStatus.UNKNOWN, RaftTimings.defaults());
        cluster.start();
        try {
            awaitLeader(cluster);
            await().atMost(3, TimeUnit.SECONDS).until(() -> cluster.getWriteBehindSynchronizer() != null
                    && cluster.getWriteBehindSynchronizer().isFullySynchronized());

            dbProxy.disconnect();
            int taskCount = 50;
            AtomicInteger successfulOperations = new AtomicInteger();
            List<String> ids = cluster.getNodeIds();
            for (int i = 0; i < taskCount; i++) {
                final int opId = i;
                String nodeId = ids.get(i % ids.size());
                DistributedQueueService queue = cluster.getQueueService(nodeId);
                cluster.getLockExecutor(nodeId).execute("db-down-lock", "op-" + opId, () -> {
                    queue.offer("TASK-OFFLINE-" + opId);
                    QueueLease lease = queue.pollLease();
                    if (lease != null && queue.ack(lease)) {
                        successfulOperations.incrementAndGet();
                    }
                    return null;
                });
            }
            WriteBehindSynchronizer sync = cluster.getWriteBehindSynchronizer();
            await().atMost(2, TimeUnit.SECONDS).until(() -> sync.getState() == WriteBehindSynchronizer.State.DB_DOWN);
            assertThat(successfulOperations.get()).isEqualTo(taskCount);          // 중단 시간 0초
            assertThat(cluster.runningNodes()).allMatch(RaftNode::isReady);        // Readiness 영향 없음
            assertThat(sync.getUnsyncedRecordCount()).isGreaterThanOrEqualTo(taskCount * 5L);

            long tDbRecovered = System.currentTimeMillis();
            dbProxy.reconnect();
            await().atMost(30, TimeUnit.SECONDS).until(sync::isFullySynchronized);
            long syncElapsed = System.currentTimeMillis() - tDbRecovered;
            log.info("TC-QAS-09: {} records synchronized in {}ms after DB recovery", dbProxy.countRecords(), syncElapsed);

            assertThat(syncElapsed).isLessThanOrEqualTo(30_000L);
            assertThat(sync.getUnsyncedRecordCount()).isZero();
            assertThat(dbProxy.countRecords()).isGreaterThanOrEqualTo(taskCount * 5L);
            assertThat(dbProxy.readCheckpoint("virtual-cluster")).isEqualTo(sync.getCheckpoint());
        } finally {
            cluster.shutdown();
        }
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────
    static RaftNode awaitLeader(VirtualCluster cluster) {
        return await().atMost(5, TimeUnit.SECONDS).until(cluster::getLeaderNode, java.util.Objects::nonNull);
    }

    static void assertLogsConverged(VirtualCluster cluster) {
        List<RaftNode> nodes = cluster.runningNodes();
        long commit = nodes.get(0).getCommitIndex();
        for (RaftNode n : nodes) {
            assertThat(n.getCommitIndex()).isEqualTo(commit);
            assertThat(n.getLastApplied()).isEqualTo(commit);
            for (long i = 1; i <= commit; i++) {
                assertThat(n.getRaftLog().termAt(i)).as(n.getNodeId() + " term@" + i)
                        .isEqualTo(nodes.get(0).getRaftLog().termAt(i));
            }
        }
    }

    private static long totalGcTime() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream()
                .mapToLong(GarbageCollectorMXBean::getCollectionTime).sum();
    }
}
