package com.batchservice.cluster.mock;

import com.batchservice.cluster.api.DistributedLockExecutor;
import com.batchservice.cluster.api.DistributedQueueService;
import com.batchservice.cluster.api.TaskJournalService;
import com.batchservice.cluster.consensus.RaftNode;
import com.batchservice.cluster.consensus.RaftTimings;
import com.batchservice.cluster.fsm.ClusterStateMachine;
import com.batchservice.cluster.recovery.ExecutionStatusProvider;
import com.batchservice.cluster.recovery.RecoveryCoordinator;
import com.batchservice.cluster.storage.RaftLogManager;
import com.batchservice.cluster.storage.WriteBehindSynchronizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 단일 JVM 가상 클러스터 (7.1.3 VirtualClusterContext).
 *
 * <p>노드마다 <b>독립된</b> WAL 디렉터리, Multi-FSM, RaftNode, WriteBehindSynchronizer, RecoveryCoordinator를
 * 구성하여 실제 복제·합의를 검증합니다 (FSM 공유 없음). 초기 리더도 강제 지정하지 않고 선출로 결정합니다.
 */
public class VirtualCluster {
    private static final Logger log = LoggerFactory.getLogger(VirtualCluster.class);

    public static final class VirtualNode {
        final RaftLogManager raftLog;
        final RaftNode raft;
        final WriteBehindSynchronizer sync;
        final RecoveryCoordinator recovery;

        VirtualNode(RaftLogManager raftLog, RaftNode raft, WriteBehindSynchronizer sync, RecoveryCoordinator recovery) {
            this.raftLog = raftLog;
            this.raft = raft;
            this.sync = sync;
            this.recovery = recovery;
        }
    }

    private final Path baseDir;
    private final List<String> nodeIds = new ArrayList<>();
    private final Map<String, VirtualNode> nodes = new LinkedHashMap<>();
    private final Map<String, Long> clockOffsets = new ConcurrentHashMap<>();
    private final VirtualNetworkRouter networkRouter = new VirtualNetworkRouter();
    private final MockDatabaseProxy dbProxy;
    private final ExecutionStatusProvider statusProvider;
    private final RaftTimings timings;

    public VirtualCluster(int nodeCount, Path baseDir) {
        this(nodeCount, baseDir, new MockDatabaseProxy(), ExecutionStatusProvider.unknown(), RaftTimings.defaults());
    }

    public VirtualCluster(int nodeCount, Path baseDir, MockDatabaseProxy dbProxy,
                          ExecutionStatusProvider statusProvider, RaftTimings timings) {
        this.baseDir = baseDir;
        this.dbProxy = dbProxy;
        this.statusProvider = statusProvider;
        this.timings = timings;
        for (int i = 1; i <= nodeCount; i++) {
            nodeIds.add("node-" + i);
        }
    }

    public synchronized void start() {
        log.info("Starting VirtualCluster with nodes {}", nodeIds);
        for (String id : nodeIds) {
            startNode(id);
        }
    }

    public synchronized void restart(String nodeId) {
        if (nodes.containsKey(nodeId)) {
            throw new IllegalStateException(nodeId + " is still running");
        }
        log.info("Restarting {} from its WAL directory", nodeId);
        startNode(nodeId);
    }

    private void startNode(String id) {
        RaftLogManager raftLog = new RaftLogManager(baseDir.resolve(id), true);
        RaftNode raft = new RaftNode(id, nodeIds, networkRouter, raftLog, new ClusterStateMachine(), timings,
                () -> System.currentTimeMillis() + clockOffsets.getOrDefault(id, 0L));
        WriteBehindSynchronizer sync = new WriteBehindSynchronizer(raft, dbProxy, "virtual-cluster", 500, 200L);
        RecoveryCoordinator recovery = new RecoveryCoordinator(raft, statusProvider, 500L, 30_000L);
        raft.start();
        sync.start();
        recovery.start();
        nodes.put(id, new VirtualNode(raftLog, raft, sync, recovery));
    }

    /** 노드 시스템 시계 오차 주입 (리더가 기록하는 proposedAt 및 리더의 만료 판정 시계에 반영). */
    public void setClockOffset(String nodeId, long offsetMs) {
        clockOffsets.put(nodeId, offsetMs);
    }

    /** 프로세스 강제 종료 시뮬레이션: 영속화 없이 정지 (WAL 파일은 유지). */
    public synchronized void crash(String nodeId) {
        VirtualNode n = nodes.remove(nodeId);
        if (n == null) return;
        n.recovery.stop();
        n.sync.stop();
        n.raft.crash();
        n.raftLog.close();
    }

    public synchronized void shutdown() {
        for (VirtualNode n : nodes.values()) {
            n.recovery.stop();
            n.sync.stop();
            n.raft.stop();
            n.raftLog.close();
        }
        nodes.clear();
        networkRouter.shutdown();
        log.info("VirtualCluster shutdown complete.");
    }

    // ─── 조회 ─────────────────────────────────────────────────────────────────
    public synchronized List<RaftNode> runningNodes() {
        return nodes.values().stream().map(n -> n.raft).toList();
    }

    public synchronized RaftNode getNode(String nodeId) {
        VirtualNode n = nodes.get(nodeId);
        return n == null ? null : n.raft;
    }

    public int getActiveLeaderCount() {
        return (int) runningNodes().stream().filter(RaftNode::isLeader).count();
    }

    /** 서비스 가능한(NO_OP 커밋 완료) 리더. 여러 개면 가장 높은 term. */
    public RaftNode getLeaderNode() {
        return runningNodes().stream().filter(RaftNode::isLeaderReady)
                .max(Comparator.comparingLong(RaftNode::getCurrentTerm)).orElse(null);
    }

    public String getLeaderNodeId() {
        RaftNode leader = getLeaderNode();
        return leader == null ? null : leader.getNodeId();
    }

    public List<String> getNodeIds() { return nodeIds; }
    public VirtualNetworkRouter getNetworkRouter() { return networkRouter; }
    public MockDatabaseProxy getDbProxy() { return dbProxy; }

    public DistributedLockExecutor getLockExecutor(String nodeId) {
        return new DistributedLockExecutor(Objects.requireNonNull(getNode(nodeId)));
    }

    public DistributedQueueService getQueueService(String nodeId) {
        return new DistributedQueueService(Objects.requireNonNull(getNode(nodeId)));
    }

    public TaskJournalService getJournalService(String nodeId) {
        return new TaskJournalService(Objects.requireNonNull(getNode(nodeId)));
    }

    /** 현재 리더의 WriteBehindSynchronizer (리더가 없으면 null). */
    public synchronized WriteBehindSynchronizer getWriteBehindSynchronizer() {
        RaftNode leader = getLeaderNode();
        return leader == null ? null : nodes.get(leader.getNodeId()).sync;
    }

    /** 모든 노드 RecoveryCoordinator의 복구 조치 합계. */
    public synchronized List<RecoveryCoordinator.RecoveryAction> getRecoveryActions() {
        List<RecoveryCoordinator.RecoveryAction> all = new ArrayList<>();
        nodes.values().forEach(n -> all.addAll(n.recovery.getActions()));
        return all;
    }
}
