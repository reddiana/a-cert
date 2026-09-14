package com.sds.batchservice.cluster.recovery;

import com.sds.batchservice.cluster.consensus.RaftNode;
import com.sds.batchservice.cluster.fsm.ClusterStateMachine;
import com.sds.batchservice.cluster.fsm.Command;
import com.sds.batchservice.cluster.fsm.CommandResult;
import com.sds.batchservice.cluster.fsm.JournalStateMachine.Execution;
import com.sds.batchservice.cluster.fsm.LockStateMachine.LockInfo;
import com.sds.batchservice.cluster.fsm.QueueStateMachine.InFlight;
import com.sds.batchservice.cluster.recovery.ExecutionStatusProvider.ExecutionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * 고립(Orphan) 작업 판정 및 복구 조정기 (DD-04, 5.3 Scenario 2 4~5단계). 리더에서만 동작합니다.
 *
 * <ul>
 *   <li>추적 노드가 ALIVE이고 lease가 유효한 작업은 조치하지 않음 (중복 실행 방지)</li>
 *   <li>추적 노드 DEAD 또는 lease 만료 작업은 Execution Target 상태를 조회하여 인수/완료/재적재를 결정</li>
 *   <li>모든 복구 조치는 FSM 직접 수정이 아닌 로그 명령으로 제안 (전 노드 FSM 일관성)</li>
 *   <li>DEAD 노드 소유 락 또는 lease 만료 락은 LOCK_EXPIRE로 회수</li>
 * </ul>
 */
public class RecoveryCoordinator {
    private static final Logger log = LoggerFactory.getLogger(RecoveryCoordinator.class);

    public enum ActionKind { ADOPTED, REQUEUED, COMPLETED, LOCK_RECLAIMED }

    public record RecoveryAction(ActionKind kind, String target, String executionId,
                                 ExecutionStatus observedStatus, long leaseToken) {}

    private final RaftNode raftNode;
    private final ExecutionStatusProvider statusProvider;
    private final long scanIntervalMs;
    private final long visibilityTimeoutMs;

    private final Set<String> inProgress = ConcurrentHashMap.newKeySet();
    private final List<RecoveryAction> actions = new CopyOnWriteArrayList<>();
    private final List<Consumer<RecoveryAction>> listeners = new CopyOnWriteArrayList<>();
    private ScheduledExecutorService scanner;
    private ExecutorService worker;

    public RecoveryCoordinator(RaftNode raftNode, ExecutionStatusProvider statusProvider,
                               long scanIntervalMs, long visibilityTimeoutMs) {
        this.raftNode = raftNode;
        this.statusProvider = statusProvider;
        this.scanIntervalMs = scanIntervalMs;
        this.visibilityTimeoutMs = visibilityTimeoutMs;
    }

    public void start() {
        String id = raftNode.getNodeId();
        worker = Executors.newSingleThreadExecutor(r -> daemon(r, "recovery-worker-" + id));
        scanner = Executors.newSingleThreadScheduledExecutor(r -> daemon(r, "recovery-scan-" + id));
        scanner.scheduleWithFixedDelay(this::safeScan, scanIntervalMs, scanIntervalMs, TimeUnit.MILLISECONDS);
        raftNode.addLeadershipListener(term -> scanner.execute(this::safeScan));
    }

    public void stop() {
        if (scanner != null) scanner.shutdownNow();
        if (worker != null) worker.shutdownNow();
    }

    /** 수요처 BatchLauncher가 인수(ADOPTED)·재적재 이벤트를 받아 추적을 이어가도록 등록. */
    public void addListener(Consumer<RecoveryAction> listener) {
        listeners.add(listener);
    }

    public List<RecoveryAction> getActions() {
        return List.copyOf(actions);
    }

    private void safeScan() {
        try {
            scan();
        } catch (RuntimeException e) {
            log.error("[{}] recovery scan failed", raftNode.getNodeId(), e);
        }
    }

    public void scan() {
        if (!raftNode.isLeaderReady()) return;
        ClusterStateMachine fsm = raftNode.getStateMachine();
        Set<String> dead = fsm.getDeadMembers();
        dead.remove(raftNode.getNodeId()); // 스캔 중인 리더 자신은 생존 (자신의 ALIVE 커밋 전 자기 작업 회수 방지)
        long now = raftNode.wallClockMillis(); // proposedAt과 동일한 시계로 만료 판정

        for (LockInfo lock : fsm.getLock().snapshot()) {
            String ownerNode = lock.owner().contains(":") ? lock.owner().substring(0, lock.owner().indexOf(':')) : lock.owner();
            if (!(dead.contains(ownerNode) || lock.isExpiredAt(now))) continue;
            String key = "lock:" + lock.key() + ":" + lock.fenceToken();
            if (!inProgress.add(key)) continue;
            submit(key, () -> {
                CommandResult r = raftNode.execute(Command.lockExpire(lock.key(), lock.fenceToken()), timeout());
                if (r.isOk()) record(new RecoveryAction(ActionKind.LOCK_RECLAIMED, lock.key(), null, null, lock.fenceToken()));
            });
        }

        for (InFlight f : fsm.getQueue().inFlightSnapshot()) {
            boolean executorDead = dead.contains(f.worker());
            boolean leaseExpired = f.deadline() <= now;
            if (!executorDead && !leaseExpired) continue; // 추적 노드 생존 + lease 유효 → 조치 없음
            String key = "job:" + f.jobId() + ":" + f.leaseToken();
            if (!inProgress.add(key)) continue;
            submit(key, () -> recoverJob(f, executorDead));
        }
    }

    private void recoverJob(InFlight f, boolean executorDead) {
        Execution exec = raftNode.getStateMachine().getJournal().latestForJob(f.jobId());
        if (exec != null && exec.leaseToken() != f.leaseToken()) {
            exec = null; // 이전 실행 기록 — 현재 lease에 대응하는 실행 없음
        }
        ExecutionStatus status = exec == null ? ExecutionStatus.NOT_FOUND : queryStatus(exec);
        log.warn("[{}] Orphan job [{}] (worker={}, dead={}, leaseToken={}) → target status {}.",
                raftNode.getNodeId(), f.jobId(), f.worker(), executorDead, f.leaseToken(), status);

        switch (status) {
            case RUNNING -> {
                CommandResult r = raftNode.execute(
                        Command.reassign(f.jobId(), f.leaseToken(), raftNode.getNodeId(), visibilityTimeoutMs), timeout());
                if (r.isOk()) {
                    raftNode.execute(Command.journal(exec.executionId(), f.jobId(), "RUNNING",
                            raftNode.getNodeId(), r.getToken()), timeout());
                    record(new RecoveryAction(ActionKind.ADOPTED, f.jobId(), exec.executionId(), status, r.getToken()));
                }
            }
            case COMPLETED -> {
                List<Command> cmds = List.of(
                        Command.journal(exec.executionId(), f.jobId(), "COMPLETED", exec.executorNode(), f.leaseToken()),
                        Command.queueAck(f.jobId(), f.leaseToken()));
                if (raftNode.execute(cmds, timeout()).get(1).isOk()) {
                    record(new RecoveryAction(ActionKind.COMPLETED, f.jobId(), exec.executionId(), status, f.leaseToken()));
                }
            }
            default -> {
                List<Command> cmds = new ArrayList<>();
                if (exec != null) {
                    cmds.add(Command.journal(exec.executionId(), f.jobId(), "PENDING", exec.executorNode(), f.leaseToken()));
                }
                cmds.add(Command.requeue(f.jobId(), f.leaseToken(), true,
                        executorDead ? "executor-dead" : "lease-expired"));
                List<CommandResult> results = raftNode.execute(cmds, timeout());
                if (results.get(results.size() - 1).isOk()) {
                    record(new RecoveryAction(ActionKind.REQUEUED, f.jobId(),
                            exec == null ? null : exec.executionId(), status, f.leaseToken()));
                }
            }
        }
    }

    private ExecutionStatus queryStatus(Execution exec) {
        try {
            ExecutionStatus s = statusProvider.getStatus(exec.executionId(), exec.jobId());
            return s == null ? ExecutionStatus.UNKNOWN : s;
        } catch (RuntimeException e) {
            log.warn("[{}] Execution status query failed for {}: {}", raftNode.getNodeId(), exec.executionId(), e.getMessage());
            return ExecutionStatus.UNKNOWN;
        }
    }

    private void submit(String key, Runnable task) {
        try {
            worker.execute(() -> {
                try {
                    if (raftNode.isLeaderReady()) task.run();
                } catch (RuntimeException e) {
                    log.warn("[{}] recovery action {} failed: {}", raftNode.getNodeId(), key, e.getMessage());
                } finally {
                    inProgress.remove(key);
                }
            });
        } catch (RuntimeException e) {
            inProgress.remove(key);
        }
    }

    private void record(RecoveryAction action) {
        log.info("[{}] Recovery action: {}", raftNode.getNodeId(), action);
        actions.add(action);
        listeners.forEach(l -> l.accept(action));
    }

    private long timeout() {
        return raftNode.getTimings().clientTimeoutMs() * 2;
    }

    private static Thread daemon(Runnable r, String name) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }
}
