package com.batchservice.cluster.storage;

import com.batchservice.cluster.consensus.RaftNode;
import com.batchservice.cluster.fsm.Command;
import com.batchservice.cluster.fsm.CommandType;
import com.batchservice.cluster.fsm.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Write-Behind 비동기 DB 동기화 및 Reconciliation (5.1 계층 5, 5.4 WriteBehindSynchronizer, 5.3 Scenario 4).
 *
 * <ul>
 *   <li>리더(NO_OP 커밋 완료)에서만 활성화. 강등 시 즉시 STANDBY</li>
 *   <li>동기화 기준점은 활성화·장애 복구 시마다 DB 체크포인트에서 재조회 (메모리 값 미신뢰)</li>
 *   <li>커밋된 WAL 구간을 batchSize 단위로 판독하여 이력 반영 + 체크포인트 갱신을 단일 트랜잭션으로 커밋</li>
 *   <li>DB 장애 시 1s→2s→4s→8s→10s(상한) backoff. 합의·FSM 경로는 영향 없음</li>
 *   <li>반영 완료 후 SYNC_CHECKPOINT를 제안하여 전 노드 WAL 압축 하한선 동기화</li>
 * </ul>
 */
public class WriteBehindSynchronizer {
    private static final Logger log = LoggerFactory.getLogger(WriteBehindSynchronizer.class);
    private static final long INITIAL_BACKOFF_MS = 1_000L;
    private static final long MAX_BACKOFF_MS = 10_000L;
    private static final int MAX_BATCHES_PER_RUN = 200;

    public enum State { STANDBY, NORMAL, DB_DOWN, RECONCILING }

    private final RaftNode raftNode;
    private final DatabaseStorage database;
    private final String clusterId;
    private final int batchSize;
    private final long intervalMs;

    private volatile State state = State.STANDBY;
    private volatile long checkpoint = -1L;
    private long backoffMs;
    private long nextAttemptAt;
    private boolean dataSyncedSinceProposal;
    private ScheduledExecutorService executor;

    public WriteBehindSynchronizer(RaftNode raftNode, DatabaseStorage database, String clusterId,
                                   int batchSize, long intervalMs) {
        this.raftNode = raftNode;
        this.database = database;
        this.clusterId = clusterId;
        this.batchSize = batchSize;
        this.intervalMs = intervalMs;
    }

    public void start() {
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "write-behind-" + raftNode.getNodeId());
            t.setDaemon(true);
            return t;
        });
        executor.scheduleWithFixedDelay(this::safeRun, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (executor != null) executor.shutdownNow();
    }

    private void safeRun() {
        try {
            runOnce();
        } catch (RuntimeException e) {
            log.error("[{}] Write-Behind run failed", raftNode.getNodeId(), e);
        }
    }

    public synchronized void runOnce() {
        if (!raftNode.isLeaderReady()) {
            if (state != State.STANDBY) {
                log.info("[{}] Write-Behind → STANDBY (not leader).", raftNode.getNodeId());
            }
            state = State.STANDBY;
            checkpoint = -1L;
            return;
        }
        long now = System.currentTimeMillis();
        if (state == State.DB_DOWN && now < nextAttemptAt) {
            return;
        }
        try {
            if (checkpoint < 0) {
                checkpoint = database.readCheckpoint(clusterId);
                log.info("[{}] Write-Behind activated from DB checkpoint last_log_index={}.", raftNode.getNodeId(), checkpoint);
                if (state == State.DB_DOWN) {
                    state = State.RECONCILING;
                }
            }
            long commit = raftNode.getCommitIndex();
            int batches = 0;
            while (checkpoint < commit && batches++ < MAX_BATCHES_PER_RUN) {
                List<LogEntry> batch = raftNode.getRaftLog().slice(checkpoint + 1, commit, batchSize);
                if (batch.isEmpty()) break;
                long last = batch.get(batch.size() - 1).getIndex();
                database.writeBatch(clusterId, batch, last);
                if (batch.stream().anyMatch(e -> e.getType() != CommandType.SYNC_CHECKPOINT)) {
                    dataSyncedSinceProposal = true;
                }
                checkpoint = last;
                raftNode.getRaftLog().setCompactionFloor(last);
            }
            backoffMs = 0L;
            if (checkpoint >= commit) {
                if (state != State.NORMAL) {
                    log.info("[{}] Write-Behind synchronized up to index {} → NORMAL.", raftNode.getNodeId(), checkpoint);
                }
                state = State.NORMAL;
                if (dataSyncedSinceProposal) {
                    dataSyncedSinceProposal = false;
                    raftNode.propose(List.of(Command.syncCheckpoint(checkpoint)));
                }
            } else if (state == State.STANDBY) {
                state = State.RECONCILING;
            }
        } catch (Exception e) {
            if (state != State.DB_DOWN) {
                log.warn("[{}] Meta DB unavailable → DB_DOWN (checkpoint={}): {}", raftNode.getNodeId(), checkpoint, e.getMessage());
            }
            state = State.DB_DOWN;
            checkpoint = -1L;
            backoffMs = backoffMs == 0 ? INITIAL_BACKOFF_MS : Math.min(backoffMs * 2, MAX_BACKOFF_MS);
            nextAttemptAt = now + backoffMs;
        }
    }

    /** 체크포인트 이후 커밋된 이력 중 DB 미반영 건수 (메타 엔트리 SYNC_CHECKPOINT 제외). */
    public long getUnsyncedRecordCount() {
        long base = checkpoint >= 0 ? checkpoint : raftNode.getStateMachine().getSyncCheckpoint();
        long commit = raftNode.getCommitIndex();
        if (commit <= base) return 0L;
        return raftNode.getRaftLog().slice(base + 1, commit, Integer.MAX_VALUE).stream()
                .filter(e -> e.getType() != CommandType.SYNC_CHECKPOINT)
                .count();
    }

    public boolean isFullySynchronized() {
        return state == State.NORMAL && getUnsyncedRecordCount() == 0L;
    }

    public State getState() { return state; }
    public long getCheckpoint() { return checkpoint; }
}
