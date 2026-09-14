package com.sds.batchservice.cluster.api;

import com.sds.batchservice.cluster.consensus.RaftNode;
import com.sds.batchservice.cluster.fsm.Command;
import com.sds.batchservice.cluster.fsm.CommandResult;
import com.sds.batchservice.cluster.fsm.LockStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 분산 락 실행기 (DD-02, In-Process API).
 *
 * <ul>
 *   <li>AS-IS Hazelcast 기반 공통모듈과 동일한 {@code execute(lockName, ownerId, Supplier|Runnable)} 시그니처와 락 의미를 제공 (NR-05)
 *     <ul>
 *       <li>기본 대기는 {@link #WAIT_FOREVER}로, {@code IMap.lock()}과 같이 획득할 때까지 대기</li>
 *       <li>소유 단위는 스레드. 같은 {@code ownerId}를 넘기는 서로 다른 스레드도 상호 배제됨</li>
 *       <li>같은 스레드의 같은 락 재진입 허용. 가장 바깥 호출이 끝날 때 해제</li>
 *     </ul>
 *   </li>
 *   <li>소유자 식별자는 {@code "{nodeId}:{threadId}:{ownerId}"} 형식으로 기록되어 노드 장애 시 회수 대상 식별이 가능</li>
 *   <li>획득 실패 시 FSM의 FIFO 대기열에 등록되고, 로컬 FSM 뷰가 자신을 소유자로 반영할 때까지 대기 (재시도 폭주 없음)</li>
 *   <li>임계 구역 수행 중 lease의 1/3 주기로 {@code LOCK_RENEW}, 종료 시 fenceToken으로 {@code LOCK_RELEASE}</li>
 *   <li>팔로워 노드에서 호출해도 RaftNode가 리더로 전달</li>
 * </ul>
 */
public class DistributedLockExecutor {
    private static final Logger log = LoggerFactory.getLogger(DistributedLockExecutor.class);

    /** 획득할 때까지 대기 (AS-IS Hazelcast {@code IMap.lock()}과 동일). */
    public static final long WAIT_FOREVER = Long.MAX_VALUE;

    /** {@code LOCK_ACQUIRE} 커밋 대기 상한. Hazelcast 기본 operation call timeout(60초)과 동일. */
    private static final long MAX_COMMIT_TIMEOUT_MS = 60_000L;

    private static final ScheduledExecutorService RENEWER = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "lock-renewer");
        t.setDaemon(true);
        return t;
    });

    private final RaftNode raftNode;
    private final long defaultWaitMs;
    private final long defaultLeaseMs;
    /** 스레드별 보유 중인 락 이름 → 재진입 깊이 */
    private final ThreadLocal<Map<String, Integer>> holdCounts = ThreadLocal.withInitial(HashMap::new);

    public DistributedLockExecutor(RaftNode raftNode) {
        this(raftNode, WAIT_FOREVER, 10_000L);
    }

    public DistributedLockExecutor(RaftNode raftNode, long defaultWaitMs, long defaultLeaseMs) {
        this.raftNode = raftNode;
        this.defaultWaitMs = defaultWaitMs;
        this.defaultLeaseMs = defaultLeaseMs;
    }

    public <T> T execute(String lockName, String ownerId, Supplier<T> supplier) {
        return execute(lockName, ownerId, defaultWaitMs, defaultLeaseMs, supplier);
    }

    public void execute(String lockName, String ownerId, Runnable runnable) {
        execute(lockName, ownerId, defaultWaitMs, defaultLeaseMs, () -> {
            runnable.run();
            return null;
        });
    }

    public <T> T execute(String lockName, String ownerId, long waitMs, long leaseMs, Supplier<T> supplier) {
        Map<String, Integer> holds = holdCounts.get();
        Integer depth = holds.get(lockName);
        if (depth != null) {
            // 재진입: 이미 이 스레드가 보유 중이므로 합의 명령 없이 실행
            holds.put(lockName, depth + 1);
            try {
                return supplier.get();
            } finally {
                holds.put(lockName, depth);
            }
        }

        String owner = raftNode.getNodeId() + ":" + Thread.currentThread().getId() + ":" + ownerId;
        long fenceToken = acquire(lockName, owner, waitMs, leaseMs);
        holds.put(lockName, 1);
        log.debug("Acquired lock: [{}][{}] fenceToken={}", lockName, owner, fenceToken);
        long renewPeriod = Math.max(leaseMs / 3, 1L);
        ScheduledFuture<?> renewal = RENEWER.scheduleAtFixedRate(
                () -> renew(lockName, owner, fenceToken, leaseMs), renewPeriod, renewPeriod, TimeUnit.MILLISECONDS);
        try {
            return supplier.get();
        } finally {
            holds.remove(lockName);
            renewal.cancel(false);
            release(lockName, owner, fenceToken);
        }
    }

    /**
     * 락을 획득하고 fenceToken을 반환합니다.
     *
     * @param waitMs 획득 대기 시간. {@link #WAIT_FOREVER}이면 획득하거나 인터럽트될 때까지 대기
     * @throws IllegalStateException waitMs 내에 획득하지 못했거나 대기 중 인터럽트된 경우
     */
    public long acquire(String lockName, String owner, long waitMs, long leaseMs) {
        long deadline = LockStateMachine.saturatedAdd(System.currentTimeMillis(), waitMs);
        long commitTimeout = Math.max(Math.min(waitMs, MAX_COMMIT_TIMEOUT_MS), raftNode.getTimings().clientTimeoutMs());
        CommandResult result = raftNode.execute(Command.lockAcquire(lockName, owner, leaseMs, waitMs), commitTimeout);
        if (result.isOk()) {
            return result.getToken();
        }

        LockStateMachine lockView = raftNode.getStateMachine().getLock();
        try {
            while (true) {
                long now = System.currentTimeMillis();
                if (now >= deadline) break;
                LockStateMachine.LockInfo info = lockView.get(lockName);
                if (info != null && info.owner().equals(owner)) {
                    return info.fenceToken(); // FIFO 대기열에서 승격됨
                }
                if (info == null || info.isExpiredAt(now)) {
                    // 락이 비었거나 만료됨 → 재제안하여 만료 처리 및 승격 유도 (신규 requestId)
                    result = raftNode.execute(Command.lockAcquire(lockName, owner, leaseMs, deadline - now), commitTimeout);
                    if (result.isOk()) {
                        return result.getToken();
                    }
                }
                raftNode.getStateMachine().awaitApply(Math.min(50L, Math.max(deadline - now, 1L)));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 대기 취소 (취소 커밋 직전에 승격되었다면 LOCK_CANCEL이 함께 해제)
        // 인터럽트 상태에서는 커밋 대기가 즉시 실패하므로 취소 커밋 동안 인터럽트 플래그를 보류
        boolean interrupted = Thread.interrupted();
        try {
            raftNode.execute(Command.lockCancel(lockName, owner), raftNode.getTimings().clientTimeoutMs());
        } catch (RuntimeException e) {
            log.warn("Lock cancel failed for [{}][{}]: {}", lockName, owner, e.getMessage());
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        if (interrupted) {
            throw new IllegalStateException("Interrupted while waiting for lock: " + lockName);
        }
        throw new IllegalStateException("Could not acquire lock: " + lockName + " within " + waitMs + "ms");
    }

    private void renew(String lockName, String owner, long fenceToken, long leaseMs) {
        try {
            CommandResult r = raftNode.execute(Command.lockRenew(lockName, fenceToken, leaseMs),
                    raftNode.getTimings().clientTimeoutMs());
            if (!r.isOk()) {
                log.warn("Lock lease renewal rejected: [{}][{}] fenceToken={} (lock lost)", lockName, owner, fenceToken);
            }
        } catch (RuntimeException e) {
            log.warn("Lock lease renewal failed: [{}][{}]: {}", lockName, owner, e.getMessage());
        }
    }

    private void release(String lockName, String owner, long fenceToken) {
        try {
            CommandResult r = raftNode.execute(Command.lockRelease(lockName, fenceToken),
                    raftNode.getTimings().clientTimeoutMs());
            if (!r.isOk()) {
                log.warn("Lock release rejected (already expired/reclaimed): [{}][{}] fenceToken={}", lockName, owner, fenceToken);
            }
        } catch (RuntimeException e) {
            log.warn("Lock release failed: [{}][{}]: {} (lease expiry will reclaim)", lockName, owner, e.getMessage());
        }
    }
}
