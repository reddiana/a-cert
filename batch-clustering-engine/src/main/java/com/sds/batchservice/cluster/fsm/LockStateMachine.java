package com.sds.batchservice.cluster.fsm;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 결정론적 분산 락 FSM (DD-02).
 *
 * <ul>
 *   <li>lease 만료 판정은 로컬 시계가 아닌 엔트리의 {@code proposedAt} 기준</li>
 *   <li>신규 리더의 {@code NO_OP} apply 시 lease를 신규 리더 시계 기준으로 재설정 (노드 간 시계 동기화 비의존)</li>
 *   <li>{@code fenceToken} = 락을 부여한 커밋 엔트리의 로그 인덱스 (단조 증가)</li>
 *   <li>대기자(waiter)는 FIFO로 관리하여 해제 시 결정론적으로 다음 소유자에게 부여 (경합 재시도 폭주 방지)</li>
 * </ul>
 */
public class LockStateMachine {

    public record LockInfo(String key, String owner, long fenceToken, long leaseMs, long expireAt) {
        public boolean isExpiredAt(long time) { return expireAt <= time; }
    }

    private record Waiter(String owner, long leaseMs, long waitMs, long waitDeadline) {}

    public static final String GRANTED = "GRANTED";
    public static final String QUEUED = "QUEUED";
    public static final String DENIED = "DENIED";

    private final Map<String, LockInfo> locks = new HashMap<>();
    private final Map<String, ArrayDeque<Waiter>> waiters = new HashMap<>();

    synchronized CommandResult apply(LogEntry e) {
        Command c = e.getCommand();
        String key = c.attr(Command.KEY);
        long now = e.getProposedAt();
        LockInfo cur = locks.get(key);

        switch (c.getType()) {
            case LOCK_ACQUIRE -> {
                String owner = c.attr(Command.OWNER);
                long leaseMs = c.longAttr(Command.LEASE_MS, 10_000L);
                long waitMs = c.longAttr(Command.WAIT_MS, 0L);
                if (cur != null && cur.isExpiredAt(now)) {
                    locks.remove(key);
                    promote(key, e);
                    cur = locks.get(key);
                }
                if (cur == null) {
                    removeWaiter(key, owner);
                    locks.put(key, new LockInfo(key, owner, e.getIndex(), leaseMs, now + leaseMs));
                    return new CommandResult(true, e.getIndex(), GRANTED);
                }
                if (cur.owner().equals(owner)) {
                    locks.put(key, new LockInfo(key, owner, cur.fenceToken(), leaseMs, now + leaseMs));
                    return new CommandResult(true, cur.fenceToken(), GRANTED);
                }
                if (waitMs > 0) {
                    ArrayDeque<Waiter> q = waiters.computeIfAbsent(key, k -> new ArrayDeque<>());
                    if (q.stream().noneMatch(w -> w.owner().equals(owner))) {
                        q.addLast(new Waiter(owner, leaseMs, waitMs, saturatedAdd(now, waitMs)));
                    }
                    return new CommandResult(false, 0L, QUEUED);
                }
                return new CommandResult(false, 0L, DENIED);
            }
            case LOCK_RENEW -> {
                if (cur != null && cur.fenceToken() == c.longAttr(Command.TOKEN, -1L) && !cur.isExpiredAt(now)) {
                    long leaseMs = c.longAttr(Command.LEASE_MS, 10_000L);
                    locks.put(key, new LockInfo(key, cur.owner(), cur.fenceToken(), leaseMs, now + leaseMs));
                    return new CommandResult(true, cur.fenceToken(), GRANTED);
                }
                return CommandResult.rejected();
            }
            case LOCK_RELEASE, LOCK_EXPIRE -> {
                if (cur != null && cur.fenceToken() == c.longAttr(Command.TOKEN, -1L)) {
                    locks.remove(key);
                    promote(key, e);
                    return CommandResult.ok();
                }
                return CommandResult.rejected();
            }
            case LOCK_CANCEL -> {
                String owner = c.attr(Command.OWNER);
                boolean removed = removeWaiter(key, owner);
                if (cur != null && cur.owner().equals(owner)) {
                    locks.remove(key);
                    promote(key, e);
                    removed = true;
                }
                return CommandResult.of(removed);
            }
            default -> {
                return CommandResult.rejected();
            }
        }
    }

    /**
     * 신규 리더의 {@code NO_OP} apply 시 lease 기준 시계를 재설정합니다.
     *
     * <p>기존 만료 시각은 이전 리더의 시계로 계산되어 신규 리더 시계와 비교할 수 없으므로,
     * {@code NO_OP}의 {@code proposedAt}(신규 리더 시계)부터 lease·대기 기간 전체를 다시 부여합니다.
     * 만료는 늦어질 수만 있고 앞당겨지지 않으므로 노드 간 시계 차이로 인한 이중 소유가 발생하지 않습니다.
     */
    synchronized void rebaseLeases(long now) {
        locks.replaceAll((k, l) -> new LockInfo(k, l.owner(), l.fenceToken(), l.leaseMs(), now + l.leaseMs()));
        for (ArrayDeque<Waiter> q : waiters.values()) {
            List<Waiter> rebased = q.stream()
                    .map(w -> new Waiter(w.owner(), w.leaseMs(), w.waitMs(), saturatedAdd(now, w.waitMs())))
                    .toList();
            q.clear();
            q.addAll(rebased);
        }
    }

    /** 무기한 대기({@code Long.MAX_VALUE})의 기한 계산이 음수로 넘치지 않도록 상한에서 멈추는 덧셈. */
    public static long saturatedAdd(long time, long durationMs) {
        return durationMs >= Long.MAX_VALUE - time ? Long.MAX_VALUE : time + durationMs;
    }

    /** 유효한(대기 기한이 남은) 첫 번째 대기자에게 락 부여. fenceToken = 부여 엔트리 인덱스. */
    private void promote(String key, LogEntry e) {
        ArrayDeque<Waiter> q = waiters.get(key);
        while (q != null && !q.isEmpty()) {
            Waiter w = q.pollFirst();
            if (w.waitDeadline() > e.getProposedAt()) {
                locks.put(key, new LockInfo(key, w.owner(), e.getIndex(), w.leaseMs(), e.getProposedAt() + w.leaseMs()));
                break;
            }
        }
        if (q != null && q.isEmpty()) {
            waiters.remove(key);
        }
    }

    private boolean removeWaiter(String key, String owner) {
        ArrayDeque<Waiter> q = waiters.get(key);
        if (q == null) return false;
        boolean removed = false;
        for (Iterator<Waiter> it = q.iterator(); it.hasNext(); ) {
            if (it.next().owner().equals(owner)) {
                it.remove();
                removed = true;
            }
        }
        if (q.isEmpty()) waiters.remove(key);
        return removed;
    }

    // ─── 로컬 조회 (복제 지연이 있을 수 있는 읽기 전용 뷰) ─────────────────────
    public synchronized LockInfo get(String key) {
        return locks.get(key);
    }

    public synchronized List<LockInfo> snapshot() {
        return new ArrayList<>(locks.values());
    }

    public synchronized boolean isWaiting(String key, String owner) {
        ArrayDeque<Waiter> q = waiters.get(key);
        return q != null && q.stream().anyMatch(w -> w.owner().equals(owner));
    }
}
