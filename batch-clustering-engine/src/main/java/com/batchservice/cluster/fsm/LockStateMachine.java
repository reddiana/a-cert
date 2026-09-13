package com.batchservice.cluster.fsm;

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
 *   <li>{@code fenceToken} = 락을 부여한 커밋 엔트리의 로그 인덱스 (단조 증가)</li>
 *   <li>대기자(waiter)는 FIFO로 관리하여 해제 시 결정론적으로 다음 소유자에게 부여 (경합 재시도 폭주 방지)</li>
 * </ul>
 */
public class LockStateMachine {

    public record LockInfo(String key, String owner, long fenceToken, long expireAt) {
        public boolean isExpiredAt(long time) { return expireAt <= time; }
    }

    private record Waiter(String owner, long leaseMs, long waitDeadline) {}

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
                    locks.put(key, new LockInfo(key, owner, e.getIndex(), now + leaseMs));
                    return new CommandResult(true, e.getIndex(), GRANTED);
                }
                if (cur.owner().equals(owner)) {
                    locks.put(key, new LockInfo(key, owner, cur.fenceToken(), now + leaseMs));
                    return new CommandResult(true, cur.fenceToken(), GRANTED);
                }
                if (waitMs > 0) {
                    ArrayDeque<Waiter> q = waiters.computeIfAbsent(key, k -> new ArrayDeque<>());
                    if (q.stream().noneMatch(w -> w.owner().equals(owner))) {
                        q.addLast(new Waiter(owner, leaseMs, now + waitMs));
                    }
                    return new CommandResult(false, 0L, QUEUED);
                }
                return new CommandResult(false, 0L, DENIED);
            }
            case LOCK_RENEW -> {
                if (cur != null && cur.fenceToken() == c.longAttr(Command.TOKEN, -1L) && !cur.isExpiredAt(now)) {
                    locks.put(key, new LockInfo(key, cur.owner(), cur.fenceToken(),
                            now + c.longAttr(Command.LEASE_MS, 10_000L)));
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

    /** 유효한(대기 기한이 남은) 첫 번째 대기자에게 락 부여. fenceToken = 부여 엔트리 인덱스. */
    private void promote(String key, LogEntry e) {
        ArrayDeque<Waiter> q = waiters.get(key);
        while (q != null && !q.isEmpty()) {
            Waiter w = q.pollFirst();
            if (w.waitDeadline() > e.getProposedAt()) {
                locks.put(key, new LockInfo(key, w.owner(), e.getIndex(), e.getProposedAt() + w.leaseMs()));
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
