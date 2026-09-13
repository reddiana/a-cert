package com.batchservice.cluster.fsm;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Multi-FSM 라우터 (5.1 Multi-FSM Layer).
 *
 * <p>커밋된 엔트리를 인덱스 순서대로 도메인 FSM(Lock/Queue/Journal) 및 클러스터 상태(멤버십, 동기화 체크포인트)에
 * 적용합니다. {@code requestId}별 결과를 보관하여 클라이언트 재시도 시 동일 결과를 반환합니다 (멱등성).
 */
public class ClusterStateMachine {
    private static final int MAX_REMEMBERED_REQUESTS = 50_000;

    private final LockStateMachine lock = new LockStateMachine();
    private final QueueStateMachine queue = new QueueStateMachine();
    private final JournalStateMachine journal = new JournalStateMachine();

    private final Set<String> deadMembers = new TreeSet<>();
    private long syncCheckpoint = 0L;
    private long appliedIndex = 0L;

    private final Map<String, CommandResult> requestResults =
            new LinkedHashMap<>(1024, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, CommandResult> eldest) {
                    return size() > MAX_REMEMBERED_REQUESTS;
                }
            };

    private final List<Consumer<LogEntry>> applyListeners = new CopyOnWriteArrayList<>();
    private final Object applyMonitor = new Object();

    public CommandResult apply(LogEntry e) {
        CommandResult result;
        synchronized (this) {
            String requestId = e.getCommand().getRequestId();
            CommandResult cached = requestId == null ? null : requestResults.get(requestId);
            if (cached != null) {
                result = cached;
            } else {
                result = route(e);
                if (requestId != null) {
                    requestResults.put(requestId, result);
                }
            }
            appliedIndex = e.getIndex();
        }
        synchronized (applyMonitor) {
            applyMonitor.notifyAll();
        }
        for (Consumer<LogEntry> listener : applyListeners) {
            listener.accept(e);
        }
        return result;
    }

    private CommandResult route(LogEntry e) {
        return switch (e.getType()) {
            case NO_OP -> CommandResult.ok();
            case LOCK_ACQUIRE, LOCK_RENEW, LOCK_RELEASE, LOCK_EXPIRE, LOCK_CANCEL -> lock.apply(e);
            case QUEUE_ENQUEUE, QUEUE_DEQUEUE, QUEUE_RENEW, QUEUE_ACK, QUEUE_REQUEUE, QUEUE_REASSIGN -> queue.apply(e);
            case JOURNAL_RECORD -> journal.apply(e);
            case MEMBER_STATUS -> {
                String node = e.getCommand().attr(Command.NODE);
                boolean changed = e.getCommand().boolAttr(Command.ALIVE) ? deadMembers.remove(node) : deadMembers.add(node);
                yield CommandResult.of(changed);
            }
            case SYNC_CHECKPOINT -> {
                syncCheckpoint = Math.max(syncCheckpoint, e.getCommand().longAttr(Command.CHECKPOINT, 0L));
                yield CommandResult.ok();
            }
        };
    }

    /** 다음 apply가 일어나거나 timeout이 경과할 때까지 대기 (로컬 뷰 변경 대기용). */
    public void awaitApply(long timeoutMs) throws InterruptedException {
        if (timeoutMs <= 0) return;
        synchronized (applyMonitor) {
            applyMonitor.wait(timeoutMs);
        }
    }

    public void addApplyListener(Consumer<LogEntry> listener) {
        applyListeners.add(listener);
    }

    public LockStateMachine getLock() { return lock; }
    public QueueStateMachine getQueue() { return queue; }
    public JournalStateMachine getJournal() { return journal; }

    public synchronized Set<String> getDeadMembers() { return new TreeSet<>(deadMembers); }
    public synchronized boolean isAlive(String nodeId) { return !deadMembers.contains(nodeId); }
    public synchronized long getSyncCheckpoint() { return syncCheckpoint; }
    public synchronized long getAppliedIndex() { return appliedIndex; }
}
