package com.batchservice.cluster.fsm;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 결정론적 분산 큐 FSM (DD-03).
 *
 * <p>인출 대상 작업은 커밋된 {@code QUEUE_DEQUEUE} 엔트리를 apply하는 시점에 FSM이 결정하며,
 * 인출된 작업은 ACK 전까지 in-flight(lease)로 보관되어 워커 장애 시 재적재됩니다 (At-least-once).
 */
public class QueueStateMachine {

    public record InFlight(String jobId, String worker, long leaseToken, long deadline) {}

    private final ArrayDeque<String> waiting = new ArrayDeque<>();
    private final Set<String> known = new HashSet<>();
    private final Map<String, InFlight> inFlight = new LinkedHashMap<>();

    synchronized CommandResult apply(LogEntry e) {
        Command c = e.getCommand();
        String jobId = c.attr(Command.JOB_ID);
        long token = c.longAttr(Command.TOKEN, -1L);
        InFlight f = jobId == null ? null : inFlight.get(jobId);

        switch (c.getType()) {
            case QUEUE_ENQUEUE -> {
                if (jobId == null || !known.add(jobId)) {
                    return CommandResult.rejected();
                }
                if (c.boolAttr(Command.FRONT)) waiting.addFirst(jobId); else waiting.addLast(jobId);
                return CommandResult.ok();
            }
            case QUEUE_DEQUEUE -> {
                String head = waiting.pollFirst();
                if (head == null) {
                    return CommandResult.rejected();
                }
                long visibility = c.longAttr(Command.VISIBILITY_MS, 30_000L);
                inFlight.put(head, new InFlight(head, c.attr(Command.WORKER), e.getIndex(),
                        e.getProposedAt() + visibility));
                return new CommandResult(true, e.getIndex(), head);
            }
            case QUEUE_RENEW -> {
                if (f != null && f.leaseToken() == token) {
                    inFlight.put(jobId, new InFlight(jobId, f.worker(), token,
                            e.getProposedAt() + c.longAttr(Command.VISIBILITY_MS, 30_000L)));
                    return CommandResult.ok();
                }
                return CommandResult.rejected();
            }
            case QUEUE_ACK -> {
                if (f != null && f.leaseToken() == token) {
                    inFlight.remove(jobId);
                    known.remove(jobId);
                    return CommandResult.ok();
                }
                return CommandResult.rejected();
            }
            case QUEUE_REQUEUE -> {
                if (f != null && f.leaseToken() == token) {
                    inFlight.remove(jobId);
                    if (c.boolAttr(Command.FRONT)) waiting.addFirst(jobId); else waiting.addLast(jobId);
                    return CommandResult.ok();
                }
                return CommandResult.rejected();
            }
            case QUEUE_REASSIGN -> {
                if (f != null && f.leaseToken() == token) {
                    long visibility = c.longAttr(Command.VISIBILITY_MS, 30_000L);
                    inFlight.put(jobId, new InFlight(jobId, c.attr(Command.WORKER), e.getIndex(),
                            e.getProposedAt() + visibility));
                    return new CommandResult(true, e.getIndex(), jobId);
                }
                return CommandResult.rejected();
            }
            default -> {
                return CommandResult.rejected();
            }
        }
    }

    // ─── 로컬 조회 ────────────────────────────────────────────────────────────
    public synchronized int size() { return waiting.size(); }
    public synchronized boolean isEmpty() { return waiting.isEmpty(); }
    public synchronized int inFlightCount() { return inFlight.size(); }
    public synchronized InFlight getInFlight(String jobId) { return inFlight.get(jobId); }
    public synchronized List<InFlight> inFlightSnapshot() { return new ArrayList<>(inFlight.values()); }
    public synchronized List<String> waitingSnapshot() { return new ArrayList<>(waiting); }
}
