package com.batchservice.cluster.api;

import com.batchservice.cluster.consensus.RaftNode;
import com.batchservice.cluster.fsm.Command;
import com.batchservice.cluster.fsm.CommandResult;
import com.batchservice.cluster.fsm.QueueStateMachine;

import java.util.concurrent.TimeUnit;

/**
 * 분산 큐 서비스 (DD-03, In-Process API).
 *
 * <p>모든 적재·인출·ACK는 과반수 커밋 후 FSM apply 결과로만 반환됩니다. 인출된 작업은 ACK 전까지
 * in-flight lease로 유지되며, 워커 장애 또는 lease 만료 시 RecoveryCoordinator가 재적재합니다 (At-least-once).
 */
public class DistributedQueueService {

    private final RaftNode raftNode;
    private final long visibilityTimeoutMs;

    public DistributedQueueService(RaftNode raftNode) {
        this(raftNode, 30_000L);
    }

    public DistributedQueueService(RaftNode raftNode, long visibilityTimeoutMs) {
        this.raftNode = raftNode;
        this.visibilityTimeoutMs = visibilityTimeoutMs;
    }

    public boolean offer(String jobId) {
        return execute(Command.enqueue(jobId, false)).isOk();
    }

    public boolean offerFirst(String jobId) {
        return execute(Command.enqueue(jobId, true)).isOk();
    }

    /** 대기열 head를 인출합니다. 비어 있으면 {@code null}. */
    public QueueLease pollLease() {
        CommandResult r = execute(Command.dequeue(raftNode.getNodeId(), visibilityTimeoutMs));
        return r.isOk() ? new QueueLease(r.getValue(), r.getToken()) : null;
    }

    /** timeout 동안 대기하며 인출을 시도합니다. */
    public QueueLease pollLease(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
        QueueStateMachine view = raftNode.getStateMachine().getQueue();
        while (true) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) return null;
            if (!view.isEmpty()) {
                QueueLease lease = pollLease();
                if (lease != null) return lease;
            }
            raftNode.getStateMachine().awaitApply(Math.min(remaining, 20L));
        }
    }

    public String poll() {
        QueueLease lease = pollLease();
        return lease == null ? null : lease.jobId();
    }

    public String poll(long timeout, TimeUnit unit) throws InterruptedException {
        QueueLease lease = pollLease(timeout, unit);
        return lease == null ? null : lease.jobId();
    }

    public boolean renew(QueueLease lease) {
        return execute(Command.queueRenew(lease.jobId(), lease.leaseToken(), visibilityTimeoutMs)).isOk();
    }

    public boolean ack(QueueLease lease) {
        return execute(Command.queueAck(lease.jobId(), lease.leaseToken())).isOk();
    }

    public boolean requeue(QueueLease lease) {
        return execute(Command.requeue(lease.jobId(), lease.leaseToken(), true, "worker-requeue")).isOk();
    }

    /** 로컬 FSM 뷰 기준 대기 건수 (복제 지연이 있을 수 있음). */
    public int size() {
        return raftNode.getStateMachine().getQueue().size();
    }

    public boolean isEmpty() {
        return raftNode.getStateMachine().getQueue().isEmpty();
    }

    public int inFlightCount() {
        return raftNode.getStateMachine().getQueue().inFlightCount();
    }

    private CommandResult execute(Command command) {
        return raftNode.execute(command, raftNode.getTimings().clientTimeoutMs() * 2);
    }
}
