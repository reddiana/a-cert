package com.batchservice.cluster.api;

import com.batchservice.cluster.consensus.RaftNode;
import com.batchservice.cluster.fsm.Command;
import com.batchservice.cluster.fsm.JournalStateMachine;
import com.batchservice.cluster.fsm.JournalStateMachine.Execution;
import com.batchservice.cluster.fsm.JournalStateMachine.TaskStatus;

import java.util.List;

/**
 * 태스크 실행 저널 서비스 (DD-04, In-Process API).
 *
 * <p>실행 상태 전이는 {@code JOURNAL_RECORD}로 커밋되며, 보고자의 leaseToken이 현재 기록보다 낮으면 거부됩니다.
 */
public class TaskJournalService {

    private final RaftNode raftNode;

    public TaskJournalService(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    /**
     * @return 반영 여부 (false = 구 실행분의 뒤늦은 보고로 거부됨)
     */
    public boolean record(String executionId, String jobId, TaskStatus status, long leaseToken) {
        return raftNode.execute(
                Command.journal(executionId, jobId, status.name(), raftNode.getNodeId(), leaseToken),
                raftNode.getTimings().clientTimeoutMs() * 2).isOk();
    }

    public Execution get(String executionId) {
        return journal().get(executionId);
    }

    public List<Execution> unfinished() {
        return journal().unfinished();
    }

    private JournalStateMachine journal() {
        return raftNode.getStateMachine().getJournal();
    }
}
