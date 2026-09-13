package com.batchservice.cluster.fsm;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 결정론적 실행 저널 FSM (DD-04).
 *
 * <p>실행 상태는 {@code JOURNAL_RECORD} 커밋 엔트리로만 변경되며, 현재보다 낮은 leaseToken을 가진
 * 보고(구 실행분의 뒤늦은 보고)는 거부됩니다 (Fencing). 복구 판정은 조회만 제공하고 상태 변경은
 * RecoveryCoordinator가 로그로 제안합니다.
 */
public class JournalStateMachine {

    public enum TaskStatus { PENDING, RUNNING, COMPLETED, FAILED }

    public record Execution(String executionId, String jobId, TaskStatus status,
                            String executorNode, long leaseToken, long updatedAt) {}

    private final Map<String, Execution> executions = new LinkedHashMap<>();

    synchronized CommandResult apply(LogEntry e) {
        Command c = e.getCommand();
        String executionId = c.attr(Command.EXECUTION_ID);
        if (executionId == null) {
            return CommandResult.rejected();
        }
        TaskStatus status;
        try {
            status = TaskStatus.valueOf(c.attr(Command.STATUS).toUpperCase());
        } catch (RuntimeException ex) {
            return CommandResult.rejected();
        }
        long token = c.longAttr(Command.TOKEN, 0L);
        Execution cur = executions.get(executionId);
        if (cur != null && token < cur.leaseToken()) {
            return CommandResult.rejected();
        }
        String jobId = c.attr(Command.JOB_ID) != null ? c.attr(Command.JOB_ID) : (cur != null ? cur.jobId() : null);
        executions.put(executionId, new Execution(executionId, jobId, status,
                c.attr(Command.NODE), token, e.getProposedAt()));
        return CommandResult.ok();
    }

    // ─── 로컬 조회 ────────────────────────────────────────────────────────────
    public synchronized Execution get(String executionId) {
        return executions.get(executionId);
    }

    public synchronized List<Execution> unfinished() {
        List<Execution> result = new ArrayList<>();
        for (Execution ex : executions.values()) {
            if (ex.status() == TaskStatus.RUNNING) result.add(ex);
        }
        return result;
    }

    /** 특정 작업(jobId)의 최신 실행 기록 (가장 높은 leaseToken). */
    public synchronized Execution latestForJob(String jobId) {
        Execution latest = null;
        for (Execution ex : executions.values()) {
            if (jobId.equals(ex.jobId()) && (latest == null || ex.leaseToken() >= latest.leaseToken())) {
                latest = ex;
            }
        }
        return latest;
    }

    public synchronized List<Execution> snapshot() {
        return new ArrayList<>(executions.values());
    }
}
