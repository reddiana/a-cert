package com.sds.batchservice.cluster.recovery;

/**
 * Execution Target 실행 상태 조회 (5.3 Scenario 2 4단계).
 *
 * <p>수요처가 K8s Job API 등으로 구현하여 빈으로 등록합니다. 기본 구현은 {@link ExecutionStatus#UNKNOWN}을
 * 반환하며, 이 경우 고립 작업은 재적재됩니다 (At-least-once: 재실행 가능 작업 전제).
 */
@FunctionalInterface
public interface ExecutionStatusProvider {

    enum ExecutionStatus { RUNNING, COMPLETED, FAILED, NOT_FOUND, UNKNOWN }

    ExecutionStatus getStatus(String executionId, String jobId);

    static ExecutionStatusProvider unknown() {
        return (executionId, jobId) -> ExecutionStatus.UNKNOWN;
    }
}
