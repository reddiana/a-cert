package com.sds.batchservice.cluster.sync;

import com.sds.batchservice.cluster.fsm.LogEntry;

import java.util.List;

/**
 * Write-Behind 대상 메타 DB 저장소 (DD-05, 5.3 Scenario 4).
 *
 * <p>동기화 기준점(체크포인트)은 노드 메모리가 아닌 DB가 원천이며, 이력 반영과 체크포인트 갱신은
 * 하나의 트랜잭션으로 커밋되어야 합니다. 동일 {@code log_index}의 재반영은 멱등하게 처리되어야 합니다.
 */
public interface DatabaseStorage {

    /** 테이블이 없으면 생성합니다 (선택). */
    void initializeSchema();

    /** 마지막으로 반영 완료된 로그 인덱스. 기록이 없으면 0. */
    long readCheckpoint(String clusterId) throws Exception;

    /** entries 반영과 체크포인트 갱신을 단일 트랜잭션으로 커밋합니다 (log_index 기준 멱등). */
    void writeBatch(String clusterId, List<LogEntry> entries, long newCheckpoint) throws Exception;

    /** 반영된 이력 건수 (진단용). */
    long countRecords() throws Exception;
}
