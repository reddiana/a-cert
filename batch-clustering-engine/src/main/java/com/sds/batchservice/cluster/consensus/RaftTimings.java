package com.sds.batchservice.cluster.consensus;

/**
 * Raft 타이밍 파라미터 (5.3.2 공통 런타임 규약 6).
 *
 * @param heartbeatIntervalMs     리더 Heartbeat 주기 (기본 500ms)
 * @param electionTimeoutMinMs    Election Timeout 하한 (기본 1,500ms, GC STW 1초 + Heartbeat 여유)
 * @param electionTimeoutMaxMs    Election Timeout 상한 (기본 2,000ms, QAS-05 감지 2초)
 * @param memberFailureTimeoutMs  팔로워 무응답 장애 판정 시간 (기본 2,000ms, QAS-04)
 * @param clientTimeoutMs         클라이언트 제안 1회 대기 타임아웃 (기본 2,000ms)
 * @param maxBatchEntries         AppendEntries 1회 최대 엔트리 수 (Group Commit)
 */
public record RaftTimings(long heartbeatIntervalMs,
                          long electionTimeoutMinMs,
                          long electionTimeoutMaxMs,
                          long memberFailureTimeoutMs,
                          long clientTimeoutMs,
                          int maxBatchEntries) {

    public RaftTimings {
        if (heartbeatIntervalMs <= 0) throw new IllegalArgumentException("heartbeatIntervalMs must be > 0");
        if (electionTimeoutMinMs < heartbeatIntervalMs * 2) {
            throw new IllegalArgumentException("electionTimeoutMinMs must be >= 2 x heartbeatIntervalMs");
        }
        if (electionTimeoutMaxMs < electionTimeoutMinMs) {
            throw new IllegalArgumentException("electionTimeoutMaxMs must be >= electionTimeoutMinMs");
        }
        if (maxBatchEntries <= 0) throw new IllegalArgumentException("maxBatchEntries must be > 0");
    }

    public static RaftTimings defaults() {
        return new RaftTimings(500L, 1_500L, 2_000L, 2_000L, 2_000L, 256);
    }
}
