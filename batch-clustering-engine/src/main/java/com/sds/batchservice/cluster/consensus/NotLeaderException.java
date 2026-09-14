package com.sds.batchservice.cluster.consensus;

/**
 * 요청을 받은 노드가 리더가 아니거나 커밋 전에 리더십을 잃은 경우.
 * 클라이언트는 동일 requestId로 재시도하며 FSM의 멱등성으로 중복 반영이 방지됩니다.
 */
public class NotLeaderException extends RuntimeException {
    private final String leaderHint;

    public NotLeaderException(String leaderHint) {
        super("Not leader" + (leaderHint != null ? " (leaderHint=" + leaderHint + ")" : ""));
        this.leaderHint = leaderHint;
    }

    public String getLeaderHint() {
        return leaderHint;
    }
}
