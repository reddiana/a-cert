package com.batchservice.cluster.health;

import com.batchservice.cluster.consensus.RaftNode;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * Raft Quorum 참여 상태 HealthIndicator (5.4.2 Readiness Probe).
 *
 * <p>리더가 알려져 있고, 리더면 현재 term NO_OP 커밋 완료, 팔로워면 커밋된 멤버십상 ALIVE일 때 UP.
 */
public class RaftHealthIndicator implements HealthIndicator {

    private final RaftNode raftNode;

    public RaftHealthIndicator(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public Health health() {
        Health.Builder builder = raftNode.isReady() ? Health.up() : Health.outOfService();
        return builder
                .withDetail("nodeId", raftNode.getNodeId())
                .withDetail("role", raftNode.getRole().name())
                .withDetail("term", raftNode.getCurrentTerm())
                .withDetail("leaderId", String.valueOf(raftNode.getLeaderId()))
                .withDetail("commitIndex", raftNode.getCommitIndex())
                .withDetail("lastApplied", raftNode.getLastApplied())
                .withDetail("activeMembers", raftNode.getActiveMembers())
                .build();
    }
}
