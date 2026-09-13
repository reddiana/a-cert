package com.batchservice.cluster;

import com.batchservice.cluster.consensus.RaftTimings;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 클러스터 설정 ({@code batch.cluster.*}).
 *
 * <p>값이 비어 있으면 5.4.2 매니페스트의 환경변수로 대체합니다:
 * {@code RAFT_NODE_ID}(없으면 {@code POD_NAME}), {@code RAFT_PORT}, {@code RAFT_PEERS}, {@code WAL_DATA_DIR},
 * {@code RAFT_AUTH_TOKEN}.
 *
 * <p>peers 항목은 {@code "nodeId=host:port"} 또는 {@code "host:port"} 형식을 모두 허용합니다. 후자는
 * 호스트의 첫 DNS 레이블(예: {@code batch-scheduler-0.batch-service...} → {@code batch-scheduler-0})을 nodeId로
 * 사용하며, 자기 자신이 포함되어 있어도 됩니다.
 */
@ConfigurationProperties(prefix = "batch.cluster")
public class BatchClusterProperties {

    private boolean enabled = true;
    private String nodeId;
    private Integer port;
    private List<String> peers = new ArrayList<>();
    private String walDir;
    private boolean walFsync = true;
    private String authToken;
    /** peers 없이 단일 노드로 기동 허용 (개발/테스트 전용). 운영에서 false 유지 → 설정 누락 시 Split-Brain 방지. */
    private boolean allowSingleNode = false;
    private String clusterId = "batch-cluster";

    private long heartbeatIntervalMs = 500L;
    private long electionTimeoutMinMs = 1_500L;
    private long electionTimeoutMaxMs = 2_000L;
    private long memberFailureTimeoutMs = 2_000L;
    private long clientTimeoutMs = 2_000L;
    private int maxBatchEntries = 256;

    private long queueVisibilityTimeoutMs = 30_000L;
    private long recoveryScanIntervalMs = 1_000L;

    private final Sync sync = new Sync();

    public static class Sync {
        private boolean enabled = true;
        private boolean initializeSchema = true;
        private int batchSize = 500;
        private long intervalMs = 200L;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public boolean isInitializeSchema() { return initializeSchema; }
        public void setInitializeSchema(boolean initializeSchema) { this.initializeSchema = initializeSchema; }
        public int getBatchSize() { return batchSize; }
        public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
        public long getIntervalMs() { return intervalMs; }
        public void setIntervalMs(long intervalMs) { this.intervalMs = intervalMs; }
    }

    // ─── 해석 (설정값 → 환경변수 → 기본값) ────────────────────────────────────
    public String resolveNodeId() {
        return firstNonBlank(nodeId, env("RAFT_NODE_ID"), env("POD_NAME"));
    }

    public int resolvePort() {
        if (port != null) return port;
        String p = env("RAFT_PORT");
        return isBlank(p) ? 7800 : Integer.parseInt(p.trim());
    }

    public String resolveWalDir() {
        return firstNonBlank(walDir, env("WAL_DATA_DIR"), "./data/wal");
    }

    public String resolveAuthToken() {
        return firstNonBlank(authToken, env("RAFT_AUTH_TOKEN"));
    }

    public List<String> resolvePeerSpecs() {
        if (peers != null && !peers.isEmpty()) return peers;
        String raw = env("RAFT_PEERS");
        return isBlank(raw) ? List.of() : Arrays.stream(raw.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
    }

    /** 로컬 노드를 제외한 nodeId → host:port. */
    public Map<String, String> resolvePeerHostMap() {
        String self = resolveNodeId();
        Map<String, String> map = new LinkedHashMap<>();
        for (String spec : resolvePeerSpecs()) {
            String id;
            String hostPort;
            int eq = spec.indexOf('=');
            if (eq > 0) {
                id = spec.substring(0, eq).trim();
                hostPort = spec.substring(eq + 1).trim();
            } else {
                hostPort = spec;
                String host = spec.substring(0, spec.lastIndexOf(':'));
                id = host.contains(".") ? host.substring(0, host.indexOf('.')) : host;
            }
            if (!id.equals(self)) {
                map.put(id, hostPort);
            }
        }
        return map;
    }

    public List<String> resolveAllNodeIds() {
        List<String> ids = new ArrayList<>();
        ids.add(resolveNodeId());
        ids.addAll(resolvePeerHostMap().keySet());
        return ids;
    }

    public RaftTimings toTimings() {
        return new RaftTimings(heartbeatIntervalMs, electionTimeoutMinMs, electionTimeoutMaxMs,
                memberFailureTimeoutMs, clientTimeoutMs, maxBatchEntries);
    }

    /** 기동 전 설정 검증 (Fail-fast). */
    public void validate() {
        if (isBlank(resolveNodeId())) {
            throw new IllegalStateException("batch.cluster.node-id (or RAFT_NODE_ID / POD_NAME) must be set.");
        }
        Map<String, String> peerMap = resolvePeerHostMap();
        if (peerMap.isEmpty() && !allowSingleNode) {
            throw new IllegalStateException("batch.cluster.peers (or RAFT_PEERS) is empty. Refusing to start as a "
                    + "single-node cluster to prevent split-brain. Set batch.cluster.allow-single-node=true only for development.");
        }
        if (!peerMap.isEmpty() && isBlank(resolveAuthToken())) {
            throw new IllegalStateException("batch.cluster.auth-token (or RAFT_AUTH_TOKEN) must be set for multi-node clusters.");
        }
        toTimings();
    }

    protected String env(String name) {
        return System.getenv(name);
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

    private static String firstNonBlank(String... values) {
        for (String v : values) {
            if (!isBlank(v)) return v.trim();
        }
        return null;
    }

    // ─── Getters / Setters ────────────────────────────────────────────────────
    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }
    public Integer getPort() { return port; }
    public void setPort(Integer port) { this.port = port; }
    public List<String> getPeers() { return peers; }
    public void setPeers(List<String> peers) { this.peers = peers; }
    public String getWalDir() { return walDir; }
    public void setWalDir(String walDir) { this.walDir = walDir; }
    public boolean isWalFsync() { return walFsync; }
    public void setWalFsync(boolean walFsync) { this.walFsync = walFsync; }
    public String getAuthToken() { return authToken; }
    public void setAuthToken(String authToken) { this.authToken = authToken; }
    public boolean isAllowSingleNode() { return allowSingleNode; }
    public void setAllowSingleNode(boolean allowSingleNode) { this.allowSingleNode = allowSingleNode; }
    public String getClusterId() { return clusterId; }
    public void setClusterId(String clusterId) { this.clusterId = clusterId; }
    public long getHeartbeatIntervalMs() { return heartbeatIntervalMs; }
    public void setHeartbeatIntervalMs(long v) { this.heartbeatIntervalMs = v; }
    public long getElectionTimeoutMinMs() { return electionTimeoutMinMs; }
    public void setElectionTimeoutMinMs(long v) { this.electionTimeoutMinMs = v; }
    public long getElectionTimeoutMaxMs() { return electionTimeoutMaxMs; }
    public void setElectionTimeoutMaxMs(long v) { this.electionTimeoutMaxMs = v; }
    public long getMemberFailureTimeoutMs() { return memberFailureTimeoutMs; }
    public void setMemberFailureTimeoutMs(long v) { this.memberFailureTimeoutMs = v; }
    public long getClientTimeoutMs() { return clientTimeoutMs; }
    public void setClientTimeoutMs(long v) { this.clientTimeoutMs = v; }
    public int getMaxBatchEntries() { return maxBatchEntries; }
    public void setMaxBatchEntries(int v) { this.maxBatchEntries = v; }
    public long getQueueVisibilityTimeoutMs() { return queueVisibilityTimeoutMs; }
    public void setQueueVisibilityTimeoutMs(long v) { this.queueVisibilityTimeoutMs = v; }
    public long getRecoveryScanIntervalMs() { return recoveryScanIntervalMs; }
    public void setRecoveryScanIntervalMs(long v) { this.recoveryScanIntervalMs = v; }
    public Sync getSync() { return sync; }
}
