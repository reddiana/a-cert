package com.batchservice.cluster.fsm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 클라이언트가 제안하는 FSM 명령 (인덱스·term 할당 전).
 *
 * <p>{@code requestId}는 재시도 시에도 동일하게 유지되어 FSM에서 중복 반영을 제거합니다.
 */
public final class Command {
    public static final String KEY = "key";
    public static final String OWNER = "owner";
    public static final String LEASE_MS = "leaseMs";
    public static final String WAIT_MS = "waitMs";
    public static final String TOKEN = "token";
    public static final String JOB_ID = "jobId";
    public static final String WORKER = "worker";
    public static final String VISIBILITY_MS = "visibilityMs";
    public static final String FRONT = "front";
    public static final String EXECUTION_ID = "executionId";
    public static final String STATUS = "status";
    public static final String NODE = "node";
    public static final String ALIVE = "alive";
    public static final String CHECKPOINT = "checkpoint";
    public static final String REASON = "reason";

    private final CommandType type;
    private final String requestId;
    private final Map<String, String> attrs;

    private Command(CommandType type, String requestId, Map<String, String> attrs) {
        this.type = type;
        this.requestId = requestId;
        this.attrs = Collections.unmodifiableMap(new LinkedHashMap<>(attrs));
    }

    public static Builder builder(CommandType type) {
        return new Builder(type);
    }

    public CommandType getType() { return type; }
    public String getRequestId() { return requestId; }
    public Map<String, String> getAttrs() { return attrs; }

    public String attr(String name) { return attrs.get(name); }

    public long longAttr(String name, long defaultValue) {
        String v = attrs.get(name);
        return v == null ? defaultValue : Long.parseLong(v);
    }

    public boolean boolAttr(String name) {
        return Boolean.parseBoolean(attrs.get(name));
    }

    // ─── Factory methods ─────────────────────────────────────────────────────
    public static Command noOp() {
        return builder(CommandType.NO_OP).build();
    }

    public static Command lockAcquire(String key, String owner, long leaseMs, long waitMs) {
        return builder(CommandType.LOCK_ACQUIRE).put(KEY, key).put(OWNER, owner)
                .put(LEASE_MS, leaseMs).put(WAIT_MS, waitMs).build();
    }

    public static Command lockRenew(String key, long fenceToken, long leaseMs) {
        return builder(CommandType.LOCK_RENEW).put(KEY, key).put(TOKEN, fenceToken).put(LEASE_MS, leaseMs).build();
    }

    public static Command lockRelease(String key, long fenceToken) {
        return builder(CommandType.LOCK_RELEASE).put(KEY, key).put(TOKEN, fenceToken).build();
    }

    public static Command lockExpire(String key, long fenceToken) {
        return builder(CommandType.LOCK_EXPIRE).put(KEY, key).put(TOKEN, fenceToken).build();
    }

    public static Command lockCancel(String key, String owner) {
        return builder(CommandType.LOCK_CANCEL).put(KEY, key).put(OWNER, owner).build();
    }

    public static Command enqueue(String jobId, boolean front) {
        return builder(CommandType.QUEUE_ENQUEUE).put(JOB_ID, jobId).put(FRONT, front).build();
    }

    public static Command dequeue(String workerId, long visibilityMs) {
        return builder(CommandType.QUEUE_DEQUEUE).put(WORKER, workerId).put(VISIBILITY_MS, visibilityMs).build();
    }

    public static Command queueRenew(String jobId, long leaseToken, long visibilityMs) {
        return builder(CommandType.QUEUE_RENEW).put(JOB_ID, jobId).put(TOKEN, leaseToken)
                .put(VISIBILITY_MS, visibilityMs).build();
    }

    public static Command queueAck(String jobId, long leaseToken) {
        return builder(CommandType.QUEUE_ACK).put(JOB_ID, jobId).put(TOKEN, leaseToken).build();
    }

    public static Command requeue(String jobId, long leaseToken, boolean front, String reason) {
        return builder(CommandType.QUEUE_REQUEUE).put(JOB_ID, jobId).put(TOKEN, leaseToken)
                .put(FRONT, front).put(REASON, reason).build();
    }

    public static Command reassign(String jobId, long leaseToken, String newWorkerId, long visibilityMs) {
        return builder(CommandType.QUEUE_REASSIGN).put(JOB_ID, jobId).put(TOKEN, leaseToken)
                .put(WORKER, newWorkerId).put(VISIBILITY_MS, visibilityMs).build();
    }

    public static Command journal(String executionId, String jobId, String status, String executorNode, long leaseToken) {
        return builder(CommandType.JOURNAL_RECORD).put(EXECUTION_ID, executionId).put(JOB_ID, jobId)
                .put(STATUS, status).put(NODE, executorNode).put(TOKEN, leaseToken).build();
    }

    public static Command memberStatus(String nodeId, boolean alive) {
        return builder(CommandType.MEMBER_STATUS).put(NODE, nodeId).put(ALIVE, alive).build();
    }

    public static Command syncCheckpoint(long lastDbSyncedIndex) {
        return builder(CommandType.SYNC_CHECKPOINT).put(CHECKPOINT, lastDbSyncedIndex).build();
    }

    // ─── Binary codec (WAL / Transport 공용) ──────────────────────────────────
    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(type.ordinal());
        writeNullable(out, requestId);
        out.writeShort(attrs.size());
        for (Map.Entry<String, String> e : attrs.entrySet()) {
            out.writeUTF(e.getKey());
            writeNullable(out, e.getValue());
        }
    }

    public static Command readFrom(DataInput in) throws IOException {
        CommandType type = CommandType.values()[in.readUnsignedByte()];
        String requestId = readNullable(in);
        int n = in.readUnsignedShort();
        Map<String, String> attrs = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            attrs.put(in.readUTF(), readNullable(in));
        }
        return new Command(type, requestId, attrs);
    }

    static void writeNullable(DataOutput out, String s) throws IOException {
        out.writeBoolean(s != null);
        if (s != null) out.writeUTF(s);
    }

    static String readNullable(DataInput in) throws IOException {
        return in.readBoolean() ? in.readUTF() : null;
    }

    @Override
    public String toString() {
        return type.name() + attrs + (requestId != null ? "#" + requestId : "");
    }

    public static final class Builder {
        private final CommandType type;
        private String requestId = UUID.randomUUID().toString();
        private final Map<String, String> attrs = new LinkedHashMap<>();

        private Builder(CommandType type) { this.type = type; }

        public Builder requestId(String requestId) { this.requestId = requestId; return this; }
        public Builder put(String k, String v) { if (v != null) attrs.put(k, v); return this; }
        public Builder put(String k, long v) { attrs.put(k, Long.toString(v)); return this; }
        public Builder put(String k, boolean v) { attrs.put(k, Boolean.toString(v)); return this; }
        public Command build() { return new Command(type, requestId, attrs); }
    }
}
