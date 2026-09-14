package com.sds.batchservice.cluster.fsm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * FSM apply 결과.
 *
 * <ul>
 *   <li>LOCK_ACQUIRE: ok=획득 여부, token=fenceToken, value=상태(GRANTED/QUEUED/DENIED)</li>
 *   <li>QUEUE_DEQUEUE / QUEUE_REASSIGN: ok=인출 여부, token=leaseToken, value=jobId</li>
 *   <li>기타: ok=반영 여부</li>
 * </ul>
 */
public final class CommandResult {
    private static final CommandResult OK = new CommandResult(true, 0L, null);
    private static final CommandResult REJECTED = new CommandResult(false, 0L, null);

    private final boolean ok;
    private final long token;
    private final String value;

    public CommandResult(boolean ok, long token, String value) {
        this.ok = ok;
        this.token = token;
        this.value = value;
    }

    public static CommandResult ok() { return OK; }
    public static CommandResult rejected() { return REJECTED; }
    public static CommandResult of(boolean ok) { return ok ? OK : REJECTED; }

    public boolean isOk() { return ok; }
    public long getToken() { return token; }
    public String getValue() { return value; }

    public void writeTo(DataOutput out) throws IOException {
        out.writeBoolean(ok);
        out.writeLong(token);
        Command.writeNullable(out, value);
    }

    public static CommandResult readFrom(DataInput in) throws IOException {
        return new CommandResult(in.readBoolean(), in.readLong(), Command.readNullable(in));
    }

    @Override
    public String toString() {
        return "CommandResult{ok=" + ok + ", token=" + token + ", value=" + value + '}';
    }
}
