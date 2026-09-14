package com.sds.batchservice.cluster.fsm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Raft 로그 엔트리: {@code { index, term, proposedAt, command }}.
 *
 * <p>FSM은 로컬 시계 대신 리더가 기록한 {@code proposedAt}만 사용하여 결정론적으로 apply합니다.
 */
public final class LogEntry {
    private final long index;
    private final long term;
    private final long proposedAt;
    private final Command command;

    public LogEntry(long index, long term, long proposedAt, Command command) {
        this.index = index;
        this.term = term;
        this.proposedAt = proposedAt;
        this.command = command;
    }

    public long getIndex() { return index; }
    public long getTerm() { return term; }
    public long getProposedAt() { return proposedAt; }
    public Command getCommand() { return command; }
    public CommandType getType() { return command.getType(); }

    public void writeTo(DataOutput out) throws IOException {
        out.writeLong(index);
        out.writeLong(term);
        out.writeLong(proposedAt);
        command.writeTo(out);
    }

    public static LogEntry readFrom(DataInput in) throws IOException {
        long index = in.readLong();
        long term = in.readLong();
        long proposedAt = in.readLong();
        return new LogEntry(index, term, proposedAt, Command.readFrom(in));
    }

    @Override
    public String toString() {
        return "LogEntry{index=" + index + ", term=" + term + ", " + command + '}';
    }
}
