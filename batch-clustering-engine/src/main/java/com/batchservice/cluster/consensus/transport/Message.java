package com.batchservice.cluster.consensus.transport;

import com.batchservice.cluster.fsm.Command;
import com.batchservice.cluster.fsm.CommandResult;
import com.batchservice.cluster.fsm.LogEntry;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 노드 간 RPC 메시지 (5.3.2 공통 런타임 규약 3).
 *
 * <p>Java 기본 직렬화를 사용하지 않고 {@link #writeTo(DataOutput)} / {@link #readFrom(DataInput)}의
 * 고정 필드 순서 바이너리 인코딩으로 교환합니다.
 */
public final class Message {

    public enum Type {
        PRE_VOTE, REQUEST_VOTE, VOTE_RESPONSE,
        APPEND_ENTRIES, APPEND_RESPONSE,
        CLIENT_REQUEST, CLIENT_RESPONSE
    }

    public enum ClientStatus { OK, NOT_LEADER, TIMEOUT }

    private Type type;
    private String senderId;
    private String receiverId;
    private long term;
    private boolean success;
    private boolean preVote;
    private long prevLogIndex;
    private long prevLogTerm;
    private long lastLogIndex;
    private long lastLogTerm;
    private long leaderCommit;
    private long matchIndex;
    private long conflictIndex;
    private List<LogEntry> entries = Collections.emptyList();
    private String rpcId;
    private List<Command> commands = Collections.emptyList();
    private ClientStatus clientStatus;
    private String leaderHint;
    private List<CommandResult> results = Collections.emptyList();

    private Message() {}

    private static Message base(Type type, String from, String to, long term) {
        Message m = new Message();
        m.type = type;
        m.senderId = from;
        m.receiverId = to;
        m.term = term;
        return m;
    }

    // ─── Factory methods ─────────────────────────────────────────────────────
    public static Message preVote(String from, String to, long proposedTerm, long lastLogIndex, long lastLogTerm) {
        Message m = base(Type.PRE_VOTE, from, to, proposedTerm);
        m.preVote = true;
        m.lastLogIndex = lastLogIndex;
        m.lastLogTerm = lastLogTerm;
        return m;
    }

    public static Message requestVote(String from, String to, long term, long lastLogIndex, long lastLogTerm) {
        Message m = base(Type.REQUEST_VOTE, from, to, term);
        m.lastLogIndex = lastLogIndex;
        m.lastLogTerm = lastLogTerm;
        return m;
    }

    public static Message voteResponse(String from, String to, long term, boolean granted, boolean preVote) {
        Message m = base(Type.VOTE_RESPONSE, from, to, term);
        m.success = granted;
        m.preVote = preVote;
        return m;
    }

    public static Message appendEntries(String from, String to, long term, long prevLogIndex, long prevLogTerm,
                                        List<LogEntry> entries, long leaderCommit) {
        Message m = base(Type.APPEND_ENTRIES, from, to, term);
        m.prevLogIndex = prevLogIndex;
        m.prevLogTerm = prevLogTerm;
        m.entries = List.copyOf(entries);
        m.leaderCommit = leaderCommit;
        return m;
    }

    public static Message appendResponse(String from, String to, long term, boolean success,
                                         long matchIndex, long conflictIndex) {
        Message m = base(Type.APPEND_RESPONSE, from, to, term);
        m.success = success;
        m.matchIndex = matchIndex;
        m.conflictIndex = conflictIndex;
        return m;
    }

    public static Message clientRequest(String from, String to, String rpcId, List<Command> commands) {
        Message m = base(Type.CLIENT_REQUEST, from, to, 0L);
        m.rpcId = rpcId;
        m.commands = List.copyOf(commands);
        return m;
    }

    public static Message clientResponse(String from, String to, String rpcId, ClientStatus status,
                                         String leaderHint, List<CommandResult> results) {
        Message m = base(Type.CLIENT_RESPONSE, from, to, 0L);
        m.rpcId = rpcId;
        m.clientStatus = status;
        m.leaderHint = leaderHint;
        m.results = results == null ? Collections.emptyList() : List.copyOf(results);
        return m;
    }

    // ─── Getters ──────────────────────────────────────────────────────────────
    public Type getType() { return type; }
    public String getSenderId() { return senderId; }
    public String getReceiverId() { return receiverId; }
    public long getTerm() { return term; }
    public boolean isSuccess() { return success; }
    public boolean isPreVote() { return preVote; }
    public long getPrevLogIndex() { return prevLogIndex; }
    public long getPrevLogTerm() { return prevLogTerm; }
    public long getLastLogIndex() { return lastLogIndex; }
    public long getLastLogTerm() { return lastLogTerm; }
    public long getLeaderCommit() { return leaderCommit; }
    public long getMatchIndex() { return matchIndex; }
    public long getConflictIndex() { return conflictIndex; }
    public List<LogEntry> getEntries() { return entries; }
    public String getRpcId() { return rpcId; }
    public List<Command> getCommands() { return commands; }
    public ClientStatus getClientStatus() { return clientStatus; }
    public String getLeaderHint() { return leaderHint; }
    public List<CommandResult> getResults() { return results; }

    // ─── Binary codec ─────────────────────────────────────────────────────────
    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(type.ordinal());
        writeStr(out, senderId);
        writeStr(out, receiverId);
        out.writeLong(term);
        out.writeBoolean(success);
        out.writeBoolean(preVote);
        out.writeLong(prevLogIndex);
        out.writeLong(prevLogTerm);
        out.writeLong(lastLogIndex);
        out.writeLong(lastLogTerm);
        out.writeLong(leaderCommit);
        out.writeLong(matchIndex);
        out.writeLong(conflictIndex);
        out.writeInt(entries.size());
        for (LogEntry e : entries) e.writeTo(out);
        writeStr(out, rpcId);
        out.writeInt(commands.size());
        for (Command c : commands) c.writeTo(out);
        out.writeByte(clientStatus == null ? -1 : clientStatus.ordinal());
        writeStr(out, leaderHint);
        out.writeInt(results.size());
        for (CommandResult r : results) r.writeTo(out);
    }

    public static Message readFrom(DataInput in) throws IOException {
        Message m = new Message();
        m.type = Type.values()[in.readUnsignedByte()];
        m.senderId = readStr(in);
        m.receiverId = readStr(in);
        m.term = in.readLong();
        m.success = in.readBoolean();
        m.preVote = in.readBoolean();
        m.prevLogIndex = in.readLong();
        m.prevLogTerm = in.readLong();
        m.lastLogIndex = in.readLong();
        m.lastLogTerm = in.readLong();
        m.leaderCommit = in.readLong();
        m.matchIndex = in.readLong();
        m.conflictIndex = in.readLong();
        int n = in.readInt();
        List<LogEntry> entries = new ArrayList<>(n);
        for (int i = 0; i < n; i++) entries.add(LogEntry.readFrom(in));
        m.entries = entries;
        m.rpcId = readStr(in);
        n = in.readInt();
        List<Command> commands = new ArrayList<>(n);
        for (int i = 0; i < n; i++) commands.add(Command.readFrom(in));
        m.commands = commands;
        byte status = in.readByte();
        m.clientStatus = status < 0 ? null : ClientStatus.values()[status];
        m.leaderHint = readStr(in);
        n = in.readInt();
        List<CommandResult> results = new ArrayList<>(n);
        for (int i = 0; i < n; i++) results.add(CommandResult.readFrom(in));
        m.results = results;
        return m;
    }

    private static void writeStr(DataOutput out, String s) throws IOException {
        out.writeBoolean(s != null);
        if (s != null) out.writeUTF(s);
    }

    private static String readStr(DataInput in) throws IOException {
        return in.readBoolean() ? in.readUTF() : null;
    }

    @Override
    public String toString() {
        return "Message{" + type + " " + senderId + "->" + receiverId + " term=" + term
                + (entries.isEmpty() ? "" : " entries=" + entries.size()) + '}';
    }
}
