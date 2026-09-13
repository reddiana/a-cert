package com.batchservice.cluster.consensus;

import com.batchservice.cluster.consensus.transport.Message;
import com.batchservice.cluster.consensus.transport.NetworkTransport;
import com.batchservice.cluster.fsm.ClusterStateMachine;
import com.batchservice.cluster.fsm.Command;
import com.batchservice.cluster.fsm.CommandResult;
import com.batchservice.cluster.fsm.LogEntry;
import com.batchservice.cluster.storage.RaftLogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Raft 합의 엔진 (DD-01, 5.4 ConsensusEngine).
 *
 * <p>5.3.2 공통 런타임 규약의 안전성 불변식을 구현합니다.
 * <ol>
 *   <li>영속 후 계수: 리더/팔로워 모두 WAL fsync 이후에만 복제 성공으로 계수</li>
 *   <li>커밋 규칙: 과반수 matchIndex ≥ N && log[N].term == currentTerm</li>
 *   <li>선반영 금지: 모든 명령(큐 인출 포함)은 커밋 후 apply 결과만 반환</li>
 *   <li>투표 영속화 및 로그 최신성 검사 (PreVote / RequestVote)</li>
 *   <li>Quorum 분모 고정, Check-Quorum 기반 리더 자진 강등</li>
 * </ol>
 */
public class RaftNode {
    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    private final String nodeId;
    private final List<String> allNodeIds;
    private final List<String> peers;
    private final NetworkTransport transport;
    private final RaftLogManager raftLog;
    private final ClusterStateMachine fsm;
    private final RaftTimings timings;
    private final Random random = new Random();

    /** Raft 상태 보호 락. 순서: lock → fsm → raftLog */
    private final Object lock = new Object();

    // ─── 영속 상태 (RaftLogManager에 fsync 저장) ───────────────────────────────
    private long currentTerm;
    private String votedFor;

    // ─── 휘발 상태 ────────────────────────────────────────────────────────────
    private RaftRole role = RaftRole.FOLLOWER;
    private String leaderId;
    private long commitIndex;
    private long lastApplied;
    private long electionDeadline;
    private final Map<String, Long> lastHeardFrom = new HashMap<>();

    // ─── 선출 상태 ────────────────────────────────────────────────────────────
    private long preVoteTerm;
    private final Set<String> preVoteGrants = new HashSet<>();
    private final Set<String> votesGranted = new HashSet<>();

    // ─── 리더 상태 ────────────────────────────────────────────────────────────
    private long leaderSince;
    private long lastHeartbeatSent;
    private long noOpIndex;
    private boolean leadershipReady;
    private long selfDurableIndex;
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();
    private final Map<String, Long> sentUpTo = new HashMap<>();
    private final Map<String, Long> lastSentCommit = new HashMap<>();
    private final Map<String, Long> lastAck = new HashMap<>();

    // ─── 제안/전달 상태 ───────────────────────────────────────────────────────
    private final Map<Long, CompletableFuture<CommandResult>> pendingByIndex = new HashMap<>();
    private final Map<String, CompletableFuture<List<CommandResult>>> forwarded = new ConcurrentHashMap<>();
    private final Set<String> pendingMemberProposals = ConcurrentHashMap.newKeySet();
    private final List<Consumer<Long>> leadershipListeners = new CopyOnWriteArrayList<>();

    // ─── 스레드 ───────────────────────────────────────────────────────────────
    private volatile boolean running;
    private ScheduledExecutorService ticker;
    private ExecutorService callbackExecutor;
    private Thread replicatorThread;
    private final Semaphore replicateSignal = new Semaphore(0);
    private long lastCommitPersistAt;

    public RaftNode(String nodeId, List<String> allNodeIds, NetworkTransport transport,
                    RaftLogManager raftLog, ClusterStateMachine fsm, RaftTimings timings) {
        if (!allNodeIds.contains(nodeId)) {
            throw new IllegalArgumentException("allNodeIds must contain local nodeId " + nodeId);
        }
        this.nodeId = nodeId;
        this.allNodeIds = List.copyOf(allNodeIds);
        this.peers = this.allNodeIds.stream().filter(id -> !id.equals(nodeId)).toList();
        this.transport = transport;
        this.raftLog = raftLog;
        this.fsm = fsm;
        this.timings = timings;
        this.currentTerm = raftLog.getCurrentTerm();
        this.votedFor = raftLog.getVotedFor();
        this.commitIndex = raftLog.getPersistedCommitIndex();
    }

    // ─── 라이프사이클 ─────────────────────────────────────────────────────────
    public void start() {
        synchronized (lock) {
            if (running) return;
            running = true;
            long now = System.currentTimeMillis();
            // 기동 시 WAL Replay: 영속화된 commitIndex까지 FSM 복원 (Scenario 3)
            applyLocked(new ArrayList<>());
            for (String p : peers) {
                lastHeardFrom.put(p, now);
            }
            role = RaftRole.FOLLOWER;
            resetElectionTimerLocked(now);
            if (peers.isEmpty()) {
                electionDeadline = now; // 단일 노드(개발 모드): 즉시 자기 선출
            }
            lastCommitPersistAt = now;
        }
        transport.registerReceiver(nodeId, this::handleMessage);
        callbackExecutor = Executors.newSingleThreadExecutor(r -> daemon(r, "raft-callback-" + nodeId));
        ticker = Executors.newSingleThreadScheduledExecutor(r -> daemon(r, "raft-ticker-" + nodeId));
        ticker.scheduleAtFixedRate(this::safeTick, 20, 20, TimeUnit.MILLISECONDS);
        replicatorThread = daemon(this::replicatorLoop, "raft-replicator-" + nodeId);
        replicatorThread.start();
        log.info("[{}] RaftNode started (term={}, lastIndex={}, commitIndex={}, peers={}).",
                nodeId, currentTerm, raftLog.lastIndex(), commitIndex, peers);
    }

    /** 정상 종료: 스레드 정지 후 commitIndex 영속화. */
    public void stop() {
        halt();
        raftLog.saveCommitIndex(getCommitIndex());
        log.info("[{}] RaftNode stopped.", nodeId);
    }

    /** 비정상 종료 시뮬레이션: 영속화 없이 즉시 정지. */
    public void crash() {
        halt();
        log.warn("[{}] RaftNode CRASHED.", nodeId);
    }

    private void halt() {
        synchronized (lock) {
            if (!running) return;
            running = false;
            role = RaftRole.FOLLOWER;
            failPendingLocked(new NotLeaderException(null));
        }
        transport.unregisterReceiver(nodeId);
        if (ticker != null) ticker.shutdownNow();
        if (replicatorThread != null) replicatorThread.interrupt();
        if (callbackExecutor != null) callbackExecutor.shutdownNow();
        forwarded.values().forEach(f -> f.completeExceptionally(new NotLeaderException(null)));
        forwarded.clear();
    }

    // ─── 타이머 ───────────────────────────────────────────────────────────────
    private void safeTick() {
        try {
            tick();
        } catch (RuntimeException e) {
            log.error("[{}] tick failed", nodeId, e);
        }
    }

    private void tick() {
        long now = System.currentTimeMillis();
        List<Runnable> after = new ArrayList<>();
        long commitToPersist = -1;
        synchronized (lock) {
            if (!running) return;
            if (role == RaftRole.LEADER) {
                checkQuorumLocked(now);
            }
            if (role == RaftRole.LEADER) {
                if (now - lastHeartbeatSent >= timings.heartbeatIntervalMs()) {
                    lastHeartbeatSent = now;
                    for (String p : peers) {
                        sentUpTo.put(p, matchIndex.get(p)); // 미확인 구간 재전송
                        replicateToLocked(p, true);
                    }
                }
                if (leadershipReady) {
                    detectMemberChangesLocked(now, after);
                }
            } else if (now >= electionDeadline) {
                startPreVoteLocked(now);
            }
            if (now - lastCommitPersistAt >= 1_000L) {
                lastCommitPersistAt = now;
                commitToPersist = commitIndex;
            }
        }
        if (commitToPersist > 0) {
            raftLog.saveCommitIndex(commitToPersist);
        }
        after.forEach(Runnable::run);
    }

    /** Check-Quorum: Election Timeout 동안 과반수 응답이 없으면 리더 자진 강등 (Scenario 5). */
    private void checkQuorumLocked(long now) {
        if (now - leaderSince < timings.electionTimeoutMinMs()) return;
        int reachable = 1;
        for (String p : peers) {
            if (now - lastAck.getOrDefault(p, 0L) < timings.electionTimeoutMinMs()) reachable++;
        }
        if (reachable < quorum()) {
            log.warn("[{}] Check-Quorum failed ({}/{} reachable). Stepping down to FOLLOWER.",
                    nodeId, reachable, allNodeIds.size());
            becomeFollowerLocked(currentTerm, null);
            resetElectionTimerLocked(now);
        }
    }

    /** 팔로워 무응답 2초 → DEAD, 따라잡기 완료 → ALIVE 를 로그로 커밋 (Scenario 3, QAS-04). */
    private void detectMemberChangesLocked(long now, List<Runnable> after) {
        for (String p : peers) {
            boolean unresponsive = now - lastAck.getOrDefault(p, 0L) >= timings.memberFailureTimeoutMs();
            boolean markedDead = !fsm.isAlive(p);
            if (unresponsive && !markedDead && pendingMemberProposals.add(p)) {
                log.warn("[{}] No AppendResponse from [{}] for {}ms. Proposing MEMBER_STATUS DEAD.",
                        nodeId, p, now - lastAck.getOrDefault(p, 0L));
                after.add(() -> proposeMemberStatus(p, false));
            } else if (!unresponsive && markedDead && matchIndex.getOrDefault(p, 0L) >= commitIndex
                    && pendingMemberProposals.add(p)) {
                log.info("[{}] Node [{}] caught up (matchIndex={}). Proposing MEMBER_STATUS ALIVE.",
                        nodeId, p, matchIndex.get(p));
                after.add(() -> proposeMemberStatus(p, true));
            }
        }
    }

    private void proposeMemberStatus(String peer, boolean alive) {
        propose(List.of(Command.memberStatus(peer, alive)))
                .whenComplete((r, e) -> pendingMemberProposals.remove(peer));
    }

    // ─── 리더 선출 ────────────────────────────────────────────────────────────
    private void startPreVoteLocked(long now) {
        resetElectionTimerLocked(now);
        if (peers.isEmpty()) {
            startElectionLocked(now);
            return;
        }
        preVoteTerm = currentTerm + 1;
        preVoteGrants.clear();
        preVoteGrants.add(nodeId);
        long lastIdx = raftLog.lastIndex();
        long lastTerm = raftLog.lastTerm();
        log.debug("[{}] Election timeout. Sending PreVote for term {}.", nodeId, preVoteTerm);
        for (String p : peers) {
            transport.send(Message.preVote(nodeId, p, preVoteTerm, lastIdx, lastTerm));
        }
    }

    private void startElectionLocked(long now) {
        currentTerm++;
        votedFor = nodeId;
        raftLog.saveHardState(currentTerm, votedFor);
        role = RaftRole.CANDIDATE;
        leaderId = null;
        preVoteTerm = 0;
        votesGranted.clear();
        votesGranted.add(nodeId);
        resetElectionTimerLocked(now);
        log.info("[{}] Started Leader Election for term {}.", nodeId, currentTerm);
        if (votesGranted.size() >= quorum()) {
            becomeLeaderLocked(now);
            return;
        }
        long lastIdx = raftLog.lastIndex();
        long lastTerm = raftLog.lastTerm();
        for (String p : peers) {
            transport.send(Message.requestVote(nodeId, p, currentTerm, lastIdx, lastTerm));
        }
    }

    private void handlePreVoteLocked(Message m, long now) {
        boolean leaderAlive = role == RaftRole.LEADER
                || (leaderId != null && now - lastHeardFrom.getOrDefault(leaderId, 0L) < timings.electionTimeoutMinMs());
        boolean granted = m.getTerm() > currentTerm && !leaderAlive
                && isUpToDateLocked(m.getLastLogIndex(), m.getLastLogTerm());
        transport.send(Message.voteResponse(nodeId, m.getSenderId(), m.getTerm(), granted, true));
    }

    private void handleRequestVoteLocked(Message m, long now) {
        boolean granted = false;
        if (m.getTerm() == currentTerm
                && (votedFor == null || votedFor.equals(m.getSenderId()))
                && isUpToDateLocked(m.getLastLogIndex(), m.getLastLogTerm())) {
            if (votedFor == null) {
                votedFor = m.getSenderId();
                raftLog.saveHardState(currentTerm, votedFor); // 투표 응답 전 fsync
            }
            granted = true;
            resetElectionTimerLocked(now);
        }
        transport.send(Message.voteResponse(nodeId, m.getSenderId(), currentTerm, granted, false));
    }

    private void handleVoteResponseLocked(Message m, long now) {
        if (m.isPreVote()) {
            if (role != RaftRole.LEADER && preVoteTerm == currentTerm + 1
                    && m.getTerm() == preVoteTerm && m.isSuccess()) {
                preVoteGrants.add(m.getSenderId());
                if (preVoteGrants.size() >= quorum()) {
                    startElectionLocked(now);
                }
            }
            return;
        }
        if (role == RaftRole.CANDIDATE && m.getTerm() == currentTerm && m.isSuccess()) {
            votesGranted.add(m.getSenderId());
            log.info("[{}] Received vote from [{}] ({}/{}).", nodeId, m.getSenderId(), votesGranted.size(), quorum());
            if (votesGranted.size() >= quorum()) {
                becomeLeaderLocked(now);
            }
        }
    }

    /** 후보 로그가 자신보다 같거나 최신인지 (lastLogTerm 우선, 같으면 lastLogIndex). */
    private boolean isUpToDateLocked(long candidateLastIndex, long candidateLastTerm) {
        long myLastTerm = raftLog.lastTerm();
        return candidateLastTerm > myLastTerm
                || (candidateLastTerm == myLastTerm && candidateLastIndex >= raftLog.lastIndex());
    }

    private void becomeLeaderLocked(long now) {
        role = RaftRole.LEADER;
        leaderId = nodeId;
        leaderSince = now;
        leadershipReady = false;
        selfDurableIndex = 0L;
        long last = raftLog.lastIndex();
        for (String p : peers) {
            nextIndex.put(p, last + 1);
            matchIndex.put(p, 0L);
            sentUpTo.put(p, last);
            lastSentCommit.put(p, -1L);
            lastAck.put(p, lastHeardFrom.getOrDefault(p, now));
        }
        log.info("[{}] Promoted to LEADER for term {} (lastIndex={}).", nodeId, currentTerm, last);
        // 현재 term의 NO_OP 커밋으로 이전 term 엔트리 확정 (Scenario 2 3단계)
        noOpIndex = last + 1;
        appendLocked(List.of(Command.noOp()), now);
        lastHeartbeatSent = now;
        for (String p : peers) {
            replicateToLocked(p, true);
        }
        replicateSignal.release();
    }

    private void becomeFollowerLocked(long term, String newLeaderId) {
        boolean wasLeader = role == RaftRole.LEADER;
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;
            raftLog.saveHardState(currentTerm, null);
        }
        role = RaftRole.FOLLOWER;
        leaderId = newLeaderId;
        leadershipReady = false;
        preVoteTerm = 0;
        if (wasLeader) {
            log.warn("[{}] Stepped down from LEADER (term={}).", nodeId, currentTerm);
            failPendingLocked(new NotLeaderException(newLeaderId));
        }
    }

    private void resetElectionTimerLocked(long now) {
        long span = timings.electionTimeoutMaxMs() - timings.electionTimeoutMinMs();
        electionDeadline = now + timings.electionTimeoutMinMs() + (span > 0 ? random.nextInt((int) span + 1) : 0);
    }

    private int quorum() {
        return allNodeIds.size() / 2 + 1;
    }

    // ─── 메시지 디스패치 ──────────────────────────────────────────────────────
    public void handleMessage(Message m) {
        if (!running) return;
        switch (m.getType()) {
            case CLIENT_REQUEST -> {
                handleClientRequest(m);
                return;
            }
            case CLIENT_RESPONSE -> {
                handleClientResponse(m);
                return;
            }
            default -> {
            }
        }
        Message deferredResponse = null;
        boolean needsSync = false;
        List<Runnable> completions = new ArrayList<>();
        synchronized (lock) {
            if (!running) return;
            long now = System.currentTimeMillis();
            lastHeardFrom.put(m.getSenderId(), now);
            boolean preVoteMessage = m.isPreVote();
            if (!preVoteMessage && m.getTerm() > currentTerm) {
                becomeFollowerLocked(m.getTerm(), m.getType() == Message.Type.APPEND_ENTRIES ? m.getSenderId() : null);
            }
            switch (m.getType()) {
                case PRE_VOTE -> handlePreVoteLocked(m, now);
                case REQUEST_VOTE -> handleRequestVoteLocked(m, now);
                case VOTE_RESPONSE -> handleVoteResponseLocked(m, now);
                case APPEND_ENTRIES -> {
                    AppendOutcome outcome = handleAppendEntriesLocked(m, now);
                    deferredResponse = outcome.response();
                    needsSync = outcome.needsSync();
                }
                case APPEND_RESPONSE -> handleAppendResponseLocked(m, now, completions);
                default -> {
                }
            }
        }
        if (needsSync) {
            raftLog.sync(); // 팔로워: fsync 완료 후 ACK (불변식 1)
        }
        if (deferredResponse != null) {
            transport.send(deferredResponse);
        }
        completions.forEach(Runnable::run);
    }

    // ─── 클라이언트 제안 API ──────────────────────────────────────────────────

    /**
     * 명령을 과반수 합의로 커밋하고 apply 결과를 반환합니다.
     * 로컬 노드가 팔로워면 리더로 전달(ClientRequest)하며, 리더 교체·타임아웃 시 동일 requestId로 재시도합니다.
     *
     * @throws ClusterUnavailableException timeoutMs 내에 커밋하지 못한 경우
     */
    public List<CommandResult> execute(List<Command> commands, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        Throwable lastError = null;
        while (running) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) break;
            String target;
            synchronized (lock) {
                target = role == RaftRole.LEADER ? nodeId : leaderId;
            }
            if (target == null) {
                sleepQuietly(Math.min(remaining, 20L));
                continue;
            }
            CompletableFuture<List<CommandResult>> future = target.equals(nodeId)
                    ? propose(commands) : forward(target, commands);
            try {
                return future.get(Math.min(remaining, timings.clientTimeoutMs()), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                future.cancel(false);
                lastError = e;
            } catch (ExecutionException e) {
                lastError = e.getCause();
                sleepQuietly(Math.min(Math.max(deadline - System.currentTimeMillis(), 0L), 20L));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ClusterUnavailableException("Interrupted while waiting for commit", e);
            }
        }
        throw new ClusterUnavailableException("[" + nodeId + "] Could not commit " + commands
                + " within " + timeoutMs + "ms", lastError);
    }

    public CommandResult execute(Command command, long timeoutMs) {
        return execute(List.of(command), timeoutMs).get(0);
    }

    /** 리더 로컬 제안 (리더가 아니면 즉시 NotLeaderException으로 완료). */
    public CompletableFuture<List<CommandResult>> propose(List<Command> commands) {
        List<CompletableFuture<CommandResult>> futures;
        synchronized (lock) {
            if (!running || role != RaftRole.LEADER) {
                return CompletableFuture.failedFuture(new NotLeaderException(leaderId));
            }
            futures = appendLocked(commands, System.currentTimeMillis());
        }
        replicateSignal.release();
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> futures.stream().map(CompletableFuture::join).toList());
    }

    private List<CompletableFuture<CommandResult>> appendLocked(List<Command> commands, long now) {
        long index = raftLog.lastIndex();
        List<LogEntry> entries = new ArrayList<>(commands.size());
        List<CompletableFuture<CommandResult>> futures = new ArrayList<>(commands.size());
        for (Command c : commands) {
            index++;
            entries.add(new LogEntry(index, currentTerm, now, c));
            CompletableFuture<CommandResult> f = new CompletableFuture<>();
            pendingByIndex.put(index, f);
            futures.add(f);
        }
        raftLog.append(entries); // OS 버퍼 기록, fsync는 replicator가 Group Commit으로 수행
        return futures;
    }

    private CompletableFuture<List<CommandResult>> forward(String leader, List<Command> commands) {
        String rpcId = nodeId + "-" + UUID.randomUUID();
        CompletableFuture<List<CommandResult>> future = new CompletableFuture<>();
        forwarded.put(rpcId, future);
        future.whenComplete((r, e) -> forwarded.remove(rpcId));
        transport.send(Message.clientRequest(nodeId, leader, rpcId, commands));
        return future;
    }

    private void handleClientRequest(Message m) {
        String hint;
        boolean leader;
        synchronized (lock) {
            leader = running && role == RaftRole.LEADER;
            hint = leaderId;
        }
        if (!leader) {
            transport.send(Message.clientResponse(nodeId, m.getSenderId(), m.getRpcId(),
                    Message.ClientStatus.NOT_LEADER, hint, null));
            return;
        }
        propose(m.getCommands()).whenCompleteAsync((results, error) -> {
            if (error == null) {
                transport.send(Message.clientResponse(nodeId, m.getSenderId(), m.getRpcId(),
                        Message.ClientStatus.OK, nodeId, results));
            } else {
                transport.send(Message.clientResponse(nodeId, m.getSenderId(), m.getRpcId(),
                        Message.ClientStatus.NOT_LEADER, getLeaderId(), null));
            }
        }, callbackExecutor);
    }

    private void handleClientResponse(Message m) {
        CompletableFuture<List<CommandResult>> future = forwarded.remove(m.getRpcId());
        if (future == null) return;
        if (m.getClientStatus() == Message.ClientStatus.OK) {
            future.complete(m.getResults());
        } else {
            future.completeExceptionally(new NotLeaderException(m.getLeaderHint()));
        }
    }

    // ─── 로그 복제 (리더) ─────────────────────────────────────────────────────

    /** Group Commit: 신호가 오면 WAL을 1회 fsync하고 커밋 판정 후 팔로워에 일괄 전송. */
    private void replicatorLoop() {
        while (running) {
            try {
                replicateSignal.tryAcquire(timings.heartbeatIntervalMs(), TimeUnit.MILLISECONDS);
                replicateSignal.drainPermits();
            } catch (InterruptedException e) {
                return;
            }
            try {
                if (!isLeader()) continue;
                long durable = raftLog.sync();
                List<Runnable> completions = new ArrayList<>();
                synchronized (lock) {
                    if (!running || role != RaftRole.LEADER) continue;
                    selfDurableIndex = Math.max(selfDurableIndex, Math.min(durable, raftLog.lastIndex()));
                    advanceCommitLocked(completions);
                    for (String p : peers) {
                        replicateToLocked(p, false);
                    }
                }
                completions.forEach(Runnable::run);
            } catch (RuntimeException e) {
                log.error("[{}] replicator failed", nodeId, e);
            }
        }
    }

    /**
     * 팔로워에 AppendEntries 전송. 미확인 엔트리는 {@code sentUpTo} 이후부터 파이프라이닝으로 전송하고,
     * 새 엔트리나 커밋 변경이 없으면 force(Heartbeat)일 때만 전송합니다.
     */
    private void replicateToLocked(String peer, boolean force) {
        long last = raftLog.lastIndex();
        long from = Math.max(nextIndex.get(peer), sentUpTo.get(peer) + 1);
        List<LogEntry> batch = from <= last ? raftLog.slice(from, last, timings.maxBatchEntries()) : List.of();
        if (batch.isEmpty() && !force && lastSentCommit.get(peer) >= commitIndex) {
            return;
        }
        long prev = from - 1;
        transport.send(Message.appendEntries(nodeId, peer, currentTerm, prev, raftLog.termAt(prev), batch, commitIndex));
        if (!batch.isEmpty()) {
            sentUpTo.put(peer, batch.get(batch.size() - 1).getIndex());
        }
        lastSentCommit.put(peer, commitIndex);
    }

    private record AppendOutcome(Message response, boolean needsSync) {}

    /** 팔로워의 AppendEntries 처리: prevLog 일관성 검사 → 충돌 절단 → append → leaderCommit까지 apply. */
    private AppendOutcome handleAppendEntriesLocked(Message m, long now) {
        if (m.getTerm() < currentTerm) {
            return new AppendOutcome(Message.appendResponse(nodeId, m.getSenderId(), currentTerm, false, 0L, 0L), false);
        }
        if (role != RaftRole.FOLLOWER) {
            becomeFollowerLocked(currentTerm, m.getSenderId());
        }
        leaderId = m.getSenderId();
        resetElectionTimerLocked(now);

        long prev = m.getPrevLogIndex();
        long myLast = raftLog.lastIndex();
        if (prev > myLast) {
            return new AppendOutcome(Message.appendResponse(nodeId, leaderId, currentTerm, false, 0L, myLast + 1), false);
        }
        long myPrevTerm = raftLog.termAt(prev);
        if (myPrevTerm != m.getPrevLogTerm()) {
            long conflict = prev;
            while (conflict > 1 && raftLog.termAt(conflict - 1) == myPrevTerm) conflict--;
            return new AppendOutcome(Message.appendResponse(nodeId, leaderId, currentTerm, false, 0L, conflict), false);
        }

        List<LogEntry> toAppend = new ArrayList<>();
        for (LogEntry e : m.getEntries()) {
            if (!toAppend.isEmpty()) {
                toAppend.add(e);
                continue;
            }
            long existingTerm = raftLog.termAt(e.getIndex());
            if (existingTerm == -1L) {
                toAppend.add(e);
            } else if (existingTerm != e.getTerm()) {
                if (e.getIndex() <= commitIndex) {
                    log.error("[{}] Refusing to truncate committed index {} (commitIndex={}).", nodeId, e.getIndex(), commitIndex);
                    return new AppendOutcome(Message.appendResponse(nodeId, leaderId, currentTerm, false, 0L, 0L), false);
                }
                log.info("[{}] Log conflict at index {} (local term {} != leader term {}). Truncating.",
                        nodeId, e.getIndex(), existingTerm, e.getTerm());
                raftLog.truncateFrom(e.getIndex());
                toAppend.add(e);
            }
        }
        if (!toAppend.isEmpty()) {
            raftLog.append(toAppend);
        }
        long lastNew = prev + m.getEntries().size();
        long newCommit = Math.min(m.getLeaderCommit(), lastNew);
        if (newCommit > commitIndex) {
            commitIndex = newCommit;
            applyLocked(new ArrayList<>());
        }
        return new AppendOutcome(Message.appendResponse(nodeId, leaderId, currentTerm, true, lastNew, 0L),
                !toAppend.isEmpty());
    }

    private void handleAppendResponseLocked(Message m, long now, List<Runnable> completions) {
        if (role != RaftRole.LEADER || m.getTerm() != currentTerm) return;
        String p = m.getSenderId();
        if (!nextIndex.containsKey(p)) return;
        lastAck.put(p, now);
        if (m.isSuccess()) {
            if (m.getMatchIndex() > matchIndex.get(p)) {
                matchIndex.put(p, m.getMatchIndex());
            }
            nextIndex.put(p, Math.max(nextIndex.get(p), matchIndex.get(p) + 1));
            if (sentUpTo.get(p) < matchIndex.get(p)) {
                sentUpTo.put(p, matchIndex.get(p));
            }
            advanceCommitLocked(completions);
        } else {
            long hint = m.getConflictIndex() > 0 ? m.getConflictIndex() : nextIndex.get(p) - 1;
            long next = Math.max(matchIndex.get(p) + 1, Math.max(1L, Math.min(nextIndex.get(p) - 1, hint)));
            nextIndex.put(p, next);
            sentUpTo.put(p, next - 1);
            replicateToLocked(p, true);
        }
    }

    /** 커밋 규칙: 과반수(자신의 fsync 포함) matchIndex ≥ N && log[N].term == currentTerm. */
    private void advanceCommitLocked(List<Runnable> completions) {
        long newCommit = commitIndex;
        for (long n = raftLog.lastIndex(); n > commitIndex; n--) {
            if (raftLog.termAt(n) != currentTerm) break;
            int count = selfDurableIndex >= n ? 1 : 0;
            for (String p : peers) {
                if (matchIndex.get(p) >= n) count++;
            }
            if (count >= quorum()) {
                newCommit = n;
                break;
            }
        }
        if (newCommit <= commitIndex) return;
        commitIndex = newCommit;
        applyLocked(completions);
        if (!leadershipReady && commitIndex >= noOpIndex) {
            leadershipReady = true;
            long term = currentTerm;
            log.info("[{}] Leadership ready for term {} (NO_OP index {} committed).", nodeId, term, noOpIndex);
            completions.add(() -> {
                try {
                    callbackExecutor.execute(() -> leadershipListeners.forEach(l -> l.accept(term)));
                } catch (RuntimeException ignored) {
                    // 종료 중
                }
            });
        }
        replicateSignal.release(); // 팔로워에 leaderCommit 신속 전파
    }

    /** lastApplied+1 ~ commitIndex 를 인덱스 순서대로 FSM에 적용하고 대기 Future 완료를 예약. */
    private void applyLocked(List<Runnable> completions) {
        long target = Math.min(commitIndex, raftLog.lastIndex());
        while (lastApplied < target) {
            long index = lastApplied + 1;
            LogEntry entry = raftLog.get(index);
            CompletableFuture<CommandResult> future = pendingByIndex.remove(index);
            try {
                CommandResult result = fsm.apply(entry);
                if (future != null) completions.add(() -> future.complete(result));
            } catch (RuntimeException e) {
                log.error("[{}] FSM apply failed at index {}: {}", nodeId, index, entry, e);
                if (future != null) completions.add(() -> future.completeExceptionally(e));
            }
            lastApplied = index;
        }
    }

    private void failPendingLocked(RuntimeException error) {
        for (CompletableFuture<CommandResult> f : pendingByIndex.values()) {
            f.completeExceptionally(error);
        }
        pendingByIndex.clear();
    }

    private static void sleepQuietly(long ms) {
        if (ms <= 0) return;
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ─── 공개 접근자 ──────────────────────────────────────────────────────────
    public void addLeadershipListener(Consumer<Long> listener) { leadershipListeners.add(listener); }

    public String getNodeId() { return nodeId; }
    public List<String> getAllNodeIds() { return allNodeIds; }
    public boolean isRunning() { return running; }
    public ClusterStateMachine getStateMachine() { return fsm; }
    public RaftLogManager getRaftLog() { return raftLog; }
    public RaftTimings getTimings() { return timings; }

    public RaftRole getRole() { synchronized (lock) { return role; } }
    public boolean isLeader() { synchronized (lock) { return running && role == RaftRole.LEADER; } }
    /** 리더이며 현재 term의 NO_OP이 커밋되어 서비스 가능한 상태. */
    public boolean isLeaderReady() { synchronized (lock) { return running && role == RaftRole.LEADER && leadershipReady; } }
    public String getLeaderId() { synchronized (lock) { return running ? leaderId : null; } }
    public long getCurrentTerm() { synchronized (lock) { return currentTerm; } }
    public long getCommitIndex() { synchronized (lock) { return commitIndex; } }
    public long getLastApplied() { synchronized (lock) { return lastApplied; } }

    /** 커밋된 멤버십 기준 활성 노드 목록 (Quorum 분모와 무관). */
    public List<String> getActiveMembers() {
        return allNodeIds.stream().filter(fsm::isAlive).toList();
    }

    /** Readiness: 리더가 알려져 있고, 리더면 NO_OP 커밋 완료, 팔로워면 멤버십상 ALIVE. */
    public boolean isReady() {
        synchronized (lock) {
            if (!running || leaderId == null) return false;
            if (role == RaftRole.LEADER) return leadershipReady;
        }
        return fsm.isAlive(nodeId);
    }

    private static Thread daemon(Runnable r, String name) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }
}
