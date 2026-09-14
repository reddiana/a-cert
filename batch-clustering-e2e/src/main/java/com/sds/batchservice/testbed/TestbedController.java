package com.sds.batchservice.testbed;

import com.sds.batchservice.cluster.api.DistributedLockExecutor;
import com.sds.batchservice.cluster.api.DistributedQueueService;
import com.sds.batchservice.cluster.api.QueueLease;
import com.sds.batchservice.cluster.api.TaskJournalService;
import com.sds.batchservice.cluster.consensus.RaftNode;
import com.sds.batchservice.cluster.fsm.ClusterStateMachine;
import com.sds.batchservice.cluster.fsm.JournalStateMachine.TaskStatus;
import com.sds.batchservice.cluster.fsm.QueueStateMachine;
import com.sds.batchservice.cluster.recovery.RecoveryCoordinator;
import com.sds.batchservice.cluster.sync.WriteBehindSynchronizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * K8s 통합 검증용 테스트 API (7.1.6).
 *
 * <p>수요처 애플리케이션을 대신하여 엔진의 In-Process API를 HTTP로 노출합니다. 지연 시간은 서버 측에서 계측하여
 * 검증 드라이버와의 HTTP 왕복 시간과 분리합니다. 엔진이 비활성화된 기준선 Pod(TC-K8S-07)에서는 JVM 정보만 제공합니다.
 */
@RestController
@RequestMapping("/testbed")
public class TestbedController {

    private static final Logger log = LoggerFactory.getLogger(TestbedController.class);
    private static final long LOCK_WAIT_MS = 10_000L;
    private static final long LOCK_LEASE_MS = 10_000L;
    private static final Path CGROUP_MEMORY_CURRENT = Path.of("/sys/fs/cgroup/memory.current");

    private final ObjectProvider<RaftNode> raftNode;
    private final ObjectProvider<DistributedLockExecutor> lockExecutor;
    private final ObjectProvider<DistributedQueueService> queueService;
    private final ObjectProvider<TaskJournalService> journalService;
    private final ObjectProvider<RecoveryCoordinator> recoveryCoordinator;
    private final ObjectProvider<WriteBehindSynchronizer> writeBehind;
    private final ObjectProvider<JdbcTemplate> jdbcTemplate;

    public TestbedController(ObjectProvider<RaftNode> raftNode,
                             ObjectProvider<DistributedLockExecutor> lockExecutor,
                             ObjectProvider<DistributedQueueService> queueService,
                             ObjectProvider<TaskJournalService> journalService,
                             ObjectProvider<RecoveryCoordinator> recoveryCoordinator,
                             ObjectProvider<WriteBehindSynchronizer> writeBehind,
                             ObjectProvider<JdbcTemplate> jdbcTemplate) {
        this.raftNode = raftNode;
        this.lockExecutor = lockExecutor;
        this.queueService = queueService;
        this.journalService = journalService;
        this.recoveryCoordinator = recoveryCoordinator;
        this.writeBehind = writeBehind;
        this.jdbcTemplate = jdbcTemplate;
    }

    // ─── 상태 조회 ────────────────────────────────────────────────────────────
    /** 장애 주입 관측용 경량 응답 (JVM 기동 시각으로 컨테이너 재기동 여부 판별). */
    @GetMapping("/ping")
    public Map<String, Object> ping() {
        return Map.of("startTime", ManagementFactory.getRuntimeMXBean().getStartTime(), "now", System.currentTimeMillis());
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("now", System.currentTimeMillis());
        m.put("jvm", jvm());
        RaftNode node = raftNode.getIfAvailable();
        m.put("engineEnabled", node != null);
        if (node == null) {
            return m;
        }
        ClusterStateMachine fsm = node.getStateMachine();
        m.put("nodeId", node.getNodeId());
        m.put("role", node.getRole().name());
        m.put("term", node.getCurrentTerm());
        m.put("leaderId", node.getLeaderId());
        m.put("leaderReady", node.isLeaderReady());
        m.put("ready", node.isReady());
        m.put("commitIndex", node.getCommitIndex());
        m.put("lastApplied", node.getLastApplied());
        m.put("lastLogIndex", node.getRaftLog().lastIndex());
        m.put("activeMembers", node.getActiveMembers());
        m.put("queueWaiting", fsm.getQueue().waitingSnapshot());
        m.put("queueInFlight", fsm.getQueue().inFlightSnapshot());
        m.put("journal", fsm.getJournal().snapshot());
        m.put("locks", fsm.getLock().snapshot());
        RecoveryCoordinator coordinator = recoveryCoordinator.getIfAvailable();
        m.put("recoveryActions", coordinator == null ? List.of() : coordinator.getActions());
        WriteBehindSynchronizer sync = writeBehind.getIfAvailable();
        if (sync != null) {
            Map<String, Object> wb = new LinkedHashMap<>();
            wb.put("state", sync.getState().name());
            wb.put("checkpoint", sync.getCheckpoint());
            wb.put("unsyncedRecordCount", sync.getUnsyncedRecordCount());
            wb.put("fullySynchronized", sync.isFullySynchronized());
            m.put("writeBehind", wb);
        }
        return m;
    }

    /** 커밋 구간의 로그 term 목록 (노드 간 로그 수렴 판정). */
    @GetMapping("/log-terms")
    public Map<String, Object> logTerms() {
        RaftNode node = node();
        long commit = node.getCommitIndex();
        List<Long> terms = new ArrayList<>();
        for (long i = 1; i <= commit; i++) {
            terms.add(node.getRaftLog().termAt(i));
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("nodeId", node.getNodeId());
        m.put("commitIndex", commit);
        m.put("lastApplied", node.getLastApplied());
        m.put("terms", terms);
        return m;
    }

    /** GC 후 Heap 사용량 (TC-K8S-07). */
    @PostMapping("/gc")
    public Map<String, Object> gc() throws InterruptedException {
        long gcTimeBefore = gcTimeMs();
        System.gc();
        Thread.sleep(500);
        Map<String, Object> m = jvm();
        m.put("gcTimeBeforeForcedGcMs", gcTimeBefore);
        return m;
    }

    // ─── DAG 상태 전이 (TC-K8S-01) ────────────────────────────────────────────
    @PostMapping("/dag/reset")
    public Map<String, Object> dagReset(@RequestParam String taskId) {
        JdbcTemplate jdbc = jdbc();
        jdbc.execute("CREATE TABLE IF NOT EXISTS testbed_dag_task ("
                + " task_id VARCHAR(128) NOT NULL PRIMARY KEY, status VARCHAR(32) NOT NULL)");
        jdbc.execute("CREATE TABLE IF NOT EXISTS testbed_dag_trigger ("
                + " id BIGSERIAL PRIMARY KEY, task_id VARCHAR(128) NOT NULL, node_id VARCHAR(128) NOT NULL, triggered_at BIGINT NOT NULL)");
        jdbc.update("DELETE FROM testbed_dag_trigger WHERE task_id = ?", taskId);
        jdbc.update("DELETE FROM testbed_dag_task WHERE task_id = ?", taskId);
        jdbc.update("INSERT INTO testbed_dag_task (task_id, status) VALUES (?, 'READY')", taskId);
        return Map.of("taskId", taskId, "status", "READY");
    }

    /**
     * 분산 락 하에서 후행 태스크 상태를 판정·전이합니다. 조회와 갱신이 분리된 판정 로직이므로
     * 상호 배제가 깨지면 후행 태스크가 중복 발화합니다.
     */
    @PostMapping("/dag/transition")
    public Map<String, Object> dagTransition(@RequestParam String lock, @RequestParam String requestId,
                                             @RequestParam String taskId) {
        long receivedAt = System.currentTimeMillis();
        JdbcTemplate jdbc = jdbc();
        String nodeId = node().getNodeId();
        Map<String, Object> result = lockExecutor().execute(lock, requestId, () -> {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("acquiredAt", System.currentTimeMillis());
            long csStart = System.nanoTime();
            String status = jdbc.queryForObject("SELECT status FROM testbed_dag_task WHERE task_id = ?", String.class, taskId);
            boolean triggered = false;
            if ("READY".equals(status)) {
                jdbc.update("UPDATE testbed_dag_task SET status = 'RUNNING' WHERE task_id = ?", taskId);
                jdbc.update("INSERT INTO testbed_dag_trigger (task_id, node_id, triggered_at) VALUES (?, ?, ?)",
                        taskId, nodeId, System.currentTimeMillis());
                triggered = true;
            }
            r.put("criticalSectionMicros", (System.nanoTime() - csStart) / 1_000L);
            r.put("triggered", triggered);
            return r;
        });
        result.put("nodeId", nodeId);
        result.put("receivedAt", receivedAt);
        result.put("finishedAt", System.currentTimeMillis());
        return result;
    }

    /**
     * 임계 구역에서 외부 I/O 없이 분산 락 직렬화 비용만 계측합니다 (TC-K8S-01B).
     *
     * <p>진입·이탈 시각은 {@link System#nanoTime()}(Linux CLOCK_MONOTONIC)으로 기록합니다. 싱글노드에서는 모든 Pod가
     * 같은 커널 시계를 공유하므로, 검증 드라이버가 Pod 간 임계 구역 구간의 중첩 여부로 상호 배제를 판정할 수 있습니다.
     */
    @PostMapping("/lock/critical-section")
    public Map<String, Object> lockCriticalSection(@RequestParam String lock, @RequestParam String requestId) {
        long receivedAt = System.currentTimeMillis();
        String nodeId = node().getNodeId();
        Map<String, Object> result = lockExecutor().execute(lock, requestId, () -> {
            Map<String, Object> r = new LinkedHashMap<>();
            long entered = System.nanoTime();
            r.put("enteredNanos", entered);
            long exited = System.nanoTime();
            r.put("exitedNanos", exited);
            r.put("criticalSectionMicros", (exited - entered) / 1_000L);
            return r;
        });
        result.put("nodeId", nodeId);
        result.put("receivedAt", receivedAt);
        result.put("finishedAt", System.currentTimeMillis());
        return result;
    }

    @GetMapping("/dag/triggers")
    public Map<String, Object> dagTriggers(@RequestParam String taskId) {
        Long count = jdbc().queryForObject("SELECT count(*) FROM testbed_dag_trigger WHERE task_id = ?", Long.class, taskId);
        return Map.of("taskId", taskId, "count", count == null ? 0L : count);
    }

    // ─── 분산 큐 ──────────────────────────────────────────────────────────────
    @PostMapping("/queue/offer")
    public Map<String, Object> offer(@RequestParam String jobId) {
        return Map.of("jobId", jobId, "accepted", queue().offer(jobId));
    }

    @PostMapping("/queue/offer-batch")
    public Map<String, Object> offerBatch(@RequestParam String prefix, @RequestParam int count) {
        long start = System.currentTimeMillis();
        int accepted = 0;
        for (int i = 1; i <= count; i++) {
            if (queue().offer(prefix + i)) accepted++;
        }
        return Map.of("accepted", accepted, "elapsedMs", System.currentTimeMillis() - start);
    }

    @PostMapping("/queue/poll-lease")
    public Map<String, Object> pollLease() {
        QueueLease lease = queue().pollLease();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("jobId", lease == null ? null : lease.jobId());
        m.put("leaseToken", lease == null ? null : lease.leaseToken());
        return m;
    }

    @PostMapping("/queue/ack")
    public Map<String, Object> ack(@RequestParam String jobId, @RequestParam long leaseToken) {
        return Map.of("accepted", queue().ack(new QueueLease(jobId, leaseToken)));
    }

    /**
     * 이 Pod의 워커 스레드로 대기열을 인출합니다 (TC-K8S-02). 개별 {@code poll()} 지연과 인출 시각을 서버 시계로 기록합니다.
     */
    @PostMapping("/queue/drain")
    public Map<String, Object> drain(@RequestParam(defaultValue = "4") int workers,
                                     @RequestParam(defaultValue = "50") long pollTimeoutMs,
                                     @RequestParam(defaultValue = "5000") long maxMs) throws InterruptedException {
        DistributedQueueService queue = queue();
        QueueStateMachine view = node().getStateMachine().getQueue();
        List<Map<String, Object>> dequeued = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger errors = new AtomicInteger();
        long startedAt = System.currentTimeMillis();
        long deadline = startedAt + maxMs;
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        for (int w = 0; w < workers; w++) {
            pool.submit(() -> {
                while (System.currentTimeMillis() < deadline) {
                    long fetchStart = System.currentTimeMillis();
                    try {
                        String job = queue.poll(pollTimeoutMs, TimeUnit.MILLISECONDS);
                        if (job != null) {
                            long at = System.currentTimeMillis();
                            dequeued.add(Map.of("jobId", job, "latencyMs", at - fetchStart, "at", at));
                        } else if (view.isEmpty()) {
                            return;
                        }
                    } catch (InterruptedException e) {
                        return;
                    } catch (RuntimeException e) {
                        errors.incrementAndGet();
                        log.warn("drain poll failed: {}", e.toString());
                    }
                }
            });
        }
        pool.shutdown();
        pool.awaitTermination(maxMs + 1_000L, TimeUnit.MILLISECONDS);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("nodeId", node().getNodeId());
        m.put("startedAt", startedAt);
        m.put("errors", errors.get());
        synchronized (dequeued) {
            m.put("dequeued", new ArrayList<>(dequeued));
        }
        return m;
    }

    /** 이전 테스트 잔여 작업 정리: in-flight와 대기열 전체를 ACK 처리합니다. */
    @PostMapping("/queue/clear")
    public Map<String, Object> clear() {
        DistributedQueueService queue = queue();
        int acked = 0;
        for (QueueStateMachine.InFlight f : node().getStateMachine().getQueue().inFlightSnapshot()) {
            if (queue.ack(new QueueLease(f.jobId(), f.leaseToken()))) acked++;
        }
        QueueLease lease;
        while ((lease = queue.pollLease()) != null) {
            if (queue.ack(lease)) acked++;
        }
        return Map.of("acked", acked);
    }

    // ─── 저널 ─────────────────────────────────────────────────────────────────
    @PostMapping("/journal/record")
    public Map<String, Object> record(@RequestParam String executionId, @RequestParam String jobId,
                                      @RequestParam TaskStatus status, @RequestParam long leaseToken) {
        return Map.of("accepted", journal().record(executionId, jobId, status, leaseToken));
    }

    // ─── 락 + 큐 복합 처리 (TC-K8S-09, TC-K8S-12) ─────────────────────────────
    @PostMapping("/cycle")
    public Map<String, Object> cycle(@RequestParam String lock, @RequestParam String jobId) {
        long start = System.currentTimeMillis();
        DistributedQueueService queue = queue();
        boolean success = lockExecutor().execute(lock, jobId, LOCK_WAIT_MS, LOCK_LEASE_MS, () -> {
            if (!queue.offer(jobId)) return false;
            QueueLease lease = queue.pollLease();
            return lease != null && queue.ack(lease);
        });
        return Map.of("success", success, "elapsedMs", System.currentTimeMillis() - start);
    }

    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<Map<String, Object>> onError(RuntimeException e) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", e.getClass().getSimpleName());
        body.put("message", String.valueOf(e.getMessage()));
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(body);
    }

    // ─── 내부 ─────────────────────────────────────────────────────────────────
    private RaftNode node() {
        return required(raftNode.getIfAvailable(), "RaftNode");
    }

    private DistributedLockExecutor lockExecutor() {
        return required(lockExecutor.getIfAvailable(), "DistributedLockExecutor");
    }

    private DistributedQueueService queue() {
        return required(queueService.getIfAvailable(), "DistributedQueueService");
    }

    private TaskJournalService journal() {
        return required(journalService.getIfAvailable(), "TaskJournalService");
    }

    private JdbcTemplate jdbc() {
        return required(jdbcTemplate.getIfAvailable(), "JdbcTemplate");
    }

    private static <T> T required(T bean, String name) {
        if (bean == null) {
            throw new IllegalStateException(name + " is not available (batch.cluster.enabled=false?)");
        }
        return bean;
    }

    private static Map<String, Object> jvm() {
        MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("startTime", ManagementFactory.getRuntimeMXBean().getStartTime());
        m.put("heapUsedBytes", heap.getUsed());
        m.put("heapCommittedBytes", heap.getCommitted());
        m.put("gcTimeMs", gcTimeMs());
        m.put("threadCount", ManagementFactory.getThreadMXBean().getThreadCount());
        m.put("containerMemoryBytes", containerMemoryBytes());
        return m;
    }

    private static long gcTimeMs() {
        long total = 0;
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            total += Math.max(gc.getCollectionTime(), 0L);
        }
        return total;
    }

    private static long containerMemoryBytes() {
        try {
            return Files.exists(CGROUP_MEMORY_CURRENT) ? Long.parseLong(Files.readString(CGROUP_MEMORY_CURRENT).trim()) : -1L;
        } catch (IOException | NumberFormatException e) {
            return -1L;
        }
    }
}
