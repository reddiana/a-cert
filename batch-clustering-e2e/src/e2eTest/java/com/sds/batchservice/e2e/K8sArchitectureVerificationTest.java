package com.sds.batchservice.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.sds.batchservice.e2e.support.Chaos;
import com.sds.batchservice.e2e.support.K8sCluster;
import com.sds.batchservice.e2e.support.Kubectl;
import com.sds.batchservice.e2e.support.TestbedClient;
import com.sds.batchservice.e2e.support.TestbedClient.TestbedException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.sds.batchservice.e2e.support.K8sCluster.EXECUTION_NAMESPACE;
import static com.sds.batchservice.e2e.support.K8sCluster.NAMESPACE;
import static com.sds.batchservice.e2e.support.K8sCluster.PODS;
import static com.sds.batchservice.e2e.support.K8sCluster.texts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.awaitility.Awaitility.await;

/**
 * K8s 배포 통합 검증 (7.1.6 / 7.2.13): minikube 싱글노드에 5.4.2 StatefulSet을 배포하고,
 * 실제 TCP·PVC·Pod 생명주기와 Chaos Mesh 장애 주입으로 QAS-01 ~ 09 및 DD-07 운영 시나리오를 검증합니다.
 *
 * <p>각 테스트는 실행 식별자({@code run})가 포함된 작업 ID를 사용하여 이전 테스트의 잔여 상태와 구분합니다.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class K8sArchitectureVerificationTest {

    private static final Logger log = LoggerFactory.getLogger(K8sArchitectureVerificationTest.class);
    private static final long MIB = 1024L * 1024L;

    private static K8sCluster cluster;
    private static TestbedClient client;

    private String run;

    @BeforeAll
    static void connect() {
        cluster = K8sCluster.connect();
        client = cluster.client();
    }

    @BeforeEach
    void prepare(TestInfo info) {
        run = Long.toString(System.currentTimeMillis() % 100_000_000L, 36);
        Chaos.deleteAll();
        if (info.getTags().contains("fresh-deploy")) {
            return;
        }
        cluster.ensureDeployed();
        cluster.awaitHealthy(Duration.ofMinutes(5));
        client.post(cluster.awaitLeader(), "/testbed/queue/clear", Duration.ofMinutes(2));
    }

    @AfterEach
    void cleanup() {
        Chaos.deleteAll();
    }

    @Test
    @Order(1)
    @Tag("fresh-deploy")
    @DisplayName("TC-K8S-08: 외부 미들웨어 없는 StatefulSet 배포 및 클러스터 부트스트랩 검증")
    void tcK8s08_statefulSetBootstrap() {
        cluster.undeployScheduler();

        long start = System.currentTimeMillis();
        Kubectl.run("apply", "-k", cluster.path("k8s/overlays/minikube").toString());
        await().atMost(Duration.ofMinutes(3)).pollInterval(Duration.ofMillis(500)).ignoreExceptions()
                .until(() -> cluster.k8sReadyPodCount() == PODS.size() && cluster.findLeader().isPresent());
        long totalBootstrapTime = System.currentTimeMillis() - start;
        String leader = cluster.awaitLeader();

        // 실제 TCP(Headless Service DNS) 경로로 복제·커밋 확인
        String job = "JOB-SOCKET-" + run;
        assertThat(client.post(leader, "/testbed/queue/offer?jobId=" + job).path("accepted").asBoolean()).isTrue();
        awaitAllPods(Duration.ofSeconds(3), s -> texts(s.path("queueWaiting")).contains(job));

        // ns-batchservice에는 StatefulSet 단일 컨테이너 Pod만 존재 (코디네이터 미들웨어·사이드카 없음)
        String workloads = Kubectl.run("get", "pods", "-n", NAMESPACE, "-o",
                "jsonpath={range .items[*]}{.metadata.name}|{.spec.containers[*].name}|{.spec.initContainers[*].name}{\"\\n\"}{end}");
        long externalProcessCount = workloads.lines()
                .filter(l -> !l.matches("batch-scheduler-\\d\\|batch-scheduler\\|"))
                .count();
        log.info("TC-K8S-08: kubectl apply → 3 Pods Ready + leader in {}ms (leader={}), workloads=[{}], external middleware={}",
                totalBootstrapTime, leader, workloads.replace('\n', ' '), externalProcessCount);

        assertThat(externalProcessCount).isZero();
        assertThat(totalBootstrapTime).isLessThanOrEqualTo(120_000L);
    }

    @Test
    @Order(2)
    @DisplayName("TC-K8S-01: 3개 Pod 분산 동시 100건 DAG 상태 전이 원자성 검증")
    void tcK8s01_dagConcurrencyConsistency() throws Exception {
        String leader = cluster.awaitLeader();
        String taskId = "task-B-" + run;
        String lock = "dag-job-workflow-" + run;
        client.post(leader, "/testbed/dag/reset?taskId=" + taskId);
        client.post(leader, "/testbed/dag/reset?taskId=" + taskId + "-warmup");
        warmUp(i -> "/testbed/dag/transition?lock=" + lock + "-warmup&requestId=warm-" + i + "&taskId=" + taskId + "-warmup");

        int threadCount = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<JsonNode> results = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger failures = new AtomicInteger();
        for (int i = 0; i < threadCount; i++) {
            String pod = PODS.get(i % PODS.size());
            String path = "/testbed/dag/transition?lock=" + lock + "&requestId=req-" + i + "&taskId=" + taskId;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    results.add(client.post(pod, path, Duration.ofSeconds(60)));
                } catch (Exception e) {
                    failures.incrementAndGet();
                    log.warn("transition failed on {}: {}", pod, e.toString());
                }
            });
        }
        long startTime = System.currentTimeMillis();
        startLatch.countDown();
        executor.shutdown();
        boolean finished = executor.awaitTermination(90, TimeUnit.SECONDS);
        long driverElapsed = System.currentTimeMillis() - startTime;

        long serverSpan = results.stream().mapToLong(r -> r.path("finishedAt").asLong()).max().orElse(0)
                - results.stream().mapToLong(r -> r.path("receivedAt").asLong()).min().orElse(0);
        double avgCriticalSectionMs = results.stream().mapToLong(r -> r.path("criticalSectionMicros").asLong())
                .average().orElse(0) / 1000.0;
        long triggeredResponses = results.stream().filter(r -> r.path("triggered").asBoolean()).count();
        long triggers = client.get(leader, "/testbed/dag/triggers?taskId=" + taskId).path("count").asLong();
        log.info("TC-K8S-01: {} requests, acquired={}, failures={}, triggers(DB)={}, server span={}ms, driver elapsed={}ms, "
                        + "avg critical-section={}ms",
                threadCount, results.size(), failures.get(), triggers, serverSpan, driverElapsed,
                String.format("%.2f", avgCriticalSectionMs));

        assertThat(finished).isTrue();
        assertThat(failures.get()).isZero();
        assertThat(results).hasSize(threadCount);
        assertThat(triggers).isEqualTo(1L);
        assertThat(triggeredResponses).isEqualTo(1L);
        assertThat(avgCriticalSectionMs).isLessThanOrEqualTo(10.0);
        assertThat(serverSpan).isLessThanOrEqualTo(1000L);
        awaitAllPods(Duration.ofSeconds(3), s -> s.path("locks").findValuesAsText("key").stream().noneMatch(lock::equals));
    }

    @Test
    @Order(2)
    @DisplayName("TC-K8S-01B: 임계 구역 DB 왕복 제외 3개 Pod 분산 동시 100건 락 직렬화 검증")
    void tcK8s01b_lockSerializationWithoutDb() throws Exception {
        cluster.awaitLeader();
        String lock = "lock-only-" + run;
        warmUp(i -> "/testbed/lock/critical-section?lock=" + lock + "-warmup&requestId=warm-" + i);

        int threadCount = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<JsonNode> results = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger failures = new AtomicInteger();
        for (int i = 0; i < threadCount; i++) {
            String pod = PODS.get(i % PODS.size());
            String path = "/testbed/lock/critical-section?lock=" + lock + "&requestId=req-" + i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    results.add(client.post(pod, path, Duration.ofSeconds(60)));
                } catch (Exception e) {
                    failures.incrementAndGet();
                    log.warn("critical-section failed on {}: {}", pod, e.toString());
                }
            });
        }
        long startTime = System.currentTimeMillis();
        startLatch.countDown();
        executor.shutdown();
        boolean finished = executor.awaitTermination(90, TimeUnit.SECONDS);
        long driverElapsed = System.currentTimeMillis() - startTime;

        long serverSpan = results.stream().mapToLong(r -> r.path("finishedAt").asLong()).max().orElse(0)
                - results.stream().mapToLong(r -> r.path("receivedAt").asLong()).min().orElse(0);
        double avgCriticalSectionMs = results.stream().mapToLong(r -> r.path("criticalSectionMicros").asLong())
                .average().orElse(0) / 1000.0;
        // 상호 배제: 진입 시각 순으로 정렬했을 때 앞선 임계 구역의 이탈 이전에 진입한 요청 수
        List<JsonNode> ordered = results.stream()
                .sorted(java.util.Comparator.comparingLong(r -> r.path("enteredNanos").asLong())).toList();
        int overlaps = 0;
        long latestExit = Long.MIN_VALUE;
        for (JsonNode r : ordered) {
            if (r.path("enteredNanos").asLong() < latestExit) overlaps++;
            latestExit = Math.max(latestExit, r.path("exitedNanos").asLong());
        }
        log.info("TC-K8S-01B: {} requests, acquired={}, failures={}, overlapping critical sections={}, server span={}ms, "
                        + "driver elapsed={}ms, avg critical-section={}ms",
                threadCount, results.size(), failures.get(), overlaps, serverSpan, driverElapsed,
                String.format("%.3f", avgCriticalSectionMs));

        assertThat(finished).isTrue();
        assertThat(failures.get()).isZero();
        assertThat(results).hasSize(threadCount);
        assertThat(overlaps).isZero();
        assertThat(avgCriticalSectionMs).isLessThanOrEqualTo(10.0);
        assertThat(serverSpan).isLessThanOrEqualTo(1000L);
        awaitAllPods(Duration.ofSeconds(3), s -> s.path("locks").findValuesAsText("key").stream().noneMatch(lock::equals));
    }

    @Test
    @Order(3)
    @DisplayName("TC-K8S-02: 3개 Pod 12개 워커 동시 200건 큐 인출 성능 검증")
    void tcK8s02_distributedQueuePerformance() throws Exception {
        String leader = cluster.awaitLeader();
        int taskCount = 200;
        String prefix = "TASK-BATCH-" + run + "-";
        assertThat(client.post(leader, "/testbed/queue/offer-batch?prefix=" + prefix + "&count=" + taskCount,
                Duration.ofMinutes(2)).path("accepted").asInt()).isEqualTo(taskCount);
        awaitAllPods(Duration.ofSeconds(10), s -> s.path("queueWaiting").size() == taskCount);

        ExecutorService drivers = Executors.newFixedThreadPool(PODS.size());
        CountDownLatch startLatch = new CountDownLatch(1);
        List<JsonNode> responses = Collections.synchronizedList(new ArrayList<>());
        for (String pod : PODS) {
            drivers.submit(() -> {
                startLatch.await();
                responses.add(client.post(pod, "/testbed/queue/drain?workers=4&pollTimeoutMs=50&maxMs=5000",
                        Duration.ofSeconds(30)));
                return null;
            });
        }
        startLatch.countDown();
        drivers.shutdown();
        assertThat(drivers.awaitTermination(60, TimeUnit.SECONDS)).isTrue();

        List<JsonNode> dequeued = new ArrayList<>();
        responses.forEach(r -> r.path("dequeued").forEach(dequeued::add));
        Set<String> ids = new HashSet<>();
        dequeued.forEach(d -> ids.add(d.path("jobId").asText()));
        long firstStart = responses.stream().mapToLong(r -> r.path("startedAt").asLong()).min().orElse(0);
        long lastDequeue = dequeued.stream().mapToLong(d -> d.path("at").asLong()).max().orElse(0);
        long totalElapsed = lastDequeue - firstStart;
        double avg = dequeued.stream().mapToLong(d -> d.path("latencyMs").asLong()).average().orElse(0);
        long max = dequeued.stream().mapToLong(d -> d.path("latencyMs").asLong()).max().orElse(0);
        int errors = responses.stream().mapToInt(r -> r.path("errors").asInt()).sum();
        log.info("TC-K8S-02: dequeued={} (unique {}) in {}ms, avg={}ms, max={}ms, errors={}, per pod={}",
                dequeued.size(), ids.size(), totalElapsed, String.format("%.2f", avg), max, errors,
                responses.stream().map(r -> r.path("nodeId").asText() + ":" + r.path("dequeued").size()).toList());

        assertThat(ids).hasSize(taskCount);
        assertThat(ids).allMatch(id -> id.startsWith(prefix));
        assertThat(dequeued).hasSize(taskCount);
        assertThat(avg).isLessThanOrEqualTo(20.0);
        assertThat(max).isLessThanOrEqualTo(50L);
        assertThat(totalElapsed).isLessThanOrEqualTo(500L);
    }

    @Test
    @Order(4)
    @DisplayName("TC-K8S-03: 리더 Pod 네트워크 분할(Chaos Mesh) 시 단일 리더 보장 검증")
    void tcK8s03_networkPartitionSingleLeader() {
        String oldLeader = cluster.awaitLeader();
        List<String> majority = cluster.others(oldLeader);
        String job = "JOB-PARTITION-" + run;
        assertThat(client.post(oldLeader, "/testbed/queue/offer?jobId=" + job).path("accepted").asBoolean()).isTrue();
        awaitAllPods(Duration.ofSeconds(3), s -> texts(s.path("queueWaiting")).contains(job));

        Chaos.partition("partition-" + run, oldLeader, majority);
        long partitionedAt = System.currentTimeMillis();
        TestbedException minorityPoll = catchThrowableOfType(
                () -> client.post(oldLeader, "/testbed/queue/poll-lease", Duration.ofSeconds(15)), TestbedException.class);

        String newLeader = await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(100))
                .until(() -> cluster.findLeaderAmong(majority).orElse(null), Objects::nonNull);
        long electionElapsed = System.currentTimeMillis() - partitionedAt;
        int totalActiveLeaders = cluster.activeLeaderCount();
        JsonNode oldLeaderStatus = cluster.status(oldLeader);
        JsonNode newLeaderStatus = cluster.status(newLeader);
        log.info("TC-K8S-03: minority poll → {}, new leader={} (term {}) {}ms after partition, active leaders={}, old leader role={}",
                minorityPoll == null ? "no error" : minorityPoll.error(), newLeader, newLeaderStatus.path("term").asLong(),
                electionElapsed, totalActiveLeaders, oldLeaderStatus.path("role").asText());

        assertThat(minorityPoll).isNotNull();
        assertThat(minorityPoll.error()).isEqualTo("ClusterUnavailableException");
        assertThat(oldLeaderStatus.path("role").asText()).isNotEqualTo("LEADER");
        assertThat(totalActiveLeaders).isLessThanOrEqualTo(1);
        assertThat(withRun(texts(newLeaderStatus.path("queueWaiting")))).containsExactly(job);

        // 다수파 정상 쓰기 후 분할 해제 → 재수렴
        String job301 = "JOB-301-" + run;
        assertThat(client.post(newLeader, "/testbed/queue/offer?jobId=" + job301).path("accepted").asBoolean()).isTrue();
        long healStart = System.currentTimeMillis();
        Chaos.delete("networkchaos", "partition-" + run);
        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(200)).untilAsserted(() -> {
            assertThat(cluster.activeLeaderCount()).isEqualTo(1);
            cluster.assertLogsConverged();
        });
        log.info("TC-K8S-03: partition healed, logs converged in {}ms", System.currentTimeMillis() - healStart);
        assertThat(withRun(texts(cluster.status(oldLeader).path("queueWaiting")))).containsExactly(job, job301);
    }

    @Test
    @Order(5)
    @DisplayName("TC-K8S-04: 팔로워 컨테이너 크래시 시 3초 이내 멤버십 자가 치유 검증")
    void tcK8s04_followerFailureSelfHealing() {
        String leader = cluster.awaitLeader();
        List<String> followers = cluster.others(leader);
        String crashed = followers.get(0);
        String healthy = followers.get(1);
        assertThat(texts(cluster.status(leader).path("activeMembers"))).hasSize(3);

        long t0 = cluster.crash(crashed);
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(50))
                .until(() -> !texts(cluster.status(leader).path("activeMembers")).contains(crashed));
        long totalHealTime = System.currentTimeMillis() - t0;
        List<String> active = texts(cluster.status(leader).path("activeMembers"));
        log.info("TC-K8S-04: [{}] removed from membership in {}ms, active members={}", crashed, totalHealTime, active);

        assertThat(totalHealTime).isLessThanOrEqualTo(3000L);
        assertThat(active).contains(leader, healthy);
        await().atMost(Duration.ofSeconds(1)).pollInterval(Duration.ofMillis(50))
                .until(() -> !texts(cluster.status(healthy).path("activeMembers")).contains(crashed));
    }

    @Test
    @Order(6)
    @DisplayName("TC-K8S-05: 리더 컨테이너 크래시 시 3초 이내 신규 리더 선출 검증")
    void tcK8s05_leaderFailoverTime() {
        String originalLeader = cluster.awaitLeader();
        long t0 = cluster.crash(originalLeader);
        String newLeader = await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(50))
                .until(() -> cluster.findLeaderAmong(cluster.others(originalLeader)).orElse(null), Objects::nonNull);
        long totalFailoverTime = System.currentTimeMillis() - t0;
        log.info("TC-K8S-05: new leader [{}] ready (NO_OP committed) in {}ms", newLeader, totalFailoverTime);

        assertThat(totalFailoverTime).isLessThanOrEqualTo(3000L);
        assertThat(client.post(newLeader, "/testbed/queue/offer?jobId=JOB-AFTER-FAILOVER-" + run)
                .path("accepted").asBoolean()).isTrue();
    }

    @Test
    @Order(7)
    @DisplayName("TC-K8S-06: 리더 크래시 시 K8s Job 상태 기반 고립 작업 복구 검증")
    void tcK8s06_journalRecoveryWithKubernetesJobs() {
        String leader = cluster.awaitLeader();
        String follower = cluster.others(leader).get(0);
        try {
            // Execution Target: ns-batch의 실제 Job (exec-1·3은 Job 없음 = NOT_FOUND)
            createExecutionJob(exec(2), "exit 1");
            createExecutionJob(exec(4), "sleep 600");
            createExecutionJob(exec(5), "true");
            Kubectl.run("wait", "job/" + exec(2), "-n", EXECUTION_NAMESPACE, "--for=condition=Failed", "--timeout=180s");
            Kubectl.run("wait", "job/" + exec(4), "-n", EXECUTION_NAMESPACE, "--for=jsonpath={.status.ready}=1", "--timeout=180s");
            Kubectl.run("wait", "job/" + exec(5), "-n", EXECUTION_NAMESPACE, "--for=condition=Complete", "--timeout=180s");

            // 리더가 10개 작업 추적 — 1~5 RUNNING, 6~10 COMPLETED 후 ACK
            long exec4Token = 0;
            for (int i = 1; i <= 10; i++) {
                String job = job(i);
                assertThat(client.post(leader, "/testbed/queue/offer?jobId=" + job).path("accepted").asBoolean()).isTrue();
                JsonNode lease = client.post(leader, "/testbed/queue/poll-lease");
                assertThat(lease.path("jobId").asText()).isEqualTo(job);
                long token = lease.path("leaseToken").asLong();
                if (i <= 5) {
                    record(leader, exec(i), job, "RUNNING", token);
                    if (i == 4) exec4Token = token;
                } else {
                    record(leader, exec(i), job, "COMPLETED", token);
                    client.post(leader, "/testbed/queue/ack?jobId=" + job + "&leaseToken=" + token);
                }
            }
            // 생존 팔로워가 추적 중인 작업 (복구 대상 아님)
            client.post(follower, "/testbed/queue/offer?jobId=" + job(11));
            JsonNode lease11 = client.post(follower, "/testbed/queue/poll-lease");
            assertThat(lease11.path("jobId").asText()).isEqualTo(job(11));
            record(follower, exec(11), job(11), "RUNNING", lease11.path("leaseToken").asLong());

            long t0 = cluster.crashAndHoldDown(leader);
            String newLeader = await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(100))
                    .until(() -> cluster.findLeaderAmong(cluster.others(leader)).orElse(null), Objects::nonNull);
            await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(100)).until(() ->
                    recoveryActions(cluster.status(newLeader)).size() == 5);
            long recoveryElapsed = System.currentTimeMillis() - t0;

            JsonNode status = cluster.status(newLeader);
            List<JsonNode> actions = recoveryActions(status);
            log.info("TC-K8S-06: recovered in {}ms by [{}], actions={}", recoveryElapsed, newLeader,
                    actions.stream().map(a -> a.path("kind").asText() + " " + a.path("target").asText()
                            + " (" + a.path("observedStatus").asText() + ")").toList());

            List<String> waiting = withRun(texts(status.path("queueWaiting")));
            Map<String, JsonNode> inFlight = new HashMap<>();
            status.path("queueInFlight").forEach(f -> inFlight.put(f.path("jobId").asText(), f));
            Map<String, JsonNode> latestJournal = new HashMap<>();
            status.path("journal").forEach(e -> latestJournal.merge(e.path("jobId").asText(), e,
                    (a, b) -> b.path("leaseToken").asLong() >= a.path("leaseToken").asLong() ? b : a));

            assertThat(recoveryElapsed).isLessThanOrEqualTo(10_000L);
            assertThat(actions).noneMatch(a -> job(11).equals(a.path("target").asText()));
            assertThat(waiting).containsExactlyInAnyOrder(job(1), job(2), job(3));
            assertThat(actionKind(actions, job(1))).isEqualTo("REQUEUED NOT_FOUND");
            assertThat(actionKind(actions, job(2))).isEqualTo("REQUEUED FAILED");
            assertThat(actionKind(actions, job(4))).isEqualTo("ADOPTED RUNNING");
            assertThat(actionKind(actions, job(5))).isEqualTo("COMPLETED COMPLETED");
            assertThat(inFlight.get(job(4)).path("worker").asText()).isEqualTo(newLeader);
            assertThat(inFlight).doesNotContainKey(job(5));
            assertThat(inFlight.get(job(11)).path("worker").asText()).isEqualTo(follower);
            for (int i = 1; i <= 11; i++) {
                String job = job(i);
                boolean tracked = waiting.contains(job) || inFlight.containsKey(job)
                        || (latestJournal.containsKey(job) && "COMPLETED".equals(latestJournal.get(job).path("status").asText()));
                assertThat(tracked).as(job + " tracked").isTrue();
            }
            // 펜싱: 구 추적자의 뒤늦은 보고(구 leaseToken)는 거부
            assertThat(client.post(newLeader, "/testbed/journal/record?executionId=" + exec(4) + "&jobId=" + job(4)
                    + "&status=COMPLETED&leaseToken=" + exec4Token).path("accepted").asBoolean()).isFalse();
        } finally {
            Kubectl.run("delete", "jobs", "-n", EXECUTION_NAMESPACE, "-l", "e2e=execution-target", "--ignore-not-found", "--wait=false");
        }
    }

    @Test
    @Order(8)
    @DisplayName("TC-K8S-09: 메타 DB Pod 중단 중 락·큐 무중단 및 복구 후 정합성 복원 검증")
    void tcK8s09_metaDbOutageResilience() {
        String leader = cluster.awaitLeader();
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
                .until(() -> "NORMAL".equals(writeBehindState(leader)));

        cluster.scaleMetaDb(0);
        int taskCount = 50;
        int successfulOperations = 0;
        for (int i = 0; i < taskCount; i++) {
            String pod = PODS.get(i % PODS.size());
            try {
                if (client.post(pod, "/testbed/cycle?lock=db-down-lock-" + run + "&jobId=TASK-OFFLINE-" + run + "-" + i,
                        Duration.ofSeconds(30)).path("success").asBoolean()) {
                    successfulOperations++;
                }
            } catch (RuntimeException e) {
                log.warn("cycle failed on {}: {}", pod, e.toString());
            }
        }
        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(100))
                .until(() -> "DB_DOWN".equals(writeBehindState(leader)));
        long unsyncedDuringOutage = cluster.status(leader).path("writeBehind").path("unsyncedRecordCount").asLong();
        boolean allReady = PODS.stream().allMatch(p -> cluster.status(p).path("ready").asBoolean())
                && cluster.k8sReadyPodCount() == PODS.size();
        log.info("TC-K8S-09: during DB outage success={}/{}, all ready={}, unsynced={}",
                successfulOperations, taskCount, allReady, unsyncedDuringOutage);

        assertThat(successfulOperations).isEqualTo(taskCount);
        assertThat(allReady).isTrue();
        assertThat(unsyncedDuringOutage).isGreaterThanOrEqualTo(taskCount * 5L);

        long dbRecoveredAt = cluster.scaleMetaDb(1);
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(100)).ignoreExceptions()
                .until(() -> cluster.status(leader).path("writeBehind").path("fullySynchronized").asBoolean());
        long syncElapsed = System.currentTimeMillis() - dbRecoveredAt;
        JsonNode writeBehind = cluster.status(leader).path("writeBehind");
        long dbRecords = Long.parseLong(cluster.psql("SELECT count(*) FROM batch_journal_log"));
        log.info("TC-K8S-09: DB Pod Ready → Write-Behind synchronized in {}ms, checkpoint={}, DB records={}",
                syncElapsed, writeBehind.path("checkpoint").asLong(), dbRecords);

        assertThat(syncElapsed).isLessThanOrEqualTo(30_000L);
        assertThat(writeBehind.path("unsyncedRecordCount").asLong()).isZero();
        assertThat(dbRecords).isGreaterThanOrEqualTo(taskCount * 5L);
        await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(200)).ignoreExceptions().until(() ->
                Long.parseLong(cluster.psql("SELECT last_log_index FROM batch_sync_checkpoint WHERE cluster_id = 'batch-cluster'"))
                        == cluster.status(leader).path("writeBehind").path("checkpoint").asLong());
    }

    @Test
    @Order(9)
    @DisplayName("TC-K8S-07: 엔진 비활성 기준선 Pod 대비 추가 Heap 점유 검증")
    void tcK8s07_resourceFootprint() throws Exception {
        Kubectl.run("apply", "-f", cluster.path("k8s/e2e/qas07-baseline.yaml").toString());
        try {
            Kubectl.run("rollout", "status", "deployment/testbed-baseline", "-n", NAMESPACE, "--timeout=180s");
            await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(1))
                    .until(() -> client.tryGet(K8sCluster.BASELINE, "/testbed/ping", Duration.ofSeconds(2)).isPresent());

            String leader = cluster.awaitLeader();
            long gcTimeBefore = cluster.status(leader).path("jvm").path("gcTimeMs").asLong();
            assertThat(client.post(leader, "/testbed/queue/offer-batch?prefix=JOB-RES-" + run + "-&count=1000",
                    Duration.ofMinutes(5)).path("accepted").asInt()).isEqualTo(1000);
            Thread.sleep(5000); // 백그라운드 합의 루프 가동
            long gcPause = cluster.status(leader).path("jvm").path("gcTimeMs").asLong() - gcTimeBefore;

            JsonNode leaderJvm = client.post(leader, "/testbed/gc", Duration.ofSeconds(30));
            JsonNode baselineJvm = client.post(K8sCluster.BASELINE, "/testbed/gc", Duration.ofSeconds(30));
            long additionalHeapMB = (leaderJvm.path("heapUsedBytes").asLong() - baselineJvm.path("heapUsedBytes").asLong()) / MIB;
            long additionalContainerMB = (leaderJvm.path("containerMemoryBytes").asLong()
                    - baselineJvm.path("containerMemoryBytes").asLong()) / MIB;
            log.info("TC-K8S-07: heap leader={}MB baseline={}MB → additional={}MB, container memory leader={}MB baseline={}MB "
                            + "→ additional={}MB, GC time during load={}ms",
                    leaderJvm.path("heapUsedBytes").asLong() / MIB, baselineJvm.path("heapUsedBytes").asLong() / MIB,
                    additionalHeapMB, leaderJvm.path("containerMemoryBytes").asLong() / MIB,
                    baselineJvm.path("containerMemoryBytes").asLong() / MIB, additionalContainerMB, gcPause);

            assertThat(additionalHeapMB).isLessThanOrEqualTo(50L);
            assertThat(gcPause).isLessThanOrEqualTo(1000L);
        } finally {
            Kubectl.run("delete", "-f", cluster.path("k8s/e2e/qas07-baseline.yaml").toString(), "--ignore-not-found", "--wait=false");
        }
    }

    @Test
    @Order(10)
    @DisplayName("TC-K8S-10: Pod 재생성 시 동일 PVC WAL 복원 및 재합류 검증 (DD-07)")
    void tcK8s10_podRecreatedWithSamePvcRejoins() {
        String leader = cluster.awaitLeader();
        String target = cluster.others(leader).get(0);
        List<String> jobs = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            String job = "JOB-PVC-" + run + "-" + i;
            jobs.add(job);
            assertThat(client.post(leader, "/testbed/queue/offer?jobId=" + job).path("accepted").asBoolean()).isTrue();
        }
        awaitAllPods(Duration.ofSeconds(5), s -> texts(s.path("queueWaiting")).containsAll(jobs));
        long commitBefore = cluster.status(target).path("commitIndex").asLong();
        String podUid = Kubectl.run("get", "pod", target, "-n", NAMESPACE, "-o", "jsonpath={.metadata.uid}");
        String pvcUid = Kubectl.run("get", "pvc", "wal-storage-" + target, "-n", NAMESPACE, "-o", "jsonpath={.metadata.uid}");

        long t0 = System.currentTimeMillis();
        Kubectl.run("delete", "pod", target, "-n", NAMESPACE, "--wait=true");
        await().atMost(Duration.ofMinutes(4)).pollInterval(Duration.ofSeconds(1)).ignoreExceptions().until(() ->
                !podUid.equals(Kubectl.run("get", "pod", target, "-n", NAMESPACE, "-o", "jsonpath={.metadata.uid}"))
                        && "True".equals(Kubectl.run("get", "pod", target, "-n", NAMESPACE,
                        "-o", "jsonpath={.status.conditions[?(@.type==\"Ready\")].status}")));
        long rejoinElapsed = System.currentTimeMillis() - t0;

        Matcher recovered = Pattern.compile("WAL \\S+ recovered: lastIndex=(\\d+), term=(\\d+), commitIndex=(\\d+)")
                .matcher(Kubectl.run("logs", target, "-n", NAMESPACE));
        assertThat(recovered.find()).as("WAL recovery log").isTrue();
        long recoveredLastIndex = Long.parseLong(recovered.group(1));
        String pvcUidAfter = Kubectl.run("get", "pvc", "wal-storage-" + target, "-n", NAMESPACE, "-o", "jsonpath={.metadata.uid}");
        String currentLeader = cluster.awaitLeader();
        log.info("TC-K8S-10: [{}] recreated and Ready in {}ms, same PVC={}, WAL recovered lastIndex={} (commitIndex before delete={})",
                target, rejoinElapsed, pvcUid.equals(pvcUidAfter), recoveredLastIndex, commitBefore);

        assertThat(pvcUidAfter).isEqualTo(pvcUid);
        assertThat(recoveredLastIndex).isGreaterThanOrEqualTo(commitBefore);
        assertThat(texts(cluster.status(target).path("queueWaiting"))).containsAll(jobs);
        assertThat(texts(cluster.status(currentLeader).path("activeMembers"))).contains(target);
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(200)).untilAsserted(cluster::assertLogsConverged);
    }

    @Test
    @Order(11)
    @DisplayName("TC-K8S-11: 부하 중 StatefulSet 롤링 업데이트 시 Quorum 유지 및 작업 무유실 검증 (DD-07)")
    void tcK8s11_rollingUpdateUnderLoad() throws Exception {
        AtomicBoolean stop = new AtomicBoolean(false);
        List<String> acknowledged = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger failedAttempts = new AtomicInteger();
        AtomicLong maxGapMs = new AtomicLong();
        Thread loader = new Thread(() -> {
            long lastSuccess = System.currentTimeMillis();
            for (int k = 0; !stop.get(); k++) {
                String jobId = "JOB-RU-" + run + "-" + k;
                boolean ok = false;
                for (int attempt = 0; attempt < 100 && !ok && !stop.get(); attempt++) {
                    String pod = PODS.get((k + attempt) % PODS.size());
                    try {
                        ok = client.post(pod, "/testbed/queue/offer?jobId=" + jobId, Duration.ofSeconds(6))
                                .path("accepted").asBoolean();
                    } catch (RuntimeException e) {
                        failedAttempts.incrementAndGet();
                        sleepQuietly(100);
                    }
                }
                if (ok) {
                    acknowledged.add(jobId);
                    long now = System.currentTimeMillis();
                    maxGapMs.accumulateAndGet(now - lastSuccess, Math::max);
                    lastSuccess = now;
                }
                sleepQuietly(100);
            }
        }, "rolling-update-load");
        AtomicInteger minReachable = new AtomicInteger(PODS.size());
        AtomicInteger maxLeaders = new AtomicInteger();
        Set<String> leaderHistory = ConcurrentHashMap.newKeySet();
        AtomicLong maxLeaderlessMs = new AtomicLong();
        Thread sampler = new Thread(() -> {
            long lastLeaderSeenAt = System.currentTimeMillis();
            while (!stop.get()) {
                int reachable = 0;
                int leaders = 0;
                for (String pod : PODS) {
                    JsonNode s = cluster.tryStatus(pod, Duration.ofSeconds(2)).orElse(null);
                    if (s == null) continue;
                    reachable++;
                    if (s.path("leaderReady").asBoolean()) {
                        leaders++;
                        leaderHistory.add("term " + s.path("term").asLong() + ": " + s.path("nodeId").asText());
                    }
                }
                minReachable.accumulateAndGet(reachable, Math::min);
                maxLeaders.accumulateAndGet(leaders, Math::max);
                // 리더 부재 시간: 마지막으로 서비스 가능한 리더를 관측한 시각부터 (샘플링 주기만큼 과대 계측될 수 있음)
                long now = System.currentTimeMillis();
                if (leaders > 0) {
                    lastLeaderSeenAt = now;
                } else {
                    maxLeaderlessMs.accumulateAndGet(now - lastLeaderSeenAt, Math::max);
                }
                sleepQuietly(250);
            }
        }, "rolling-update-sampler");

        loader.start();
        sampler.start();
        Thread.sleep(3000);
        long t0 = System.currentTimeMillis();
        Kubectl.run("rollout", "restart", "statefulset/batch-scheduler", "-n", NAMESPACE);
        Kubectl.run("rollout", "status", "statefulset/batch-scheduler", "-n", NAMESPACE, "--timeout=15m");
        long rolloutElapsed = System.currentTimeMillis() - t0;
        cluster.awaitHealthy(Duration.ofMinutes(3));
        Thread.sleep(3000);
        stop.set(true);
        loader.join();
        sampler.join();

        String leader = cluster.awaitLeader();
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200)).untilAsserted(cluster::assertLogsConverged);
        List<String> waiting = withRun(texts(cluster.status(leader).path("queueWaiting")));
        List<String> missing = acknowledged.stream().filter(j -> !waiting.contains(j)).toList();
        int duplicates = waiting.size() - new HashSet<>(waiting).size();
        log.info("TC-K8S-11: rollout {}ms, acknowledged={}, missing={}, duplicates={}, failed attempts={}, max write gap={}ms, "
                        + "min reachable pods={}, max ready leaders={}, max leaderless={}ms, leaders={}",
                rolloutElapsed, acknowledged.size(), missing.size(), duplicates, failedAttempts.get(), maxGapMs.get(),
                minReachable.get(), maxLeaders.get(), maxLeaderlessMs.get(), new java.util.TreeSet<>(leaderHistory));

        assertThat(acknowledged).isNotEmpty();
        assertThat(missing).isEmpty();
        assertThat(minReachable.get()).isGreaterThanOrEqualTo(2);
        assertThat(maxLeaders.get()).isLessThanOrEqualTo(1);
    }

    @Test
    @Order(12)
    @DisplayName("TC-K8S-12: Pod 간 지연·패킷 손실(부분 단절) 중 락·큐 동작 및 오탐 방지 검증")
    void tcK8s12_degradedNetwork() {
        String leader = cluster.awaitLeader();
        long termBefore = cluster.status(leader).path("term").asLong();
        Chaos.netem("netem-" + run, PODS, 50, 10, 5);

        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicInteger maxLeaders = new AtomicInteger();
        AtomicInteger minActiveMembers = new AtomicInteger(PODS.size());
        Thread sampler = new Thread(() -> {
            while (!stop.get()) {
                int leaders = 0;
                for (String pod : PODS) {
                    JsonNode s = cluster.tryStatus(pod, Duration.ofSeconds(2)).orElse(null);
                    if (s != null && s.path("leaderReady").asBoolean()) {
                        leaders++;
                        minActiveMembers.accumulateAndGet(s.path("activeMembers").size(), Math::min);
                    }
                }
                maxLeaders.accumulateAndGet(leaders, Math::max);
                sleepQuietly(250);
            }
        }, "netem-sampler");
        sampler.start();

        int cycles = 40;
        int success = 0;
        List<Long> latencies = new ArrayList<>();
        for (int i = 0; i < cycles; i++) {
            String pod = PODS.get(i % PODS.size());
            long start = System.currentTimeMillis();
            try {
                if (client.post(pod, "/testbed/cycle?lock=netem-lock-" + run + "&jobId=JOB-NETEM-" + run + "-" + i,
                        Duration.ofSeconds(30)).path("success").asBoolean()) {
                    success++;
                }
            } catch (RuntimeException e) {
                log.warn("cycle failed on {}: {}", pod, e.toString());
            }
            latencies.add(System.currentTimeMillis() - start);
        }
        stop.set(true);
        joinQuietly(sampler);
        long termAfter = cluster.status(cluster.awaitLeader()).path("term").asLong();
        Chaos.delete("networkchaos", "netem-" + run);

        List<Long> sorted = latencies.stream().sorted().toList();
        log.info("TC-K8S-12: netem 50±10ms delay + 5% loss: success={}/{}, cycle p50={}ms p95={}ms max={}ms, "
                        + "max ready leaders={}, min active members={}, term {} → {}",
                success, cycles, sorted.get(sorted.size() / 2), sorted.get((int) (sorted.size() * 0.95)),
                sorted.get(sorted.size() - 1), maxLeaders.get(), minActiveMembers.get(), termBefore, termAfter);

        assertThat(success).isEqualTo(cycles);
        assertThat(maxLeaders.get()).isLessThanOrEqualTo(1);
        assertThat(minActiveMembers.get()).isEqualTo(PODS.size());
    }

    // ─── 헬퍼 ─────────────────────────────────────────────────────────────────
    /**
     * 측정 전 워밍업: 측정과 같은 경로로 3개 Pod에 동시 30건을 보내고 결과는 버립니다.
     * QAS-01 환경(평시 피크 타임)은 가동 중인 시스템이므로, 재배포 직후 JIT·커넥션 초기화 지연을 측정에서 제외합니다.
     */
    private void warmUp(java.util.function.IntFunction<String> pathFor) throws InterruptedException {
        int count = 30;
        ExecutorService executor = Executors.newFixedThreadPool(count);
        for (int i = 0; i < count; i++) {
            String pod = PODS.get(i % PODS.size());
            String path = pathFor.apply(i);
            executor.submit(() -> {
                try {
                    client.post(pod, path, Duration.ofSeconds(60));
                } catch (RuntimeException e) {
                    log.warn("warm-up request failed on {}: {}", pod, e.toString());
                }
            });
        }
        executor.shutdown();
        assertThat(executor.awaitTermination(120, TimeUnit.SECONDS)).isTrue();
    }

    private void awaitAllPods(Duration timeout, Predicate<JsonNode> condition) {
        await().atMost(timeout).pollInterval(Duration.ofMillis(100)).ignoreExceptions().until(() ->
                PODS.stream().map(cluster::status).allMatch(condition));
    }

    private List<String> withRun(List<String> ids) {
        return ids.stream().filter(id -> id.contains("-" + run)).toList();
    }

    private String job(int i) {
        return "JOB-" + run + "-" + i;
    }

    private String exec(int i) {
        return "exec-" + run + "-" + i;
    }

    private void record(String pod, String executionId, String jobId, String status, long leaseToken) {
        assertThat(client.post(pod, "/testbed/journal/record?executionId=" + executionId + "&jobId=" + jobId
                + "&status=" + status + "&leaseToken=" + leaseToken).path("accepted").asBoolean()).isTrue();
    }

    private void createExecutionJob(String name, String script) {
        Kubectl.apply("""
                apiVersion: batch/v1
                kind: Job
                metadata:
                  name: %s
                  namespace: %s
                  labels:
                    e2e: execution-target
                spec:
                  backoffLimit: 0
                  ttlSecondsAfterFinished: 600
                  template:
                    metadata:
                      labels:
                        e2e: execution-target
                    spec:
                      restartPolicy: Never
                      terminationGracePeriodSeconds: 1
                      containers:
                        - name: target
                          image: batch-cluster-testbed:e2e
                          imagePullPolicy: IfNotPresent
                          command: ["sh", "-c", "%s"]
                          resources:
                            requests:
                              cpu: 10m
                              memory: 16Mi
                """.formatted(name, EXECUTION_NAMESPACE, script));
    }

    private List<JsonNode> recoveryActions(JsonNode status) {
        List<JsonNode> actions = new ArrayList<>();
        status.path("recoveryActions").forEach(a -> {
            if (!"LOCK_RECLAIMED".equals(a.path("kind").asText()) && a.path("target").asText().contains("-" + run + "-")) {
                actions.add(a);
            }
        });
        return actions;
    }

    private static String actionKind(List<JsonNode> actions, String job) {
        return actions.stream().filter(a -> job.equals(a.path("target").asText()))
                .map(a -> a.path("kind").asText() + " " + a.path("observedStatus").asText())
                .findFirst().orElse("NONE");
    }

    private String writeBehindState(String pod) {
        return cluster.status(pod).path("writeBehind").path("state").asText();
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void joinQuietly(Thread thread) {
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
