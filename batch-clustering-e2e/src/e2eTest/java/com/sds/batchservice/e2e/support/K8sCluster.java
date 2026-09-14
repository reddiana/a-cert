package com.sds.batchservice.e2e.support;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * minikube에 배포된 batch-scheduler StatefulSet 관측·조작 헬퍼.
 *
 * <p>Raft nodeId는 Pod 이름과 같습니다 ({@code RAFT_NODE_ID = metadata.name}).
 */
public final class K8sCluster {

    private static final Logger log = LoggerFactory.getLogger(K8sCluster.class);

    public static final String NAMESPACE = "ns-batchservice";
    public static final String META_DB_NAMESPACE = "ns-meta-db";
    public static final String EXECUTION_NAMESPACE = "ns-batch";
    public static final String BASELINE = "testbed-baseline";
    public static final List<String> PODS = List.of("batch-scheduler-0", "batch-scheduler-1", "batch-scheduler-2");

    private final Path projectDir;
    private final TestbedClient client;

    private K8sCluster(Path projectDir, TestbedClient client) {
        this.projectDir = projectDir;
        this.client = client;
    }

    public static K8sCluster connect() {
        Path projectDir = Path.of(System.getProperty("e2e.projectDir", ".")).toAbsolutePath();
        String nodeIp = Kubectl.run("get", "nodes",
                "-o", "jsonpath={.items[0].status.addresses[?(@.type==\"InternalIP\")].address}");
        Map<String, String> urls = new LinkedHashMap<>();
        for (int i = 0; i < PODS.size(); i++) {
            urls.put(PODS.get(i), "http://" + nodeIp + ":" + (30080 + i));
        }
        urls.put(BASELINE, "http://" + nodeIp + ":30089");
        log.info("K8s node {} / testbed endpoints {}", nodeIp, urls);
        return new K8sCluster(projectDir, new TestbedClient(urls));
    }

    public TestbedClient client() {
        return client;
    }

    public Path path(String relative) {
        return projectDir.resolve(relative);
    }

    // ─── 배포 ─────────────────────────────────────────────────────────────────
    public void ensureDeployed() {
        if (Kubectl.run("get", "statefulset", "batch-scheduler", "-n", NAMESPACE, "--ignore-not-found", "-o", "name").isBlank()) {
            ensureMetaDb();
            Kubectl.run("apply", "-k", path("k8s/overlays/minikube").toString());
        }
    }

    public void ensureMetaDb() {
        Kubectl.run("apply", "-f", path("k8s/overlays/minikube/meta-db.yaml").toString());
        Kubectl.run("rollout", "status", "deployment/meta-db", "-n", META_DB_NAMESPACE, "--timeout=180s");
    }

    /** StatefulSet·WAL PVC를 삭제하고 메타 DB 동기화 테이블을 비웁니다 (신규 클러스터 부트스트랩 준비). */
    public void undeployScheduler() {
        Kubectl.run("delete", "statefulset", "batch-scheduler", "-n", NAMESPACE, "--ignore-not-found", "--wait=true");
        await().atMost(Duration.ofMinutes(2)).pollInterval(Duration.ofSeconds(1)).until(() ->
                Kubectl.run("get", "pods", "-n", NAMESPACE, "-l", "app=batch-scheduler", "-o", "name").isBlank());
        List<String> args = new ArrayList<>(List.of("delete", "pvc", "-n", NAMESPACE, "--ignore-not-found", "--wait=true"));
        PODS.forEach(p -> args.add("wal-storage-" + p));
        Kubectl.run(args.toArray(String[]::new));
        cleanupWalHostPath();
        ensureMetaDb();
        psql("TRUNCATE batch_journal_log, batch_sync_checkpoint");
    }

    /**
     * minikube hostpath 프로비저너는 PVC 이름으로 디렉터리를 만들며, 프로비저너 재시작 등으로 PV가 Released 상태로 남으면
     * 같은 이름의 새 PVC가 이전 WAL을 재사용합니다. 신규 부트스트랩을 보장하기 위해 남은 PV와 디렉터리를 정리합니다.
     */
    private void cleanupWalHostPath() {
        String pvs = Kubectl.run("get", "pv", "-o",
                "jsonpath={range .items[*]}{.metadata.name}|{.spec.claimRef.namespace}|{.spec.claimRef.name}{\"\\n\"}{end}");
        for (String line : pvs.lines().toList()) {
            String[] cols = line.split("\\|", -1);
            if (cols.length == 3 && NAMESPACE.equals(cols[1]) && cols[2].startsWith("wal-storage-")) {
                log.info("Deleting leftover PV {} (claim {})", cols[0], cols[2]);
                Kubectl.run("delete", "pv", cols[0], "--ignore-not-found", "--wait=true");
            }
        }
        Kubectl.run("delete", "job", "wal-hostpath-cleanup", "-n", NAMESPACE, "--ignore-not-found", "--wait=true");
        Kubectl.apply("""
                apiVersion: batch/v1
                kind: Job
                metadata:
                  name: wal-hostpath-cleanup
                  namespace: %s
                spec:
                  backoffLimit: 0
                  template:
                    spec:
                      restartPolicy: Never
                      containers:
                        - name: cleanup
                          image: batch-cluster-testbed:e2e
                          imagePullPolicy: IfNotPresent
                          command: ["sh", "-c", "rm -rf /hostpath/wal-storage-batch-scheduler-*"]
                          volumeMounts:
                            - name: hostpath
                              mountPath: /hostpath
                      volumes:
                        - name: hostpath
                          hostPath:
                            path: /tmp/hostpath-provisioner/%s
                            type: DirectoryOrCreate
                """.formatted(NAMESPACE, NAMESPACE));
        Kubectl.run("wait", "--for=condition=complete", "job/wal-hostpath-cleanup", "-n", NAMESPACE, "--timeout=120s");
        Kubectl.run("delete", "job", "wal-hostpath-cleanup", "-n", NAMESPACE, "--ignore-not-found", "--wait=false");
    }

    public String psql(String sql) {
        return Kubectl.run("exec", "-n", META_DB_NAMESPACE, "deployment/meta-db", "--",
                "psql", "-U", "batch", "-d", "batchdb", "-tAc", sql);
    }

    /** 메타 DB 복제본 수 변경. 1로 복구하면 Pod Ready 시각을 반환합니다. */
    public long scaleMetaDb(int replicas) {
        Kubectl.run("scale", "deployment/meta-db", "-n", META_DB_NAMESPACE, "--replicas=" + replicas);
        if (replicas == 0) {
            await().atMost(Duration.ofMinutes(1)).pollInterval(Duration.ofMillis(200)).until(() ->
                    Kubectl.run("get", "pods", "-n", META_DB_NAMESPACE, "-l", "app=meta-db", "-o", "name").isBlank());
        } else {
            Kubectl.run("rollout", "status", "deployment/meta-db", "-n", META_DB_NAMESPACE, "--timeout=180s");
        }
        return System.currentTimeMillis();
    }

    public int k8sReadyPodCount() {
        String out = Kubectl.run("get", "pods", "-n", NAMESPACE, "-l", "app=batch-scheduler",
                "-o", "jsonpath={range .items[*]}{.status.conditions[?(@.type==\"Ready\")].status}{\"\\n\"}{end}");
        return (int) out.lines().filter("True"::equals).count();
    }

    // ─── 상태 관측 ────────────────────────────────────────────────────────────
    public JsonNode status(String pod) {
        return client.get(pod, "/testbed/status");
    }

    public Optional<JsonNode> tryStatus(String pod, Duration timeout) {
        return client.tryGet(pod, "/testbed/status", timeout);
    }

    /** 서비스 가능한(NO_OP 커밋 완료) 리더 Pod. 여러 개면 가장 높은 term. */
    public Optional<String> findLeaderAmong(Collection<String> pods) {
        return pods.stream()
                .map(p -> tryStatus(p, Duration.ofSeconds(2)).orElse(null))
                .filter(s -> s != null && s.path("leaderReady").asBoolean())
                .max(Comparator.comparingLong(s -> s.path("term").asLong()))
                .map(s -> s.path("nodeId").asText());
    }

    public Optional<String> findLeader() {
        return findLeaderAmong(PODS);
    }

    public String awaitLeader() {
        return await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(100))
                .until(() -> findLeader().orElse(null), l -> l != null);
    }

    /** 응답 가능한 Pod 중 LEADER 역할인 노드 수. */
    public int activeLeaderCount() {
        return (int) PODS.stream()
                .map(p -> tryStatus(p, Duration.ofSeconds(2)).orElse(null))
                .filter(s -> s != null && "LEADER".equals(s.path("role").asText()))
                .count();
    }

    public List<String> others(String pod) {
        return PODS.stream().filter(p -> !p.equals(pod)).toList();
    }

    /** 3개 Pod 모두 K8s Ready, 단일 리더, 리더의 활성 멤버십 3개. */
    public void awaitHealthy(Duration timeout) {
        await().atMost(timeout).pollInterval(Duration.ofSeconds(1)).ignoreExceptions().until(() -> {
            if (k8sReadyPodCount() != PODS.size()) return false;
            List<JsonNode> statuses = PODS.stream().map(this::status).toList();
            long leaders = statuses.stream().filter(s -> s.path("leaderReady").asBoolean()).count();
            boolean allReady = statuses.stream().allMatch(s -> s.path("ready").asBoolean());
            JsonNode leader = statuses.stream().filter(s -> s.path("leaderReady").asBoolean()).findFirst().orElse(null);
            return leaders == 1 && allReady && leader.path("activeMembers").size() == PODS.size();
        });
    }

    /** 모든 Pod의 commitIndex·lastApplied·커밋 구간 로그 term 일치 (클러스터 재수렴). */
    public void assertLogsConverged() {
        List<JsonNode> logs = PODS.stream().map(p -> client.get(p, "/testbed/log-terms")).toList();
        JsonNode first = logs.get(0);
        long commit = first.path("commitIndex").asLong();
        for (JsonNode l : logs) {
            String id = l.path("nodeId").asText();
            assertThat(l.path("commitIndex").asLong()).as(id + " commitIndex").isEqualTo(commit);
            assertThat(l.path("lastApplied").asLong()).as(id + " lastApplied").isEqualTo(commit);
            assertThat(l.path("terms")).as(id + " log terms").isEqualTo(first.path("terms"));
        }
    }

    // ─── 장애 주입 ────────────────────────────────────────────────────────────
    /**
     * 컨테이너를 SIGKILL로 종료하고, 대상 Pod가 마지막으로 응답한 시각(T0)을 반환합니다.
     *
     * <p>20ms 주기로 대상 JVM에 요청하여 마지막 정상 응답 시각을 장애 발생 시점으로 사용합니다
     * (Chaos 리소스 생성부터 실제 종료까지의 반영 지연을 측정 구간에서 제외).
     */
    public long crash(String pod) {
        long jvmStartTime = client.get(pod, "/testbed/ping").path("startTime").asLong();
        AtomicLong lastOk = new AtomicLong(System.currentTimeMillis());
        AtomicBoolean down = new AtomicBoolean(false);
        Thread observer = new Thread(() -> {
            while (!down.get()) {
                Optional<JsonNode> ping = client.tryGet(pod, "/testbed/ping", Duration.ofMillis(500));
                if (ping.isPresent() && ping.get().path("startTime").asLong() == jvmStartTime) {
                    lastOk.set(System.currentTimeMillis());
                } else {
                    down.set(true);
                    return;
                }
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }, "crash-observer-" + pod);
        observer.setDaemon(true);
        observer.start();
        long injectAt = System.currentTimeMillis();
        Chaos.containerKill("kill-" + pod + "-" + injectAt, pod);
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(10)).untilTrue(down);
        log.info("container-kill [{}]: chaos created → last response +{}ms", pod, lastOk.get() - injectAt);
        return lastOk.get();
    }

    /**
     * 크래시 후 재시작된 컨테이너가 복구 판정 전에 재합류하지 않도록 대상 Pod를 나머지 Pod와 분할합니다
     * (장애 지속 상태 재현). 분할은 {@link Chaos#deleteAll()}로 해제합니다.
     */
    public long crashAndHoldDown(String pod) {
        long t0 = crash(pod);
        Chaos.partition("hold-down-" + pod + "-" + t0, pod, others(pod));
        return t0;
    }

    public static List<String> texts(JsonNode array) {
        List<String> values = new ArrayList<>();
        array.forEach(n -> values.add(n.asText()));
        return values;
    }
}
