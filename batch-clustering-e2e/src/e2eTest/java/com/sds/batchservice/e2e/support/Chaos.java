package com.sds.batchservice.e2e.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

import static org.awaitility.Awaitility.await;

/**
 * Chaos Mesh 장애 주입 (PodChaos / NetworkChaos).
 */
public final class Chaos {

    private static final Logger log = LoggerFactory.getLogger(Chaos.class);
    private static final String NS = K8sCluster.NAMESPACE;

    private Chaos() {
    }

    /** 컨테이너 SIGKILL (프로세스 크래시). 컨테이너는 kubelet에 의해 동일 Pod·PVC로 재시작됩니다. */
    public static void containerKill(String name, String pod) {
        Kubectl.apply("""
                apiVersion: chaos-mesh.org/v1alpha1
                kind: PodChaos
                metadata:
                  name: %s
                  namespace: %s
                spec:
                  action: container-kill
                  mode: all
                  containerNames: [batch-scheduler]
                  selector:
                    pods:
                      %s: [%s]
                """.formatted(name, NS, NS, pod));
    }

    /** pod ↔ targets 양방향 네트워크 분할. 주입 완료까지 대기합니다. */
    public static void partition(String name, String pod, List<String> targets) {
        Kubectl.apply("""
                apiVersion: chaos-mesh.org/v1alpha1
                kind: NetworkChaos
                metadata:
                  name: %s
                  namespace: %s
                spec:
                  action: partition
                  mode: all
                  selector:
                    pods:
                      %s: [%s]
                  direction: both
                  target:
                    mode: all
                    selector:
                      pods:
                        %s: [%s]
                """.formatted(name, NS, NS, pod, NS, String.join(", ", targets)));
        awaitInjected("networkchaos", name);
    }

    /** pods 간 트래픽에 지연·손실 주입 (부분 단절). 주입 완료까지 대기합니다. */
    public static void netem(String name, List<String> pods, int delayMs, int jitterMs, int lossPercent) {
        String podList = String.join(", ", pods);
        Kubectl.apply("""
                apiVersion: chaos-mesh.org/v1alpha1
                kind: NetworkChaos
                metadata:
                  name: %s
                  namespace: %s
                spec:
                  action: netem
                  mode: all
                  selector:
                    pods:
                      %s: [%s]
                  direction: to
                  target:
                    mode: all
                    selector:
                      pods:
                        %s: [%s]
                  delay:
                    latency: "%dms"
                    jitter: "%dms"
                    correlation: "0"
                  loss:
                    loss: "%d"
                    correlation: "0"
                """.formatted(name, NS, NS, podList, NS, podList, delayMs, jitterMs, lossPercent));
        awaitInjected("networkchaos", name);
    }

    public static void delete(String kind, String name) {
        Kubectl.run("delete", kind, name, "-n", NS, "--ignore-not-found", "--wait=true");
    }

    public static void deleteAll() {
        String existing = Kubectl.run("get", "networkchaos,podchaos", "-n", NS, "-o", "name");
        if (!existing.isBlank()) {
            log.info("Removing chaos resources: {}", existing.replace('\n', ' '));
            Kubectl.run("delete", "networkchaos,podchaos", "--all", "-n", NS, "--wait=true");
        }
    }

    private static void awaitInjected(String kind, String name) {
        long start = System.currentTimeMillis();
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(200)).until(() -> "True".equals(
                Kubectl.run("get", kind, name, "-n", NS,
                        "-o", "jsonpath={.status.conditions[?(@.type==\"AllInjected\")].status}")));
        log.info("{} [{}] injected in {}ms", kind, name, System.currentTimeMillis() - start);
    }
}
