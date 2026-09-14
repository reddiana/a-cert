package com.sds.batchservice.testbed;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sds.batchservice.cluster.recovery.ExecutionStatusProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;

/**
 * K8s Job API 기반 Execution Target 상태 조회 (5.4.2-5 RBAC, 5.3.2 Scenario 2).
 *
 * <p>executionId를 Execution 네임스페이스의 Job 이름으로 사용합니다. ServiceAccount 토큰으로 API Server를 직접 호출하여
 * 클라이언트 라이브러리 의존성을 추가하지 않습니다.
 */
public class K8sJobExecutionStatusProvider implements ExecutionStatusProvider {

    private static final Logger log = LoggerFactory.getLogger(K8sJobExecutionStatusProvider.class);
    private static final Path SERVICE_ACCOUNT = Path.of("/var/run/secrets/kubernetes.io/serviceaccount");

    private final String namespace;
    private final ObjectMapper objectMapper;
    private final String apiServer;
    private final HttpClient http;

    public K8sJobExecutionStatusProvider(String namespace, ObjectMapper objectMapper) {
        this.namespace = namespace;
        this.objectMapper = objectMapper;
        this.apiServer = "https://" + System.getenv("KUBERNETES_SERVICE_HOST") + ":" + System.getenv("KUBERNETES_SERVICE_PORT");
        this.http = HttpClient.newBuilder()
                .sslContext(serviceAccountSslContext())
                .connectTimeout(Duration.ofSeconds(1))
                .build();
    }

    @Override
    public ExecutionStatus getStatus(String executionId, String jobId) {
        try {
            HttpRequest request = HttpRequest.newBuilder(
                            URI.create(apiServer + "/apis/batch/v1/namespaces/" + namespace + "/jobs/" + executionId))
                    .header("Authorization", "Bearer " + Files.readString(SERVICE_ACCOUNT.resolve("token")).trim())
                    .timeout(Duration.ofSeconds(2))
                    .GET()
                    .build();
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 404) {
                return ExecutionStatus.NOT_FOUND;
            }
            if (response.statusCode() != 200) {
                log.warn("Job [{}/{}] lookup failed: HTTP {}", namespace, executionId, response.statusCode());
                return ExecutionStatus.UNKNOWN;
            }
            return toStatus(objectMapper.readTree(response.body()).path("status"));
        } catch (IOException e) {
            log.warn("Job [{}/{}] lookup failed: {}", namespace, executionId, e.toString());
            return ExecutionStatus.UNKNOWN;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ExecutionStatus.UNKNOWN;
        }
    }

    private static ExecutionStatus toStatus(JsonNode status) {
        if (status.path("succeeded").asInt(0) > 0) {
            return ExecutionStatus.COMPLETED;
        }
        for (JsonNode condition : status.path("conditions")) {
            if (!"True".equals(condition.path("status").asText())) continue;
            switch (condition.path("type").asText()) {
                case "Complete", "SuccessCriteriaMet" -> { return ExecutionStatus.COMPLETED; }
                case "Failed", "FailureTarget" -> { return ExecutionStatus.FAILED; }
                default -> { }
            }
        }
        return ExecutionStatus.RUNNING;
    }

    private static SSLContext serviceAccountSslContext() {
        try (InputStream in = Files.newInputStream(SERVICE_ACCOUNT.resolve("ca.crt"))) {
            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStore.load(null, null);
            int i = 0;
            for (Certificate cert : CertificateFactory.getInstance("X.509").generateCertificates(in)) {
                trustStore.setCertificateEntry("k8s-ca-" + i++, cert);
            }
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, tmf.getTrustManagers(), null);
            return context;
        } catch (IOException | GeneralSecurityException e) {
            throw new IllegalStateException("Cannot load ServiceAccount CA certificate", e);
        }
    }
}
