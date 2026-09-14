package com.sds.batchservice.e2e.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * Pod별 Testbed API 클라이언트 (NodePort 경유).
 */
public final class TestbedClient {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);

    private final Map<String, String> baseUrls;
    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(2))
            .build();

    public TestbedClient(Map<String, String> baseUrls) {
        this.baseUrls = baseUrls;
    }

    public JsonNode get(String target, String path) {
        return send(target, "GET", path, DEFAULT_TIMEOUT);
    }

    public JsonNode post(String target, String path) {
        return send(target, "POST", path, DEFAULT_TIMEOUT);
    }

    public JsonNode post(String target, String path, Duration timeout) {
        return send(target, "POST", path, timeout);
    }

    /** 연결 실패·타임아웃·오류 응답이면 empty. */
    public Optional<JsonNode> tryGet(String target, String path, Duration timeout) {
        try {
            return Optional.of(send(target, "GET", path, timeout));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private JsonNode send(String target, String method, String path, Duration timeout) {
        String base = baseUrls.get(target);
        if (base == null) {
            throw new IllegalArgumentException("Unknown target: " + target);
        }
        HttpRequest request = HttpRequest.newBuilder(URI.create(base + path))
                .timeout(timeout)
                .method(method, HttpRequest.BodyPublishers.noBody())
                .build();
        try {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            JsonNode body = response.body().isEmpty() ? MAPPER.nullNode() : MAPPER.readTree(response.body());
            if (response.statusCode() >= 400) {
                throw new TestbedException(target, path, response.statusCode(), body);
            }
            return body;
        } catch (IOException e) {
            throw new UncheckedIOException(target + " " + method + " " + path + ": " + e, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /** Testbed API 오류 응답 (엔진 예외 클래스명 포함). */
    public static final class TestbedException extends RuntimeException {
        private final int status;
        private final JsonNode body;

        TestbedException(String target, String path, int status, JsonNode body) {
            super(target + " " + path + " → HTTP " + status + " " + body);
            this.status = status;
            this.body = body;
        }

        public int status() {
            return status;
        }

        public String error() {
            return body.path("error").asText();
        }
    }
}
