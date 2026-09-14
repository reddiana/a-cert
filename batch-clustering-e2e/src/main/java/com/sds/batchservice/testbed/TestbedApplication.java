package com.sds.batchservice.testbed;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sds.batchservice.cluster.recovery.ExecutionStatusProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * K8s 통합 검증용 수요처 애플리케이션 (7.1.6).
 *
 * <p>엔진은 수요처와 동일하게 Spring Boot 자동 구성({@code BatchClusteringAutoConfiguration})으로 주입됩니다.
 */
@SpringBootApplication
public class TestbedApplication {

    public static void main(String[] args) {
        SpringApplication.run(TestbedApplication.class, args);
    }

    /** Execution Target(K8s Job) 상태 조회. 미설정 시 엔진 기본 구현(UNKNOWN → 재적재)을 사용합니다. */
    @Bean
    @ConditionalOnProperty(prefix = "testbed.execution-target", name = "namespace")
    public ExecutionStatusProvider executionStatusProvider(
            @Value("${testbed.execution-target.namespace}") String namespace, ObjectMapper objectMapper) {
        return new K8sJobExecutionStatusProvider(namespace, objectMapper);
    }
}
