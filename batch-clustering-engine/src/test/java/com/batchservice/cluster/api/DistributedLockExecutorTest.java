package com.batchservice.cluster.api;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class DistributedLockExecutorTest {
    private static final Logger log = LoggerFactory.getLogger(DistributedLockExecutorTest.class);

    @Autowired
    private DistributedLockExecutor distributedLockExecutor;

    private final List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());

    @Test
    @DisplayName("Hazelcast 대체 검증: Spring Boot 주입 기반 DistributedLockExecutor 상호 배제 동작 테스트")
    void testLock() throws Exception {
        Thread thread1 = new Thread(() -> {
            String logStr = "lockTest#1";
            distributedLockExecutor.execute("testLock", logStr, () -> {
                testRun(logStr);
                return null;
            });
        });

        Thread thread2 = new Thread(() -> {
            String logStr = "lockTest#2";
            distributedLockExecutor.execute("testLock", logStr, () -> {
                testRun(logStr);
                return null;
            });
        });

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        // 2개 스레드가 각각 5회씩 총 10개 메시지를 출력하며, 한 스레드의 작업이 완전히 끝난 후 다른 스레드가 진입했는지 검증
        assertThat(executionOrder).hasSize(10);
        String firstThread = executionOrder.get(0).split(" ")[0];
        for (int i = 0; i < 5; i++) {
            assertThat(executionOrder.get(i)).startsWith(firstThread);
        }
    }

    private void testRun(String myName) {
        for (int i = 0; i < 5; i++) {
            String msg = myName + " message #" + (i + 1);
            executionOrder.add(msg);
            log.info("Hello, {}! This is message number {}", myName, (i + 1));
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
