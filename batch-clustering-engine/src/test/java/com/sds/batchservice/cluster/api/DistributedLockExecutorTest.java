package com.sds.batchservice.cluster.api;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * AS-IS Hazelcast 기반 {@code DistributedLockExecutor}와의 API·동작 호환 검증 (NR-05).
 */
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

    @Test
    @DisplayName("Hazelcast 호환: 서로 다른 스레드가 같은 두 번째 인자(logStr)를 넘겨도 상호 배제")
    void sameLogStrFromDifferentThreadsIsMutuallyExclusive() throws Exception {
        AtomicInteger inside = new AtomicInteger();
        AtomicInteger maxInside = new AtomicInteger();
        Runnable critical = () -> {
            maxInside.accumulateAndGet(inside.incrementAndGet(), Math::max);
            sleep(300);
            inside.decrementAndGet();
        };

        Thread thread1 = new Thread(() -> distributedLockExecutor.execute("sameLogStrLock", "batch", critical));
        Thread thread2 = new Thread(() -> distributedLockExecutor.execute("sameLogStrLock", "batch", critical));
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

        assertThat(maxInside.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("Hazelcast 호환: 같은 스레드의 중첩 호출은 재진입하며, 가장 바깥 호출이 끝나야 해제")
    void nestedExecuteOnSameThreadIsReentrant() throws Exception {
        AtomicBoolean otherEntered = new AtomicBoolean();
        AtomicBoolean enteredBeforeOuterExit = new AtomicBoolean();
        Thread other = new Thread(() ->
                distributedLockExecutor.execute("reentrantLock", "other", () -> otherEntered.set(true)));

        String result = distributedLockExecutor.execute("reentrantLock", "outer", () -> {
            String inner = distributedLockExecutor.execute("reentrantLock", "inner", () -> "inner-done");
            other.start();
            sleep(500);
            enteredBeforeOuterExit.set(otherEntered.get());
            return inner;
        });
        other.join();

        assertThat(result).isEqualTo("inner-done");
        assertThat(enteredBeforeOuterExit.get()).isFalse();
        assertThat(otherEntered.get()).isTrue();
    }

    @Test
    @DisplayName("Hazelcast 호환: 기본 대기는 무기한이므로 5초를 넘는 임계 구역 뒤에도 대기자가 획득")
    void defaultWaitHasNoTimeout() throws Exception {
        CountDownLatch holderEntered = new CountDownLatch(1);
        AtomicReference<Throwable> waiterError = new AtomicReference<>();
        AtomicBoolean waiterEntered = new AtomicBoolean();

        Thread holder = new Thread(() -> distributedLockExecutor.execute("longHoldLock", "holder", () -> {
            holderEntered.countDown();
            sleep(6_000);
        }));
        holder.start();
        assertThat(holderEntered.await(5, TimeUnit.SECONDS)).isTrue();

        Thread waiter = new Thread(() -> {
            try {
                distributedLockExecutor.execute("longHoldLock", "waiter", () -> waiterEntered.set(true));
            } catch (Throwable t) {
                waiterError.set(t);
            }
        });
        waiter.start();
        holder.join();
        waiter.join();

        assertThat(waiterError.get()).isNull();
        assertThat(waiterEntered.get()).isTrue();
    }

    private void testRun(String myName) {
        for (int i = 0; i < 5; i++) {
            String msg = myName + " message #" + (i + 1);
            executionOrder.add(msg);
            log.info("Hello, {}! This is message number {}", myName, (i + 1));
            sleep(100);
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
