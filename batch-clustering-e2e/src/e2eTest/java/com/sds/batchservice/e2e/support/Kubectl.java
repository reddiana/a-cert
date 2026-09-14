package com.sds.batchservice.e2e.support;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * kubectl 실행 래퍼. 현재 kubeconfig 컨텍스트(minikube)를 대상으로 합니다.
 */
public final class Kubectl {

    private static final long TIMEOUT_MINUTES = 20;

    private Kubectl() {
    }

    public static String run(String... args) {
        return exec(null, args);
    }

    public static Optional<String> tryRun(String... args) {
        try {
            return Optional.of(exec(null, args));
        } catch (IllegalStateException e) {
            return Optional.empty();
        }
    }

    public static String apply(String manifest) {
        return exec(manifest, "apply", "-f", "-");
    }

    private static String exec(String stdin, String... args) {
        List<String> command = new ArrayList<>();
        command.add("kubectl");
        command.addAll(List.of(args));
        try {
            Process process = new ProcessBuilder(command).start();
            CompletableFuture<String> stdout = readAsync(process.getInputStream());
            CompletableFuture<String> stderr = readAsync(process.getErrorStream());
            try (OutputStream os = process.getOutputStream()) {
                if (stdin != null) {
                    os.write(stdin.getBytes(StandardCharsets.UTF_8));
                }
            }
            if (!process.waitFor(TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
                process.destroyForcibly();
                throw new IllegalStateException("kubectl timed out: " + String.join(" ", command));
            }
            if (process.exitValue() != 0) {
                throw new IllegalStateException("kubectl failed (exit " + process.exitValue() + "): "
                        + String.join(" ", command) + "\n" + stderr.join() + stdout.join());
            }
            return stdout.join().trim();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted: " + String.join(" ", command), e);
        }
    }

    private static CompletableFuture<String> readAsync(InputStream in) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return new String(in.readAllBytes(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }
}
