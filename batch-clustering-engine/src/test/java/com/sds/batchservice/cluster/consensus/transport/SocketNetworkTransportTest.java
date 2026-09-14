package com.sds.batchservice.cluster.consensus.transport;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 송신 연결 회복 회귀 테스트 (K8s Pod 재생성 시 구 IP로의 반개방 연결에 메시지가 유실되던 결함).
 *
 * <p>테스트가 피어 역할의 ServerSocket을 직접 열어 연결 종료·무응답 상황을 만듭니다.
 */
class SocketNetworkTransportTest {

    private static final int MAGIC = 0x52414654;

    @Test
    @DisplayName("피어가 연결을 종료하면 송신할 메시지가 없어도 재연결한다")
    void reconnectsWhenPeerClosesConnection() throws Exception {
        try (ServerSocket peer = new ServerSocket(0)) {
            SocketNetworkTransport transport = new SocketNetworkTransport("node-a", 0,
                    Map.of("node-b", "127.0.0.1:" + peer.getLocalPort()), "token");
            transport.start();
            try {
                peer.setSoTimeout(5_000);
                Socket first = peer.accept();
                assertThat(readHandshakeNodeId(first)).isEqualTo("node-a");
                first.close(); // 피어 종료 (FIN)

                peer.setSoTimeout(2_000);
                try (Socket second = peer.accept()) {
                    assertThat(readHandshakeNodeId(second)).isEqualTo("node-a");
                }
            } finally {
                transport.stop();
            }
        }
    }

    @Test
    @DisplayName("송신 후 피어로부터 수신이 없으면 연결을 재수립한다")
    void reconnectsWhenPeerStopsResponding() throws Exception {
        try (ServerSocket peer = new ServerSocket(0)) {
            SocketNetworkTransport transport = new SocketNetworkTransport("node-a", 0,
                    Map.of("node-b", "127.0.0.1:" + peer.getLocalPort()), "token");
            transport.start();
            ScheduledExecutorService sender = Executors.newSingleThreadScheduledExecutor();
            try {
                peer.setSoTimeout(5_000);
                Socket first = peer.accept();
                assertThat(readHandshakeNodeId(first)).isEqualTo("node-a");
                first.setSoTimeout(0);
                Thread drain = new Thread(() -> {
                    try {
                        while (first.getInputStream().read() >= 0) {
                            // 수신만 하고 응답하지 않는 피어
                        }
                    } catch (IOException ignored) {
                    }
                });
                drain.setDaemon(true);
                drain.start();

                long start = System.nanoTime();
                sender.scheduleAtFixedRate(() -> transport.send(Message.preVote("node-a", "node-b", 1L, 0L, 0L)),
                        0L, 200L, TimeUnit.MILLISECONDS);

                peer.setSoTimeout(8_000);
                try (Socket second = peer.accept()) {
                    long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
                    assertThat(readHandshakeNodeId(second)).isEqualTo("node-a");
                    assertThat(elapsedMs).as("reconnect after peer silence timeout").isGreaterThanOrEqualTo(2_500L);
                }
                first.close();
            } finally {
                sender.shutdownNow();
                transport.stop();
            }
        }
    }

    private static String readHandshakeNodeId(Socket socket) throws IOException {
        socket.setSoTimeout(5_000);
        DataInputStream in = new DataInputStream(socket.getInputStream());
        assertThat(in.readInt()).isEqualTo(MAGIC);
        String nodeId = in.readUTF();
        in.readFully(new byte[in.readInt()]);
        return nodeId;
    }
}
