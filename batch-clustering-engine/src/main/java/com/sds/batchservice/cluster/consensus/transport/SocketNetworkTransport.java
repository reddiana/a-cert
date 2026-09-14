package com.sds.batchservice.cluster.consensus.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * 단일 Raft TCP 포트(기본 7800) 기반 P2P 전송 계층 (DD-01, 5.3.2 공통 런타임 규약 3).
 *
 * <ul>
 *   <li>피어별 영속 연결 1개와 전용 송신 스레드 → 메시지 순서 보장, 연결 비용 제거</li>
 *   <li>길이 프리픽스 바이너리 프레임 ({@link Message#writeTo}) — Java 기본 직렬화 미사용</li>
 *   <li>연결 수립 시 클러스터 공유 토큰 핸드셰이크, 발신자 ID 위조 프레임 차단</li>
 *   <li>송신 연결 회복: 피어의 연결 종료(FIN)를 감시하고, 송신 후 피어 수신이 {@value #PEER_SILENCE_TIMEOUT_MS}ms
 *       동안 없으면 재연결 (Pod 재생성으로 구 IP에 남은 반개방 연결에 메시지가 유실되는 것을 방지)</li>
 * </ul>
 * 전송 구간 암호화가 필요하면 서비스 메시(mTLS) 또는 전용 네트워크 정책을 함께 적용합니다.
 */
public class SocketNetworkTransport implements NetworkTransport {
    private static final Logger log = LoggerFactory.getLogger(SocketNetworkTransport.class);

    private static final int MAGIC = 0x52414654; // "RAFT"
    private static final int MAX_FRAME_BYTES = 64 * 1024 * 1024;
    private static final int OUTBOUND_QUEUE_CAPACITY = 10_000;
    private static final int CONNECT_TIMEOUT_MS = 500;
    /** 송신 이후 피어로부터 수신이 없을 때 송신 경로 단절로 판정하는 시간 (Raft RPC는 모두 요청·응답 쌍) */
    static final long PEER_SILENCE_TIMEOUT_MS = 3_000L;
    private static final long IDLE_CHECK_INTERVAL_MS = 250L;

    private final String localNodeId;
    private final int port;
    private final Map<String, String> peerHostMap;
    private final byte[] authToken;
    private final ConcurrentHashMap<String, Consumer<Message>> receivers = new ConcurrentHashMap<>();
    private final Map<String, PeerConnection> outbound = new ConcurrentHashMap<>();
    /** 피어별 마지막 수신 프레임 시각 (단조 시계, ms) */
    private final Map<String, Long> lastInboundAt = new ConcurrentHashMap<>();
    private final ExecutorService ioPool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "raft-io");
        t.setDaemon(true);
        return t;
    });

    private volatile boolean running;
    private ServerSocket serverSocket;

    /**
     * @param peerHostMap 로컬 노드를 제외한 nodeId → "host:port"
     * @param authToken   클러스터 공유 토큰 (모든 노드 동일)
     */
    public SocketNetworkTransport(String localNodeId, int port, Map<String, String> peerHostMap, String authToken) {
        this.localNodeId = localNodeId;
        this.port = port;
        this.peerHostMap = Map.copyOf(peerHostMap);
        this.authToken = (authToken == null ? "" : authToken).getBytes(StandardCharsets.UTF_8);
    }

    public synchronized void start() throws IOException {
        if (running) return;
        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(port));
        running = true;
        ioPool.submit(this::acceptLoop);
        peerHostMap.forEach((peerId, hostPort) -> {
            PeerConnection pc = new PeerConnection(peerId, hostPort);
            outbound.put(peerId, pc);
            ioPool.submit(pc::run);
        });
        log.info("[{}] SocketNetworkTransport listening on TCP port {} (peers={})", localNodeId, port, peerHostMap.keySet());
    }

    public synchronized void stop() {
        running = false;
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException ignored) {
        }
        outbound.values().forEach(PeerConnection::close);
        ioPool.shutdownNow();
        log.info("[{}] SocketNetworkTransport stopped.", localNodeId);
    }

    private static long monotonicMs() {
        return System.nanoTime() / 1_000_000L;
    }

    public int getLocalPort() {
        return serverSocket != null ? serverSocket.getLocalPort() : port;
    }

    @Override
    public void send(Message message) {
        PeerConnection pc = outbound.get(message.getReceiverId());
        if (pc != null && running) {
            pc.enqueue(message);
        }
    }

    @Override
    public void registerReceiver(String nodeId, Consumer<Message> receiver) {
        receivers.put(nodeId, receiver);
    }

    @Override
    public void unregisterReceiver(String nodeId) {
        receivers.remove(nodeId);
    }

    // ─── Inbound ──────────────────────────────────────────────────────────────
    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                socket.setTcpNoDelay(true);
                ioPool.submit(() -> readLoop(socket));
            } catch (IOException e) {
                if (running) log.warn("[{}] accept failed: {}", localNodeId, e.getMessage());
            }
        }
    }

    private void readLoop(Socket socket) {
        String remoteNode = null;
        try (socket; DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 1 << 16))) {
            socket.setSoTimeout(5_000);
            if (in.readInt() != MAGIC) throw new IOException("bad magic");
            remoteNode = in.readUTF();
            int tokenLength = in.readInt();
            if (tokenLength < 0 || tokenLength > 4096) throw new IOException("bad token length");
            byte[] token = new byte[tokenLength];
            in.readFully(token);
            if (!MessageDigest.isEqual(token, authToken) || !peerHostMap.containsKey(remoteNode)) {
                log.warn("[{}] Rejected unauthenticated connection from {} (node={})",
                        localNodeId, socket.getRemoteSocketAddress(), remoteNode);
                return;
            }
            socket.setSoTimeout(0);
            while (running) {
                int length = in.readInt();
                if (length <= 0 || length > MAX_FRAME_BYTES) throw new IOException("bad frame length " + length);
                byte[] body = new byte[length];
                in.readFully(body);
                Message message = Message.readFrom(new DataInputStream(new ByteArrayInputStream(body)));
                if (!remoteNode.equals(message.getSenderId())) {
                    throw new IOException("sender id mismatch: " + message.getSenderId());
                }
                lastInboundAt.put(remoteNode, monotonicMs());
                Consumer<Message> receiver = receivers.get(localNodeId);
                if (receiver != null) {
                    receiver.accept(message);
                }
            }
        } catch (IOException e) {
            if (running) log.debug("[{}] inbound connection from {} closed: {}", localNodeId, remoteNode, e.getMessage());
        } catch (RuntimeException e) {
            log.warn("[{}] inbound handler error from {}: {}", localNodeId, remoteNode, e.toString());
        }
    }

    // ─── Outbound ─────────────────────────────────────────────────────────────
    private final class PeerConnection {
        private final String peerId;
        private final String host;
        private final int peerPort;
        private final LinkedBlockingQueue<Message> queue = new LinkedBlockingQueue<>(OUTBOUND_QUEUE_CAPACITY);
        private volatile Socket socket;

        PeerConnection(String peerId, String hostPort) {
            int sep = hostPort.lastIndexOf(':');
            this.peerId = peerId;
            this.host = hostPort.substring(0, sep);
            this.peerPort = Integer.parseInt(hostPort.substring(sep + 1));
        }

        void enqueue(Message m) {
            while (!queue.offer(m)) {
                queue.poll(); // 포화 시 가장 오래된 메시지 폐기 (Raft는 재전송으로 복구)
            }
        }

        void run() {
            long backoff = 100L;
            while (running) {
                try {
                    Socket s = new Socket();
                    s.connect(new InetSocketAddress(host, peerPort), CONNECT_TIMEOUT_MS);
                    s.setTcpNoDelay(true);
                    socket = s;
                    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream(), 1 << 16));
                    out.writeInt(MAGIC);
                    out.writeUTF(localNodeId);
                    out.writeInt(authToken.length);
                    out.write(authToken);
                    out.flush();
                    backoff = 100L;
                    long connectedAt = monotonicMs();
                    long lastWriteAt = Long.MIN_VALUE;
                    watchPeerClose(s);
                    while (running) {
                        Message first = queue.poll(IDLE_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
                        if (first != null) {
                            writeFrame(out, first);
                            Message next;
                            while ((next = queue.poll()) != null) {
                                writeFrame(out, next);
                            }
                            out.flush();
                            lastWriteAt = monotonicMs();
                        }
                        if (s.isClosed()) {
                            throw new IOException("closed by peer");
                        }
                        long heardAt = Math.max(connectedAt, lastInboundAt.getOrDefault(peerId, Long.MIN_VALUE));
                        long silentMs = monotonicMs() - heardAt;
                        if (lastWriteAt > heardAt && silentMs >= PEER_SILENCE_TIMEOUT_MS) {
                            log.info("[{}] No traffic from [{}] for {}ms after sending. Reconnecting to {}:{}.",
                                    localNodeId, peerId, silentMs, host, peerPort);
                            throw new IOException("peer silent for " + silentMs + "ms");
                        }
                    }
                } catch (InterruptedException e) {
                    return;
                } catch (IOException e) {
                    log.debug("[{}] connection to [{}] {}:{} failed: {}", localNodeId, peerId, host, peerPort, e.getMessage());
                    close();
                    try {
                        Thread.sleep(backoff);
                    } catch (InterruptedException ie) {
                        return;
                    }
                    backoff = Math.min(backoff * 2, 1_000L);
                }
            }
        }

        /**
         * 피어는 송신 연결로 데이터를 보내지 않으므로 read 반환(-1)이나 오류는 연결 종료를 의미합니다.
         * 송신만 하는 소켓은 피어의 FIN을 인지하지 못해, 피어 IP가 사라지면 쓰기가 오류 없이 유실되기 때문입니다.
         */
        private void watchPeerClose(Socket s) {
            ioPool.submit(() -> {
                try {
                    s.getInputStream().read();
                } catch (IOException ignored) {
                    // 종료 또는 로컬 close
                }
                if (running && socket == s) {
                    log.info("[{}] Connection to [{}] {}:{} closed by peer. Reconnecting.", localNodeId, peerId, host, peerPort);
                    close();
                }
            });
        }

        private void writeFrame(DataOutputStream out, Message m) throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(256);
            m.writeTo(new DataOutputStream(bytes));
            out.writeInt(bytes.size());
            bytes.writeTo(out);
        }

        void close() {
            Socket s = socket;
            socket = null;
            if (s != null) {
                try {
                    s.close();
                } catch (IOException ignored) {
                }
            }
        }
    }
}
