package com.batchservice.cluster.consensus.transport;

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
import java.util.function.Consumer;

/**
 * 단일 Raft TCP 포트(기본 7800) 기반 P2P 전송 계층 (DD-01, 5.3.2 공통 런타임 규약 3).
 *
 * <ul>
 *   <li>피어별 영속 연결 1개와 전용 송신 스레드 → 메시지 순서 보장, 연결 비용 제거</li>
 *   <li>길이 프리픽스 바이너리 프레임 ({@link Message#writeTo}) — Java 기본 직렬화 미사용</li>
 *   <li>연결 수립 시 클러스터 공유 토큰 핸드셰이크, 발신자 ID 위조 프레임 차단</li>
 * </ul>
 * 전송 구간 암호화가 필요하면 서비스 메시(mTLS) 또는 전용 네트워크 정책을 함께 적용합니다.
 */
public class SocketNetworkTransport implements NetworkTransport {
    private static final Logger log = LoggerFactory.getLogger(SocketNetworkTransport.class);

    private static final int MAGIC = 0x52414654; // "RAFT"
    private static final int MAX_FRAME_BYTES = 64 * 1024 * 1024;
    private static final int OUTBOUND_QUEUE_CAPACITY = 10_000;
    private static final int CONNECT_TIMEOUT_MS = 500;

    private final String localNodeId;
    private final int port;
    private final Map<String, String> peerHostMap;
    private final byte[] authToken;
    private final ConcurrentHashMap<String, Consumer<Message>> receivers = new ConcurrentHashMap<>();
    private final Map<String, PeerConnection> outbound = new ConcurrentHashMap<>();
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
                    while (running) {
                        writeFrame(out, queue.take());
                        Message next;
                        while ((next = queue.poll()) != null) {
                            writeFrame(out, next);
                        }
                        out.flush();
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
