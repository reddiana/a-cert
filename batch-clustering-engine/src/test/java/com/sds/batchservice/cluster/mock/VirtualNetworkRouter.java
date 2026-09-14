package com.sds.batchservice.cluster.mock;

import com.sds.batchservice.cluster.consensus.transport.Message;
import com.sds.batchservice.cluster.consensus.transport.NetworkTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * 단일 JVM 내 가상 네트워크 라우터 (7.1.3 VirtualNetworkRouter).
 *
 * <p>실제 TCP 영속 연결과 동일하게 수신 노드별 단일 스레드로 순서대로 전달하며,
 * 노드 격리(isolateNode)로 네트워크 분할을 재현합니다.
 */
public class VirtualNetworkRouter implements NetworkTransport {
    private static final Logger log = LoggerFactory.getLogger(VirtualNetworkRouter.class);

    private final Map<String, Consumer<Message>> receivers = new ConcurrentHashMap<>();
    private final Map<String, ExecutorService> dispatchers = new ConcurrentHashMap<>();
    private final Set<String> isolatedNodes = ConcurrentHashMap.newKeySet();
    private volatile boolean shutdown;

    @Override
    public void registerReceiver(String nodeId, Consumer<Message> receiver) {
        receivers.put(nodeId, receiver);
        dispatchers.computeIfAbsent(nodeId, id -> Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "vnet-" + id);
            t.setDaemon(true);
            return t;
        }));
    }

    @Override
    public void unregisterReceiver(String nodeId) {
        receivers.remove(nodeId);
    }

    @Override
    public void send(Message message) {
        if (shutdown) return;
        String sender = message.getSenderId();
        String receiver = message.getReceiverId();
        if (isolatedNodes.contains(sender) || isolatedNodes.contains(receiver)) {
            return; // 패킷 드롭
        }
        ExecutorService dispatcher = dispatchers.get(receiver);
        if (dispatcher == null || receivers.get(receiver) == null) {
            return;
        }
        try {
            dispatcher.execute(() -> {
                Consumer<Message> handler = receivers.get(receiver);
                if (handler != null && !isolatedNodes.contains(receiver) && !isolatedNodes.contains(sender)) {
                    try {
                        handler.accept(message);
                    } catch (RuntimeException e) {
                        log.warn("Failed to deliver {} : {}", message, e.toString());
                    }
                }
            });
        } catch (RuntimeException ignored) {
            // dispatcher 종료
        }
    }

    public void isolateNode(String nodeId) {
        isolatedNodes.add(nodeId);
        log.warn("VirtualNetworkRouter: Node [{}] isolated from network (Simulating Partition).", nodeId);
    }

    public void reconnectNode(String nodeId) {
        isolatedNodes.remove(nodeId);
        log.info("VirtualNetworkRouter: Node [{}] reconnected to network.", nodeId);
    }

    public void unblockAll() {
        isolatedNodes.clear();
    }

    public void shutdown() {
        shutdown = true;
        dispatchers.values().forEach(ExecutorService::shutdownNow);
    }
}
