package com.batchservice.cluster.bootstrap;

import com.batchservice.cluster.consensus.RaftNode;
import com.batchservice.cluster.consensus.RaftTimings;
import com.batchservice.cluster.consensus.transport.SocketNetworkTransport;
import com.batchservice.cluster.fsm.ClusterStateMachine;
import com.batchservice.cluster.storage.RaftLogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * TC-QAS-08: 외부 미들웨어 없이 실제 TCP 소켓 전송 + 로컬 WAL만으로 N개 노드를 부트스트랩하고
 * 리더 선출·Readiness 도달까지를 검증하는 테스트용 엔진.
 */
public class StandaloneBootstrapEngine {
    private static final Logger log = LoggerFactory.getLogger(StandaloneBootstrapEngine.class);
    private static final String AUTH_TOKEN = "qas08-cluster-token";

    private final int nodeCount;
    private final Path walBaseDir;
    private final long childProcessesBefore = ProcessHandle.current().children().count();
    private final List<SocketNetworkTransport> transports = new ArrayList<>();
    private final List<RaftLogManager> logs = new ArrayList<>();
    private final List<RaftNode> nodes = new ArrayList<>();

    public StandaloneBootstrapEngine(int nodeCount, Path walBaseDir) {
        this.nodeCount = nodeCount;
        this.walBaseDir = walBaseDir;
    }

    public void bootstrap() throws IOException {
        List<String> ids = new ArrayList<>();
        List<Integer> ports = new ArrayList<>();
        for (int i = 1; i <= nodeCount; i++) {
            ids.add("node-" + i);
            ports.add(freePort());
        }
        log.info("Bootstrapping {} nodes over TCP ports {} (external middleware: NONE)", nodeCount, ports);
        for (int i = 0; i < nodeCount; i++) {
            Map<String, String> peers = new LinkedHashMap<>();
            for (int j = 0; j < nodeCount; j++) {
                if (j != i) peers.put(ids.get(j), "127.0.0.1:" + ports.get(j));
            }
            SocketNetworkTransport transport = new SocketNetworkTransport(ids.get(i), ports.get(i), peers, AUTH_TOKEN);
            transport.start();
            RaftLogManager raftLog = new RaftLogManager(walBaseDir.resolve(ids.get(i)), true);
            RaftNode node = new RaftNode(ids.get(i), ids, transport, raftLog, new ClusterStateMachine(), RaftTimings.defaults());
            node.start();
            transports.add(transport);
            logs.add(raftLog);
            nodes.add(node);
        }
    }

    /** 단일 리더가 서비스 가능하고 모든 노드가 Readiness에 도달했는지. */
    public boolean isClusterReady() {
        long readyLeaders = nodes.stream().filter(RaftNode::isLeaderReady).count();
        return readyLeaders == 1 && nodes.stream().allMatch(RaftNode::isReady);
    }

    public RaftNode getLeader() {
        return nodes.stream().filter(RaftNode::isLeaderReady).findFirst().orElse(null);
    }

    public List<RaftNode> getNodes() {
        return nodes;
    }

    /** 부트스트랩 과정에서 이 JVM이 기동한 외부(자식) 프로세스 수. */
    public long getRequiredExternalProcessCount() {
        return ProcessHandle.current().children().count() - childProcessesBefore;
    }

    public void stop() {
        nodes.forEach(RaftNode::stop);
        transports.forEach(SocketNetworkTransport::stop);
        logs.forEach(RaftLogManager::close);
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }
}
