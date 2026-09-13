package com.batchservice.cluster;

import com.batchservice.cluster.api.DistributedLockExecutor;
import com.batchservice.cluster.api.DistributedQueueService;
import com.batchservice.cluster.api.TaskJournalService;
import com.batchservice.cluster.consensus.RaftNode;
import com.batchservice.cluster.consensus.transport.NetworkTransport;
import com.batchservice.cluster.consensus.transport.SocketNetworkTransport;
import com.batchservice.cluster.fsm.ClusterStateMachine;
import com.batchservice.cluster.health.RaftHealthIndicator;
import com.batchservice.cluster.recovery.ExecutionStatusProvider;
import com.batchservice.cluster.recovery.RecoveryCoordinator;
import com.batchservice.cluster.storage.DatabaseStorage;
import com.batchservice.cluster.storage.JdbcDatabaseStorage;
import com.batchservice.cluster.storage.RaftLogManager;
import com.batchservice.cluster.storage.WriteBehindSynchronizer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.nio.file.Path;

/**
 * Embedded Library 자동 구성 (DD-06).
 *
 * <p>기동 순서: 설정 검증 → WAL 복구 → TCP 전송 → RaftNode → API / RecoveryCoordinator / Write-Behind.
 * 종료 시 역순으로 정지합니다. DataSource가 없으면 Write-Behind는 비활성화되고 합의·락·큐는 정상 동작합니다.
 */
@AutoConfiguration(afterName = "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration")
@EnableConfigurationProperties(BatchClusterProperties.class)
@ConditionalOnProperty(prefix = "batch.cluster", name = "enabled", havingValue = "true", matchIfMissing = true)
public class BatchClusteringAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public ClusterStateMachine clusterStateMachine() {
        return new ClusterStateMachine();
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public RaftLogManager raftLogManager(BatchClusterProperties properties) {
        properties.validate();
        Path dir = Path.of(properties.resolveWalDir()).resolve(properties.resolveNodeId());
        return new RaftLogManager(dir, properties.isWalFsync());
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    @ConditionalOnMissingBean(NetworkTransport.class)
    public SocketNetworkTransport raftNetworkTransport(BatchClusterProperties properties) {
        properties.validate();
        return new SocketNetworkTransport(properties.resolveNodeId(), properties.resolvePort(),
                properties.resolvePeerHostMap(), properties.resolveAuthToken());
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    @ConditionalOnMissingBean
    public RaftNode raftNode(BatchClusterProperties properties, NetworkTransport transport,
                             RaftLogManager raftLogManager, ClusterStateMachine clusterStateMachine) {
        return new RaftNode(properties.resolveNodeId(), properties.resolveAllNodeIds(), transport,
                raftLogManager, clusterStateMachine, properties.toTimings());
    }

    @Bean
    @ConditionalOnMissingBean
    public DistributedLockExecutor distributedLockExecutor(RaftNode raftNode) {
        return new DistributedLockExecutor(raftNode);
    }

    @Bean
    @ConditionalOnMissingBean
    public DistributedQueueService distributedQueueService(RaftNode raftNode, BatchClusterProperties properties) {
        return new DistributedQueueService(raftNode, properties.getQueueVisibilityTimeoutMs());
    }

    @Bean
    @ConditionalOnMissingBean
    public TaskJournalService taskJournalService(RaftNode raftNode) {
        return new TaskJournalService(raftNode);
    }

    @Bean
    @ConditionalOnMissingBean
    public ExecutionStatusProvider executionStatusProvider() {
        return ExecutionStatusProvider.unknown();
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    @ConditionalOnMissingBean
    public RecoveryCoordinator recoveryCoordinator(RaftNode raftNode, ExecutionStatusProvider executionStatusProvider,
                                                   BatchClusterProperties properties) {
        return new RecoveryCoordinator(raftNode, executionStatusProvider,
                properties.getRecoveryScanIntervalMs(), properties.getQueueVisibilityTimeoutMs());
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.springframework.jdbc.core.JdbcTemplate")
    @ConditionalOnBean(DataSource.class)
    @ConditionalOnProperty(prefix = "batch.cluster.sync", name = "enabled", havingValue = "true", matchIfMissing = true)
    static class WriteBehindConfiguration {

        @Bean
        @ConditionalOnMissingBean
        public DatabaseStorage databaseStorage(DataSource dataSource, BatchClusterProperties properties) {
            JdbcDatabaseStorage storage = new JdbcDatabaseStorage(dataSource);
            if (properties.getSync().isInitializeSchema()) {
                storage.initializeSchema();
            }
            return storage;
        }

        @Bean(initMethod = "start", destroyMethod = "stop")
        @ConditionalOnMissingBean
        public WriteBehindSynchronizer writeBehindSynchronizer(RaftNode raftNode, DatabaseStorage databaseStorage,
                                                               BatchClusterProperties properties) {
            return new WriteBehindSynchronizer(raftNode, databaseStorage, properties.getClusterId(),
                    properties.getSync().getBatchSize(), properties.getSync().getIntervalMs());
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.springframework.boot.actuate.health.HealthIndicator")
    static class HealthConfiguration {

        /**
         * Readiness Probe 연계: {@code management.endpoint.health.group.readiness.include=readinessState,raft}.
         * DB Health는 readiness 그룹에 포함하지 않습니다 (5.3 Scenario 4).
         */
        @Bean
        @ConditionalOnMissingBean(name = "raftHealthIndicator")
        public RaftHealthIndicator raftHealthIndicator(RaftNode raftNode) {
            return new RaftHealthIndicator(raftNode);
        }
    }
}
