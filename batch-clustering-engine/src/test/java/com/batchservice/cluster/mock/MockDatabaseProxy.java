package com.batchservice.cluster.mock;

import com.batchservice.cluster.fsm.LogEntry;
import com.batchservice.cluster.storage.DatabaseStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 메타 DB 장애 주입용 인메모리 저장소 (7.1.3 MockDatabaseConnectionProxy).
 * 이력 반영과 체크포인트 갱신을 원자적으로 처리하고, log_index 기준으로 멱등합니다.
 */
public class MockDatabaseProxy implements DatabaseStorage {
    private static final Logger log = LoggerFactory.getLogger(MockDatabaseProxy.class);

    private volatile boolean connected = true;
    private final TreeMap<Long, LogEntry> records = new TreeMap<>();
    private final Map<String, Long> checkpoints = new HashMap<>();

    public void disconnect() {
        connected = false;
        log.warn("MockDatabaseProxy: Connection severed (Simulating DB Outage).");
    }

    public void reconnect() {
        connected = true;
        log.info("MockDatabaseProxy: Connection restored (DB is now ONLINE).");
    }

    public boolean isConnected() {
        return connected;
    }

    @Override
    public void initializeSchema() {
    }

    @Override
    public synchronized long readCheckpoint(String clusterId) throws SQLException {
        ensureConnected();
        return checkpoints.getOrDefault(clusterId, 0L);
    }

    @Override
    public synchronized void writeBatch(String clusterId, List<LogEntry> entries, long newCheckpoint) throws SQLException {
        ensureConnected();
        for (LogEntry e : entries) {
            records.put(e.getIndex(), e);
        }
        checkpoints.merge(clusterId, newCheckpoint, Math::max);
    }

    @Override
    public synchronized long countRecords() throws SQLException {
        ensureConnected();
        return records.size();
    }

    public synchronized long lastRecordedIndex() {
        return records.isEmpty() ? 0L : records.lastKey();
    }

    private void ensureConnected() throws SQLException {
        if (!connected) {
            throw new SQLException("Database connection unavailable (Connection refused)");
        }
    }
}
