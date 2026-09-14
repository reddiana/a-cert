package com.sds.batchservice.cluster.sync;

import com.sds.batchservice.cluster.fsm.LogEntry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * JDBC 기반 메타 DB 저장소 (4.2.1: JDBC / HikariCP, 5.4 WriteBehindSynchronizer 대상).
 *
 * <ul>
 *   <li>{@code batch_journal_log}: log_index PK — 동일 구간 재반영 시 DELETE 후 INSERT로 멱등 처리</li>
 *   <li>{@code batch_sync_checkpoint}: 클러스터별 마지막 반영 인덱스 (단조 증가만 허용)</li>
 *   <li>이력 반영과 체크포인트 갱신은 단일 트랜잭션</li>
 * </ul>
 */
public class JdbcDatabaseStorage implements DatabaseStorage {
    private static final int MAX_PAYLOAD_LENGTH = 4000;

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;

    public JdbcDatabaseStorage(DataSource dataSource) {
        this.jdbc = new JdbcTemplate(dataSource);
        this.tx = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    }

    @Override
    public void initializeSchema() {
        jdbc.execute("CREATE TABLE IF NOT EXISTS batch_journal_log ("
                + " log_index BIGINT NOT NULL PRIMARY KEY,"
                + " term BIGINT NOT NULL,"
                + " entry_type VARCHAR(32) NOT NULL,"
                + " request_id VARCHAR(128),"
                + " proposed_at BIGINT NOT NULL,"
                + " payload VARCHAR(4000),"
                + " synced_at TIMESTAMP NOT NULL)");
        jdbc.execute("CREATE TABLE IF NOT EXISTS batch_sync_checkpoint ("
                + " cluster_id VARCHAR(64) NOT NULL PRIMARY KEY,"
                + " last_log_index BIGINT NOT NULL,"
                + " updated_at TIMESTAMP NOT NULL)");
    }

    @Override
    public long readCheckpoint(String clusterId) {
        List<Long> rows = jdbc.queryForList(
                "SELECT last_log_index FROM batch_sync_checkpoint WHERE cluster_id = ?", Long.class, clusterId);
        return rows.isEmpty() ? 0L : rows.get(0);
    }

    @Override
    public void writeBatch(String clusterId, List<LogEntry> entries, long newCheckpoint) {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        tx.executeWithoutResult(status -> {
            if (!entries.isEmpty()) {
                long first = entries.get(0).getIndex();
                long last = entries.get(entries.size() - 1).getIndex();
                jdbc.update("DELETE FROM batch_journal_log WHERE log_index BETWEEN ? AND ?", first, last);
                jdbc.batchUpdate("INSERT INTO batch_journal_log"
                                + " (log_index, term, entry_type, request_id, proposed_at, payload, synced_at)"
                                + " VALUES (?, ?, ?, ?, ?, ?, ?)",
                        entries, entries.size(), (ps, e) -> {
                            ps.setLong(1, e.getIndex());
                            ps.setLong(2, e.getTerm());
                            ps.setString(3, e.getType().name());
                            ps.setString(4, e.getCommand().getRequestId());
                            ps.setLong(5, e.getProposedAt());
                            ps.setString(6, encodePayload(e.getCommand().getAttrs()));
                            ps.setTimestamp(7, now);
                        });
            }
            int updated = jdbc.update("UPDATE batch_sync_checkpoint SET last_log_index = ?, updated_at = ?"
                    + " WHERE cluster_id = ? AND last_log_index < ?", newCheckpoint, now, clusterId, newCheckpoint);
            if (updated == 0) {
                Integer exists = jdbc.queryForObject(
                        "SELECT COUNT(*) FROM batch_sync_checkpoint WHERE cluster_id = ?", Integer.class, clusterId);
                if (exists == null || exists == 0) {
                    jdbc.update("INSERT INTO batch_sync_checkpoint (cluster_id, last_log_index, updated_at) VALUES (?, ?, ?)",
                            clusterId, newCheckpoint, now);
                }
            }
        });
    }

    @Override
    public long countRecords() {
        Long count = jdbc.queryForObject("SELECT COUNT(*) FROM batch_journal_log", Long.class);
        return count == null ? 0L : count;
    }

    private static String encodePayload(Map<String, String> attrs) {
        String payload = attrs.entrySet().stream()
                .map(e -> e.getKey() + "=" + URLEncoder.encode(String.valueOf(e.getValue()), StandardCharsets.UTF_8))
                .collect(Collectors.joining("&"));
        return payload.length() > MAX_PAYLOAD_LENGTH ? payload.substring(0, MAX_PAYLOAD_LENGTH) : payload;
    }
}
