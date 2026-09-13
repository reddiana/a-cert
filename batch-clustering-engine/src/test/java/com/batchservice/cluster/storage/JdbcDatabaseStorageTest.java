package com.batchservice.cluster.storage;

import com.batchservice.cluster.fsm.Command;
import com.batchservice.cluster.fsm.LogEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcDatabaseStorageTest {

    private JdbcDatabaseStorage storage;

    @BeforeEach
    void setUp() {
        DriverManagerDataSource ds = new DriverManagerDataSource(
                "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1", "sa", "");
        ds.setDriverClassName("org.h2.Driver");
        storage = new JdbcDatabaseStorage(ds);
        storage.initializeSchema();
    }

    @Test
    @DisplayName("Write-Behind: 이력 반영과 체크포인트 갱신이 원자적이며 log_index 기준으로 멱등하다")
    void writeBatchIsAtomicAndIdempotent() throws Exception {
        assertThat(storage.readCheckpoint("c1")).isZero();

        storage.writeBatch("c1", entries(1, 3), 3);
        assertThat(storage.countRecords()).isEqualTo(3);
        assertThat(storage.readCheckpoint("c1")).isEqualTo(3);

        // 리더 교체 등으로 구간이 겹쳐 재반영되어도 중복이 생기지 않음
        storage.writeBatch("c1", entries(2, 5), 5);
        assertThat(storage.countRecords()).isEqualTo(5);
        assertThat(storage.readCheckpoint("c1")).isEqualTo(5);

        // 구 리더의 뒤늦은 트랜잭션이 체크포인트를 후퇴시키지 않음
        storage.writeBatch("c1", entries(1, 2), 2);
        assertThat(storage.readCheckpoint("c1")).isEqualTo(5);
    }

    private static List<LogEntry> entries(long from, long to) {
        List<LogEntry> list = new ArrayList<>();
        for (long i = from; i <= to; i++) {
            list.add(new LogEntry(i, 1, System.currentTimeMillis(), Command.enqueue("JOB-" + i, false)));
        }
        return list;
    }
}
