package com.batchservice.cluster.storage;

import com.batchservice.cluster.fsm.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * 로컬 디스크 WAL 관리자 (DD-05 Embedded Local Disk WAL, 5.4 RaftLogManager).
 *
 * <ul>
 *   <li>로그 파일: {@code [length][crc32][entry bytes]} 프레임의 순차 append. 기동 시 CRC가 깨진 꼬리 프레임은 절단</li>
 *   <li>HardState 파일: {@code currentTerm}, {@code votedFor}, {@code commitIndex}를 임시 파일 + fsync + 원자적 교체로 저장</li>
 *   <li>{@link #sync()}는 append와 분리되어 다건 엔트리를 1회 fsync로 영속화 (Group Commit)</li>
 * </ul>
 *
 * <p>{@code dir}이 {@code null}이면 파일 없이 메모리로만 동작합니다 (단위 테스트 전용).
 */
public class RaftLogManager implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(RaftLogManager.class);

    private final boolean persistent;
    private final boolean fsync;
    private final Path logPath;
    private final Path hardStatePath;

    private final ArrayList<LogEntry> entries = new ArrayList<>();
    private final ArrayList<Long> offsets = new ArrayList<>();
    private FileChannel channel;
    private long fileSize;

    private long currentTerm;
    private String votedFor;
    private long persistedCommitIndex;
    private volatile long compactionFloor;

    public RaftLogManager(Path dir, boolean fsync) {
        this.persistent = dir != null;
        this.fsync = fsync;
        this.logPath = persistent ? dir.resolve("raft.log") : null;
        this.hardStatePath = persistent ? dir.resolve("hardstate") : null;
        if (persistent) {
            try {
                Files.createDirectories(dir);
                loadHardState();
                recoverLog();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to open WAL at " + dir, e);
            }
        }
    }

    // ─── 로그 조회 ────────────────────────────────────────────────────────────
    public synchronized long lastIndex() {
        return entries.size();
    }

    public synchronized long lastTerm() {
        return entries.isEmpty() ? 0L : entries.get(entries.size() - 1).getTerm();
    }

    /** index의 term. index=0은 0, 로그 범위를 벗어나면 -1. */
    public synchronized long termAt(long index) {
        if (index <= 0) return 0L;
        if (index > entries.size()) return -1L;
        return entries.get((int) index - 1).getTerm();
    }

    public synchronized LogEntry get(long index) {
        return index >= 1 && index <= entries.size() ? entries.get((int) index - 1) : null;
    }

    public synchronized List<LogEntry> slice(long fromIndex, long toIndexInclusive, int maxCount) {
        List<LogEntry> result = new ArrayList<>();
        long to = Math.min(toIndexInclusive, entries.size());
        for (long i = Math.max(1, fromIndex); i <= to && result.size() < maxCount; i++) {
            result.add(entries.get((int) i - 1));
        }
        return result;
    }

    // ─── 로그 변경 ────────────────────────────────────────────────────────────
    public synchronized void append(List<LogEntry> newEntries) {
        if (newEntries.isEmpty()) return;
        ByteArrayOutputStream frames = new ByteArrayOutputStream();
        long offset = fileSize;
        List<Long> newOffsets = new ArrayList<>();
        try {
            for (LogEntry e : newEntries) {
                if (e.getIndex() != entries.size() + newOffsets.size() + 1) {
                    throw new IllegalStateException("Non-contiguous append: expected index "
                            + (entries.size() + newOffsets.size() + 1) + " but was " + e.getIndex());
                }
                byte[] body = encode(e);
                CRC32 crc = new CRC32();
                crc.update(body);
                DataOutputStream out = new DataOutputStream(frames);
                out.writeInt(body.length);
                out.writeInt((int) crc.getValue());
                out.write(body);
                newOffsets.add(offset);
                offset += 8 + body.length;
            }
            if (persistent) {
                ByteBuffer buf = ByteBuffer.wrap(frames.toByteArray());
                while (buf.hasRemaining()) {
                    channel.write(buf, fileSize + buf.position());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("WAL append failed", e);
        }
        entries.addAll(newEntries);
        offsets.addAll(newOffsets);
        fileSize = offset;
    }

    /** index 이후(포함)의 엔트리를 절단합니다 (팔로워의 충돌 엔트리 제거). */
    public synchronized void truncateFrom(long index) {
        if (index < 1 || index > entries.size()) return;
        long offset = offsets.get((int) index - 1);
        entries.subList((int) index - 1, entries.size()).clear();
        offsets.subList((int) index - 1, offsets.size()).clear();
        fileSize = offset;
        if (persistent) {
            try {
                channel.truncate(offset);
            } catch (IOException e) {
                throw new UncheckedIOException("WAL truncate failed", e);
            }
        }
    }

    /**
     * 현재까지 append된 엔트리를 fsync하고 영속화가 보장된 마지막 인덱스를 반환합니다.
     * 호출 전 append된 모든 바이트는 OS 버퍼에 기록되어 있으므로 force 1회로 함께 영속화됩니다.
     */
    public long sync() {
        long durable;
        synchronized (this) {
            durable = entries.size();
        }
        if (persistent && fsync) {
            try {
                channel.force(false);
            } catch (IOException e) {
                throw new UncheckedIOException("WAL fsync failed", e);
            }
        }
        return durable;
    }

    // ─── HardState ────────────────────────────────────────────────────────────
    public synchronized void saveHardState(long term, String votedFor) {
        this.currentTerm = term;
        this.votedFor = votedFor;
        writeHardState();
    }

    public synchronized void saveCommitIndex(long commitIndex) {
        if (commitIndex <= persistedCommitIndex) return;
        this.persistedCommitIndex = commitIndex;
        writeHardState();
    }

    public synchronized long getCurrentTerm() { return currentTerm; }
    public synchronized String getVotedFor() { return votedFor; }
    public synchronized long getPersistedCommitIndex() { return persistedCommitIndex; }

    /** DB 반영이 완료되어 압축이 허용되는 하한선 (5.3 불변식 8). 스냅샷/압축 구현 시 사용. */
    public void setCompactionFloor(long index) { this.compactionFloor = Math.max(compactionFloor, index); }
    public long getCompactionFloor() { return compactionFloor; }

    @Override
    public synchronized void close() {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {
            }
            channel = null;
        }
    }

    // ─── 내부 구현 ────────────────────────────────────────────────────────────
    private void writeHardState() {
        if (!persistent) return;
        String content = currentTerm + "\n" + (votedFor == null ? "" : votedFor) + "\n" + persistedCommitIndex + "\n";
        Path tmp = hardStatePath.resolveSibling("hardstate.tmp");
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            ch.write(ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8)));
            if (fsync) ch.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException("HardState write failed", e);
        }
        try {
            Files.move(tmp, hardStatePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException("HardState replace failed", e);
        }
    }

    private void loadHardState() throws IOException {
        if (!Files.exists(hardStatePath)) return;
        List<String> lines = Files.readAllLines(hardStatePath, StandardCharsets.UTF_8);
        currentTerm = lines.size() > 0 && !lines.get(0).isBlank() ? Long.parseLong(lines.get(0).trim()) : 0L;
        votedFor = lines.size() > 1 && !lines.get(1).isBlank() ? lines.get(1).trim() : null;
        persistedCommitIndex = lines.size() > 2 && !lines.get(2).isBlank() ? Long.parseLong(lines.get(2).trim()) : 0L;
    }

    private void recoverLog() throws IOException {
        channel = FileChannel.open(logPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        long size = channel.size();
        long pos = 0;
        try (InputStream raw = Channels.newInputStream(channel.position(0));
             DataInputStream in = new DataInputStream(new BufferedInputStream(raw, 1 << 16))) {
            while (pos + 8 <= size) {
                int length = in.readInt();
                int crcValue = in.readInt();
                if (length <= 0 || pos + 8 + length > size) break;
                byte[] body = new byte[length];
                in.readFully(body);
                CRC32 crc = new CRC32();
                crc.update(body);
                if ((int) crc.getValue() != crcValue) break;
                LogEntry e = LogEntry.readFrom(new DataInputStream(new ByteArrayInputStream(body)));
                if (e.getIndex() != entries.size() + 1) break;
                entries.add(e);
                offsets.add(pos);
                pos += 8 + length;
            }
        } catch (EOFException ignored) {
            // 부분 기록된 꼬리 프레임
        }
        // Channels.newInputStream 종료 시 채널이 닫히므로 재오픈
        channel = FileChannel.open(logPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        if (pos < size) {
            log.warn("WAL {}: truncating corrupted/partial tail at offset {} (file size {}).", logPath, pos, size);
            channel.truncate(pos);
        }
        fileSize = pos;
        if (persistedCommitIndex > entries.size()) {
            persistedCommitIndex = entries.size();
        }
        log.info("WAL {} recovered: lastIndex={}, term={}, commitIndex={}",
                logPath, entries.size(), currentTerm, persistedCommitIndex);
    }

    private static byte[] encode(LogEntry e) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(128);
        e.writeTo(new DataOutputStream(bytes));
        return bytes.toByteArray();
    }
}
