package edu.minisql.distributed.datanode;

import edu.minisql.distributed.common.Jsons;
import edu.minisql.distributed.common.SqlUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class WalLog {
    private final Path walFile;
    private final Path indexFile;
    private final Set<String> requestIds = new LinkedHashSet<>();
    private long lastSequence;

    public WalLog(Path dataDir) {
        try {
            Files.createDirectories(dataDir);
            this.walFile = dataDir.resolve("wal.jsonl");
            this.indexFile = dataDir.resolve("wal-index.json");
            if (!Files.exists(walFile)) {
                Files.createFile(walFile);
            }
            loadIndex();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to initialize WAL", e);
        }
    }

    public synchronized WalEntry append(String requestId, int shardId, long shardLogIndex, String sql) {
        Optional<WalEntry> existing = findByRequestId(requestId);
        if (existing.isPresent()) {
            return existing.get();
        }
        return appendRaw(new WalEntry(lastSequence + 1, requestId, shardId, shardLogIndex,
                SqlUtils.normalize(sql), System.currentTimeMillis()));
    }

    public synchronized WalEntry appendRecovered(WalEntry peerEntry) {
        Optional<WalEntry> existing = findByRequestId(peerEntry.requestId);
        if (existing.isPresent()) {
            return existing.get();
        }
        WalEntry localEntry = new WalEntry(
                Math.max(lastSequence + 1, peerEntry.sequence),
                peerEntry.requestId,
                peerEntry.shardId,
                peerEntry.shardLogIndex,
                SqlUtils.normalize(peerEntry.sql),
                peerEntry.timestampMillis);
        return appendRaw(localEntry);
    }

    public synchronized void advanceLastSequence(long sequence) {
        if (sequence > lastSequence) {
            lastSequence = sequence;
            persistIndex();
        }
    }

    public synchronized boolean containsRequestId(String requestId) {
        return requestIds.contains(requestId) || findByRequestId(requestId).isPresent();
    }

    private WalEntry appendRaw(WalEntry entry) {
        try (BufferedWriter writer = Files.newBufferedWriter(
                walFile,
                StandardCharsets.UTF_8,
                StandardOpenOption.APPEND)) {
            writer.write(Jsons.stringify(entry));
            writer.newLine();
            writer.flush();
            lastSequence = entry.sequence;
            requestIds.add(entry.requestId);
            persistIndex();
            return entry;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to append WAL", e);
        }
    }

    private Optional<WalEntry> findByRequestId(String requestId) {
        if (requestId == null || requestId.isBlank()) {
            return Optional.empty();
        }
        return readAll().stream()
                .filter(entry -> requestId.equals(entry.requestId))
                .findFirst();
    }

    public synchronized List<WalEntry> pendingAfter(long sequence) {
        return readAll().stream()
                .filter(entry -> entry.sequence > sequence)
                .collect(Collectors.toList());
    }

    public synchronized Map<Integer, Long> maxShardLogIndexes() {
        Map<Integer, Long> indexes = new TreeMap<>();
        for (WalEntry entry : readAll()) {
            indexes.merge(entry.shardId, entry.shardLogIndex, Math::max);
        }
        return indexes;
    }

    public synchronized void compactThrough(long sequence) {
        List<WalEntry> retained = readAll().stream()
                .filter(entry -> entry.sequence > sequence)
                .sorted(Comparator.comparingLong(entry -> entry.sequence))
                .collect(Collectors.toList());
        try (BufferedWriter writer = Files.newBufferedWriter(
                walFile,
                StandardCharsets.UTF_8,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            for (WalEntry entry : retained) {
                writer.write(Jsons.stringify(entry));
                writer.newLine();
            }
            writer.flush();
            persistIndex();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to compact WAL", e);
        }
    }

    public synchronized List<WalEntry> readAll() {
        try {
            List<WalEntry> entries = new ArrayList<>();
            for (String line : Files.readAllLines(walFile, StandardCharsets.UTF_8)) {
                if (!line.isBlank()) {
                    entries.add(Jsons.parse(line, WalEntry.class));
                }
            }
            entries.sort(java.util.Comparator.comparingLong(entry -> entry.sequence));
            return entries;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read WAL", e);
        }
    }

    public synchronized long lastSequence() {
        return lastSequence;
    }

    private void loadIndex() throws IOException {
        if (Files.exists(indexFile)) {
            WalIndex index = Jsons.parse(Files.readString(indexFile, StandardCharsets.UTF_8), WalIndex.class);
            this.lastSequence = index.lastSequence;
            this.requestIds.addAll(index.requestIds);
        }
        List<WalEntry> entries = readAll();
        this.lastSequence = Math.max(lastSequence, entries.stream().mapToLong(entry -> entry.sequence).max().orElse(0L));
        entries.stream()
                .map(entry -> entry.requestId)
                .filter(requestId -> requestId != null && !requestId.isBlank())
                .forEach(requestIds::add);
        persistIndex();
    }

    private void persistIndex() {
        try {
            WalIndex index = new WalIndex();
            index.lastSequence = lastSequence;
            index.requestIds = new ArrayList<>(requestIds);
            Files.writeString(indexFile, Jsons.stringify(index), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to persist WAL index", e);
        }
    }

    public static class WalIndex {
        public long lastSequence;
        public List<String> requestIds = new ArrayList<>();
    }
}
