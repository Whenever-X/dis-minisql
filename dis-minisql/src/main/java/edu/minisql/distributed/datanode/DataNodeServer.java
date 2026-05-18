package edu.minisql.distributed.datanode;

import com.sun.net.httpserver.HttpServer;
import edu.minisql.distributed.common.HttpUtil;
import edu.minisql.distributed.common.Jsons;
import edu.minisql.distributed.common.SqlUtils;
import edu.minisql.distributed.config.ClusterConfig;
import edu.minisql.distributed.config.NodeConfig;
import edu.minisql.distributed.minisql.MiniSqlEngine;
import edu.minisql.distributed.protocol.ExecuteRequest;
import edu.minisql.distributed.protocol.ExecuteResponse;
import edu.minisql.distributed.protocol.NodeInfo;
import edu.minisql.distributed.zk.ZkMetadataStore;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class DataNodeServer {
    private final ClusterConfig clusterConfig;
    private final NodeConfig nodeConfig;
    private final ZkMetadataStore metadataStore;
    private final WalLog walLog;
    private final SnapshotManager snapshotManager;
    private final MiniSqlEngine miniSqlEngine;
    private final Path dataDir;
    private final Path miniSqlRuntimeDir;
    private final Set<Integer> shards = new HashSet<>();
    private volatile String replicaState = "RECOVERING";
    private SnapshotManifest snapshotManifest;

    public DataNodeServer(ClusterConfig clusterConfig, String nodeId) {
        this.clusterConfig = clusterConfig;
        this.nodeConfig = clusterConfig.requireNode(nodeId);
        this.metadataStore = new ZkMetadataStore(clusterConfig);
        this.metadataStore.initializeShards(clusterConfig);
        this.shards.addAll(metadataStore.shardsForNode(nodeId));
        this.dataDir = Path.of(nodeConfig.dataDir == null ? "data/" + nodeId : nodeConfig.dataDir);
        this.miniSqlRuntimeDir = dataDir.resolve("minisql-runtime");
        this.walLog = new WalLog(dataDir);
        this.snapshotManager = new SnapshotManager(dataDir);
        Path snapshotDatabases = dataDir.resolve("snapshots").resolve("current").resolve("databases");
        this.miniSqlEngine = new MiniSqlEngine(Path.of(clusterConfig.minisqlBinary), miniSqlRuntimeDir,
                snapshotDatabases, Duration.ofSeconds(30), clusterConfig.defaultDatabase);
        log("constructed cluster=%s zk=%s listen=%s:%d dataDir=%s minisql=%s initialShards=%s",
                clusterConfig.clusterName, clusterConfig.zkConnect, nodeConfig.host, nodeConfig.port,
                dataDir, clusterConfig.minisqlBinary, shards);
    }

    public void start() throws IOException {
        log("lifecycle STARTING");
        log("loading snapshot manifest");
        snapshotManifest = snapshotManager.loadManifest();
        log("snapshot manifest loaded lastWal=%d compactedWal=%d shardLogIndexes=%s",
                snapshotManifest.lastWalSequence, snapshotManifest.lastCompactedWalSequence,
                snapshotManifest.shardLogIndexes);
        log("restoring snapshot databases if needed");
        snapshotManager.restoreIfNeeded();
        log("registering RECOVERING node in ZooKeeper");
        register();
        log("starting peer WAL recovery");
        recoverFromPeers();
        log("applying pending WAL after snapshot compactedWal=%d", snapshotManifest.lastCompactedWalSequence);
        applyPendingWal();
        log("checking startup snapshot threshold");
        maybeSnapshot();
        log("starting persistent MiniSQL engine");
        syncEngineFull();
        replicaState = "SERVING";
        log("recovery complete, switching state to SERVING");
        register();
        HttpServer server = HttpServer.create(new InetSocketAddress(nodeConfig.host, nodeConfig.port), 0);
        server.createContext("/execute", exchange -> {
            try {
                ExecuteRequest request = Jsons.parse(HttpUtil.body(exchange), ExecuteRequest.class);
                ExecuteResponse response = execute(request);
                HttpUtil.json(exchange, response.ok ? 200 : 500, response);
            } catch (Exception e) {
                error("execute handler failed: %s", e.getMessage());
                HttpUtil.json(exchange, 500, ExecuteResponse.error(nodeConfig.nodeId, e.getMessage()));
            }
        });
        server.createContext("/wal", exchange -> {
            List<WalEntry> entries = walLog.readAll();
            log("serving WAL request entries=%d lastSequence=%d", entries.size(), walLog.lastSequence());
            HttpUtil.json(exchange, 200, entries);
        });
        server.createContext("/recovery-state", exchange -> {
            RecoveryState state = recoveryState();
            log("serving recovery-state request snapshotSql=%d walEntries=%d compactedWal=%d",
                    state.snapshotSql.size(), state.walEntries.size(),
                    state.snapshotManifest == null ? 0 : state.snapshotManifest.lastCompactedWalSequence);
            HttpUtil.json(exchange, 200, state);
        });
        server.createContext("/health", exchange -> HttpUtil.json(exchange, 200, currentNodeInfo()));
        server.createContext("/admin/refresh-shards", exchange -> {
            log("admin refresh-shards received");
            replicaState = "RECOVERING";
            register();
            refreshShards();
            recoverFromPeers();
            applyPendingWal();
            syncEngineFull();
            replicaState = "SERVING";
            register();
            log("admin refresh-shards completed state=%s shards=%s", replicaState, shards);
            HttpUtil.json(exchange, 200, currentNodeInfo());
        });
        server.createContext("/admin/snapshot", exchange -> {
            log("admin snapshot received");
            List<String> appliedSql = new ArrayList<>(snapshotManager.snapshotSql());
            walLog.readAll().stream()
                    .map(entry -> entry.sql)
                    .forEach(appliedSql::add);
            snapshotManifest = snapshotManager.createSnapshot(walLog.lastSequence(), currentShardLogIndexes(), appliedSql);
            walLog.compactThrough(walLog.lastSequence());
            miniSqlEngine.invalidate();
            log("admin snapshot completed compactedWal=%d appliedSql=%d shardLogIndexes=%s",
                    snapshotManifest.lastCompactedWalSequence, appliedSql.size(), snapshotManifest.shardLogIndexes);
            HttpUtil.json(exchange, 200, snapshotManifest);
        });
        server.setExecutor(Executors.newFixedThreadPool(8));
        server.start();
        log("lifecycle SERVING cluster=%s shards=%s address=%s:%d",
                clusterConfig.clusterName, shards, nodeConfig.host, nodeConfig.port);
        System.out.printf("DataNode %s serving %s shards %s at %s:%d%n",
                nodeConfig.nodeId, clusterConfig.clusterName, shards, nodeConfig.host, nodeConfig.port);
    }

    public synchronized String executeLocalSql(String sql) {
        return runOnEngine(Long.MAX_VALUE, sql, walLog.lastSequence());
    }

    private synchronized ExecuteResponse execute(ExecuteRequest request) {
        String sql = SqlUtils.normalize(request.sql);
        log("execute request requestId=%s shard=%d shardLogIndex=%d raftTerm=%d replay=%s readOnly=%s sql=%s",
                request.requestId, request.shardId, request.shardLogIndex, request.raftTerm, request.replay,
                SqlUtils.isReadOnly(sql), oneLine(sql));
        WalEntry written = null;
        if (!SqlUtils.isReadOnly(sql) && !request.replay) {
            if (!ownsShard(request.shardId)) {
                warn("execute rejected requestId=%s reason=node does not own shard %d ownedShards=%s",
                        request.requestId, request.shardId, shards);
                return ExecuteResponse.error(nodeConfig.nodeId, "node does not own shard " + request.shardId);
            }
            if (request.shardId >= 0 && request.raftTerm < metadataStore.shard(request.shardId).term) {
                warn("execute rejected requestId=%s reason=stale raft term requestTerm=%d currentTerm=%d shard=%d",
                        request.requestId, request.raftTerm, metadataStore.shard(request.shardId).term, request.shardId);
                return ExecuteResponse.error(nodeConfig.nodeId, "stale raft term " + request.raftTerm);
            }
            written = walLog.append(request.requestId, request.shardId, request.shardLogIndex, sql);
            log("wal appended requestId=%s localSequence=%d shard=%d shardLogIndex=%d sql=%s",
                    request.requestId, written.sequence, written.shardId, written.shardLogIndex, oneLine(written.sql));
            metadataStore.updateNode(currentNodeInfo());
        } else if (request.replay && walLog.containsRequestId(request.requestId)) {
            log("replay skipped duplicate requestId=%s lastSequence=%d", request.requestId, walLog.lastSequence());
            return ExecuteResponse.ok(nodeConfig.nodeId, walLog.lastSequence(), "duplicate replay ignored");
        } else if (request.replay) {
            written = walLog.appendRecovered(new WalEntry(0, request.requestId, request.shardId,
                    request.shardLogIndex, sql, System.currentTimeMillis()));
            log("replay wal appended requestId=%s localSequence=%d shard=%d shardLogIndex=%d sql=%s",
                    request.requestId, written.sequence, written.shardId, written.shardLogIndex, oneLine(written.sql));
        }

        long sequence = written == null ? walLog.lastSequence() : written.sequence;
        long sequenceExclusive = written == null ? Long.MAX_VALUE : written.sequence;
        long appliedWalSequence = written == null ? walLog.lastSequence() : written.sequence;
        log("miniSQL start requestId=%s sequenceExclusive=%d currentSql=%s engineReady=%s",
                request.requestId, sequenceExclusive, oneLine(sql), miniSqlEngine.isReady());
        String output = runOnEngine(sequenceExclusive, sql, appliedWalSequence);
        log("miniSQL completed requestId=%s outputLines=%d engineWal=%d",
                request.requestId, lineCount(output), miniSqlEngine.engineWalSequence());
        logMiniSqlOutput(request.requestId, output);
        if (written != null) {
            snapshotManifest = snapshotManager.markApplied(snapshotManifest, sequence, currentShardLogIndexes());
            log("snapshot manifest marked applied requestId=%s lastWal=%d shardLogIndexes=%s",
                    request.requestId, snapshotManifest.lastWalSequence, snapshotManifest.shardLogIndexes);
        }
        maybeSnapshot();
        metadataStore.updateNode(currentNodeInfo());
        log("execute completed requestId=%s walSequence=%d shardLogIndex=%d raftTerm=%d",
                request.requestId, sequence, written == null ? request.shardLogIndex : written.shardLogIndex,
                request.raftTerm);
        ExecuteResponse response = ExecuteResponse.ok(nodeConfig.nodeId, sequence, output);
        response.shardLogIndex = written == null ? request.shardLogIndex : written.shardLogIndex;
        response.raftTerm = request.raftTerm;
        return response;
    }

    private void recoverFromPeers() {
        List<NodeInfo> peers = metadataStore.liveNodes();
        log("peer recovery scan liveNodes=%d ownedShards=%s", peers.size(), shards);
        int recoveredTotal = 0;
        for (NodeInfo peer : peers) {
            if (peer.nodeId.equals(nodeConfig.nodeId) || java.util.Collections.disjoint(peer.shards, shards)) {
                debug("peer recovery skip peer=%s reason=%s peerShards=%s",
                        peer.nodeId, peer.nodeId.equals(nodeConfig.nodeId) ? "self" : "no shared shards", peer.shards);
                continue;
            }
            try {
                log("peer recovery fetch peer=%s url=%s sharedShards=%s", peer.nodeId, peer.baseUrl(), peer.shards);
                RecoveryState peerState = HttpUtil.getJson(peer.baseUrl() + "/recovery-state", RecoveryState.class);
                installPeerSnapshotIfNewer(peer.nodeId, peerState);
                List<WalEntry> recovered = peerState.walEntries.stream()
                        .sorted(Comparator.comparingLong(entry -> entry.sequence))
                        .filter(entry -> ownsShard(entry.shardId))
                        .filter(entry -> !walLog.containsRequestId(entry.requestId))
                        .collect(Collectors.toList());
                for (WalEntry entry : recovered) {
                    WalEntry local = walLog.appendRecovered(entry);
                    log("peer recovery appended peer=%s requestId=%s peerSequence=%d localSequence=%d shard=%d shardLogIndex=%d sql=%s",
                            peer.nodeId, entry.requestId, entry.sequence, local.sequence, local.shardId,
                            local.shardLogIndex, oneLine(local.sql));
                }
                recoveredTotal += recovered.size();
                log("peer recovery done peer=%s peerWal=%d recovered=%d",
                        peer.nodeId, peerState.walEntries.size(), recovered.size());
            } catch (Exception e) {
                warn("peer recovery failed peer=%s error=%s", peer.nodeId, e.getMessage());
            }
        }
        log("peer recovery completed recoveredTotal=%d lastSequence=%d", recoveredTotal, walLog.lastSequence());
    }

    private void installPeerSnapshotIfNewer(String peerNodeId, RecoveryState peerState) {
        if (peerState == null || peerState.snapshotManifest == null) {
            debug("peer snapshot skip peer=%s reason=no snapshot manifest", peerNodeId);
            return;
        }
        if (peerState.snapshotManifest.lastCompactedWalSequence <= snapshotManifest.lastCompactedWalSequence) {
            debug("peer snapshot skip peer=%s peerCompactedWal=%d localCompactedWal=%d",
                    peerNodeId, peerState.snapshotManifest.lastCompactedWalSequence,
                    snapshotManifest.lastCompactedWalSequence);
            return;
        }
        log("peer snapshot install peer=%s peerCompactedWal=%d localCompactedWal=%d snapshotSql=%d",
                peerNodeId, peerState.snapshotManifest.lastCompactedWalSequence,
                snapshotManifest.lastCompactedWalSequence, peerState.snapshotSql.size());
        snapshotManifest = snapshotManager.createSnapshot(
                peerState.snapshotManifest.lastCompactedWalSequence,
                peerState.snapshotManifest.shardLogIndexes,
                peerState.snapshotSql);
        walLog.advanceLastSequence(peerState.snapshotManifest.lastCompactedWalSequence);
        miniSqlEngine.invalidate();
        log("peer snapshot installed peer=%s compactedWal=%d shardLogIndexes=%s",
                peerNodeId, snapshotManifest.lastCompactedWalSequence, snapshotManifest.shardLogIndexes);
    }

    private boolean ownsShard(int shardId) {
        return shardId < 0 || shards.contains(shardId);
    }

    private void applyPendingWal() {
        List<WalEntry> pending = walLog.pendingAfter(snapshotManifest.lastCompactedWalSequence);
        if (!pending.isEmpty()) {
            log("applying pending WAL pendingStatements=%d compactedWal=%d", pending.size(),
                    snapshotManifest.lastCompactedWalSequence);
            snapshotManifest = snapshotManager.markApplied(snapshotManifest, walLog.lastSequence(), currentShardLogIndexes());
            log("pending WAL applied lastWal=%d shardLogIndexes=%s",
                    snapshotManifest.lastWalSequence, snapshotManifest.shardLogIndexes);
        } else {
            log("no pending WAL to apply after compactedWal=%d", snapshotManifest.lastCompactedWalSequence);
        }
    }

    private void syncEngineFull() {
        long compactedWal = snapshotManifest == null ? 0 : snapshotManifest.lastCompactedWalSequence;
        List<String> replay = replaySqlBefore(Long.MAX_VALUE);
        long replayThrough = walLog.readAll().stream()
                .filter(entry -> entry.sequence > compactedWal)
                .mapToLong(entry -> entry.sequence)
                .max()
                .orElse(compactedWal);
        miniSqlEngine.resetAndReplay(replay, replayThrough, compactedWal);
        miniSqlEngine.setEngineWalSequence(walLog.lastSequence());
        log("persistent MiniSQL engine ready wal=%d compactedWal=%d replayStatements=%d runtimeDir=%s",
                walLog.lastSequence(), compactedWal, replay.size(), miniSqlRuntimeDir);
    }

    private String runOnEngine(long sequenceExclusive, String sql, long appliedWalSequence) {
        long compactedWal = snapshotManifest == null ? 0 : snapshotManifest.lastCompactedWalSequence;
        try {
            if (!miniSqlEngine.isReady() || miniSqlEngine.engineCompactedWal() != compactedWal) {
                fullSyncEngine(sequenceExclusive);
            } else {
                catchUpWalBefore(sequenceExclusive);
            }
            String output = sql == null || sql.isBlank() ? "" : miniSqlEngine.executeStatement(sql);
            miniSqlEngine.setEngineWalSequence(appliedWalSequence);
            return output;
        } catch (IllegalStateException failed) {
            warn("MiniSQL engine resync after failure: %s", failed.getMessage());
            fullSyncEngine(sequenceExclusive);
            String output = sql == null || sql.isBlank() ? "" : miniSqlEngine.executeStatement(sql);
            miniSqlEngine.setEngineWalSequence(appliedWalSequence);
            return output;
        }
    }

    private void fullSyncEngine(long sequenceExclusive) {
        long compactedWal = snapshotManifest == null ? 0 : snapshotManifest.lastCompactedWalSequence;
        List<String> replay = replaySqlBefore(sequenceExclusive);
        long replayThrough = walLog.readAll().stream()
                .filter(entry -> entry.sequence < sequenceExclusive)
                .filter(entry -> entry.sequence > compactedWal)
                .mapToLong(entry -> entry.sequence)
                .max()
                .orElse(compactedWal);
        miniSqlEngine.resetAndReplay(replay, replayThrough, compactedWal);
        debug("MiniSQL engine full sync replayStatements=%d replayThrough=%d compactedWal=%d",
                replay.size(), replayThrough, compactedWal);
    }

    private void catchUpWalBefore(long sequenceExclusive) {
        if (sequenceExclusive == Long.MAX_VALUE) {
            sequenceExclusive = walLog.lastSequence() + 1;
        }
        for (WalEntry entry : walLog.readAll()) {
            if (entry.sequence <= miniSqlEngine.engineWalSequence()) {
                continue;
            }
            if (entry.sequence >= sequenceExclusive) {
                break;
            }
            miniSqlEngine.executeStatement(entry.sql);
            miniSqlEngine.setEngineWalSequence(entry.sequence);
        }
    }

    private List<String> replaySqlBefore(long sequenceExclusive) {
        List<String> replay = new ArrayList<>(snapshotManager.snapshotSql());
        walLog.readAll().stream()
                .filter(entry -> entry.sequence > snapshotManifest.lastCompactedWalSequence)
                .filter(entry -> entry.sequence < sequenceExclusive)
                .map(entry -> entry.sql)
                .forEach(replay::add);
        return replay;
    }

    private void maybeSnapshot() {
        long compactThrough = walLog.lastSequence();
        if (snapshotManifest != null && compactThrough - snapshotManifest.lastCompactedWalSequence < 20) {
            debug("skip auto snapshot lastWal=%d compactedWal=%d threshold=20",
                    compactThrough, snapshotManifest.lastCompactedWalSequence);
            return;
        }
        log("auto snapshot start compactThrough=%d", compactThrough);
        List<String> appliedSql = new ArrayList<>(snapshotManager.snapshotSql());
        walLog.readAll().stream()
                .filter(entry -> entry.sequence <= compactThrough)
                .map(entry -> entry.sql)
                .forEach(appliedSql::add);
        snapshotManifest = snapshotManager.createSnapshot(compactThrough, currentShardLogIndexes(), appliedSql);
        walLog.compactThrough(compactThrough);
        miniSqlEngine.invalidate();
        log("auto snapshot completed compactedWal=%d appliedSql=%d shardLogIndexes=%s",
                snapshotManifest.lastCompactedWalSequence, appliedSql.size(), snapshotManifest.shardLogIndexes);
    }

    private void register() {
        metadataStore.registerNode(currentNodeInfo());
        log("registered node state=%s shards=%s lastWal=%d shardLogIndexes=%s",
                replicaState, shards, walLog.lastSequence(), currentShardLogIndexes());
    }

    private void refreshShards() {
        Set<Integer> before = new HashSet<>(shards);
        shards.clear();
        shards.addAll(metadataStore.shardsForNode(nodeConfig.nodeId));
        log("refreshed shards before=%s after=%s", before, shards);
    }

    private NodeInfo currentNodeInfo() {
        return new NodeInfo(nodeConfig.nodeId, nodeConfig.host, nodeConfig.port, shards, walLog.lastSequence(),
                replicaState, currentShardLogIndexes());
    }

    private RecoveryState recoveryState() {
        return new RecoveryState(snapshotManifest, snapshotManager.snapshotSql(), walLog.readAll());
    }

    private Map<Integer, Long> currentShardLogIndexes() {
        Map<Integer, Long> indexes = new TreeMap<>();
        if (snapshotManifest != null) {
            indexes.putAll(snapshotManifest.shardLogIndexes);
        }
        for (Map.Entry<Integer, Long> entry : walLog.maxShardLogIndexes().entrySet()) {
            indexes.merge(entry.getKey(), entry.getValue(), Math::max);
        }
        return indexes;
    }

    private void debug(String format, Object... args) {
        writeLog("DEBUG", format, args);
    }

    private void log(String format, Object... args) {
        info(format, args);
    }

    private void info(String format, Object... args) {
        writeLog("INFO", format, args);
    }

    private void warn(String format, Object... args) {
        writeLog("WARN", format, args);
    }

    private void error(String format, Object... args) {
        writeLog("ERROR", format, args);
    }

    private void writeLog(String level, String format, Object... args) {
        System.out.printf("%s %-5s [%s] %s%n", Instant.now(), level, nodeConfig.nodeId, String.format(format, args));
        System.out.flush();
    }

    private void logMiniSqlOutput(String requestId, String output) {
        if (output == null || output.isBlank()) {
            debug("miniSQL output requestId=%s <empty>", requestId);
            return;
        }
        debug("miniSQL output begin requestId=%s", requestId);
        for (String line : output.split("\\R")) {
            debug("miniSQL output requestId=%s | %s", requestId, line);
        }
        debug("miniSQL output end requestId=%s", requestId);
    }

    private String oneLine(String sql) {
        if (sql == null) {
            return "<none>";
        }
        return sql.replaceAll("\\s+", " ").trim();
    }

    private int lineCount(String output) {
        if (output == null || output.isBlank()) {
            return 0;
        }
        return output.split("\\R").length;
    }
}
