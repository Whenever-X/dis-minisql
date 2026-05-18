package edu.minisql.distributed.coordinator;

import com.sun.net.httpserver.HttpServer;
import edu.minisql.distributed.common.HttpUtil;
import edu.minisql.distributed.common.Jsons;
import edu.minisql.distributed.common.SqlUtils;
import edu.minisql.distributed.config.ClusterConfig;
import edu.minisql.distributed.protocol.ExecuteRequest;
import edu.minisql.distributed.protocol.ExecuteResponse;
import edu.minisql.distributed.protocol.NodeInfo;
import edu.minisql.distributed.protocol.ShardMetadata;
import edu.minisql.distributed.protocol.SqlRequest;
import edu.minisql.distributed.protocol.SqlResponse;
import edu.minisql.distributed.zk.ZkMetadataStore;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class CoordinatorServer implements AutoCloseable {
    private final ClusterConfig config;
    private final ZkMetadataStore metadataStore;
    private final ReplicaChooser replicaChooser = new ReplicaChooser();
    private final QueryPostProcessor queryPostProcessor = new QueryPostProcessor();
    private HttpServer server;
    private ExecutorService executor;

    public CoordinatorServer(ClusterConfig config) {
        this.config = config;
        this.metadataStore = new ZkMetadataStore(config);
        this.metadataStore.initializeShards(config);
        info("constructed cluster=%s zk=%s listen=%s:%d shardCount=%d replicationFactor=%d",
                config.clusterName, config.zkConnect, config.coordinatorHost, config.coordinatorPort,
                config.shardCount, config.replicationFactor);
    }

    public synchronized void start() throws IOException {
        if (server != null) {
            throw new IllegalStateException("Coordinator already started");
        }
        info("lifecycle STARTING");
        server = HttpServer.create(new InetSocketAddress(config.coordinatorHost, config.coordinatorPort), 0);
        server.createContext("/sql", exchange -> {
            try {
                SqlRequest request = Jsons.parse(HttpUtil.body(exchange), SqlRequest.class);
                info("http /sql sql=%s shardKey=%s", oneLine(request.sql), request.shardKey);
                SqlResponse response = execute(request);
                info("http /sql completed ok=%s route=%s responses=%d error=%s",
                        response.ok, response.route, response.responses.size(), response.error);
                HttpUtil.json(exchange, response.ok ? 200 : 500, response);
            } catch (Exception e) {
                error("http /sql failed: %s", e.getMessage());
                HttpUtil.json(exchange, 500, SqlResponse.error(e.getMessage()));
            }
        });
        server.createContext("/metadata", exchange -> {
            List<ShardMetadata> metadata = metadata();
            debug("http /metadata shards=%d", metadata.size());
            HttpUtil.json(exchange, 200, metadata);
        });
        server.createContext("/nodes", exchange -> {
            List<NodeInfo> nodes = nodes();
            debug("http /nodes liveNodes=%d", nodes.size());
            HttpUtil.json(exchange, 200, nodes);
        });
        server.createContext("/admin/health", exchange -> {
            info("http /admin/health");
            HttpUtil.json(exchange, 200, health());
        });
        server.createContext("/admin/rebalance", exchange -> {
            info("http /admin/rebalance requested");
            List<ShardMetadata> updated = rebalance();
            info("http /admin/rebalance completed shards=%d", updated.size());
            HttpUtil.json(exchange, 200, updated);
        });
        server.createContext("/admin/snapshot", exchange -> {
            info("http /admin/snapshot requested");
            List<NodeInfo> results = snapshot();
            info("http /admin/snapshot completed requestedNodes=%d", results.size());
            HttpUtil.json(exchange, 200, results);
        });
        executor = Executors.newFixedThreadPool(16);
        server.setExecutor(executor);
        server.start();
        info("lifecycle SERVING address=%s:%d", config.coordinatorHost, config.coordinatorPort);
        System.out.printf("Coordinator serving %s at %s:%d%n",
                config.clusterName, config.coordinatorHost, config.coordinatorPort);
    }

    public List<ShardMetadata> metadata() {
        return metadataStore.shards();
    }

    public List<NodeInfo> nodes() {
        return metadataStore.liveNodes();
    }

    public Map<String, Object> health() {
        return Map.of(
                "nodes", nodes(),
                "shards", metadata()
        );
    }

    public List<ShardMetadata> rebalance() {
        List<NodeInfo> liveNodes = metadataStore.liveNodes();
        info("rebalance start liveNodes=%d replicationFactor=%d", liveNodes.size(), config.replicationFactor);
        List<ShardMetadata> updated = metadataStore.rebalance(liveNodes, config.replicationFactor);
        refreshAllLiveNodes();
        info("rebalance complete updatedShards=%d", updated.size());
        return updated;
    }

    public List<NodeInfo> snapshot() {
        info("snapshot start");
        return snapshotAllLiveNodes();
    }

    public SqlResponse execute(SqlRequest request) {
        String sql = SqlUtils.normalize(request.sql);
        if (sql.isBlank()) {
            warn("execute rejected reason=empty sql");
            return SqlResponse.error("sql is empty");
        }
        info("execute start op=%s shardKey=%s sql=%s", SqlUtils.firstKeyword(sql), request.shardKey, oneLine(sql));
        if (SqlUtils.isBroadcastDdl(sql)) {
            info("execute route=broadcast-ddl sql=%s", oneLine(sql));
            return broadcastWrite(sql);
        }
        if (SqlUtils.isReadOnly(sql)) {
            if (queryPostProcessor.isSimpleJoin(sql)) {
                info("execute route=distributed-join sql=%s", oneLine(sql));
                return distributedJoin(sql);
            }
            String dispatchSql = queryPostProcessor.isAggregate(sql)
                    ? queryPostProcessor.baseSqlForAggregate(sql)
                    : queryPostProcessor.sqlWithoutOrderBy(sql);
            info("execute route=distributed-read dispatchSql=%s", oneLine(dispatchSql));
            return distributedRead(sql, dispatchSql, request.shardKey);
        }
        info("execute route=routed-write sql=%s", oneLine(sql));
        return routedWrite(sql, request.shardKey);
    }

    private SqlResponse routedWrite(String sql, String explicitShardKey) {
        String shardKey = explicitShardKey != null && !explicitShardKey.isBlank()
                ? explicitShardKey
                : SqlUtils.inferShardKey(sql);
        if (shardKey == null || shardKey.isBlank()) {
            warn("routed write rejected reason=no shard key sql=%s", oneLine(sql));
            return SqlResponse.error("No shard key provided. Put the shard key as first INSERT value or pass shardKey.");
        }
        int shardId = SqlUtils.hashToShard(shardKey, config.shardCount);
        ShardMetadata shard = metadataStore.shard(shardId);
        List<NodeInfo> liveNodes = metadataStore.liveNodes();
        shard = metadataStore.electPrimary(shardId, liveNodes);
        List<NodeInfo> replicas = replicaChooser.chooseForWrite(shard.replicas, liveNodes);
        info("routed write shardKey=%s shard=%d primary=%s term=%d replicas=%s liveNodes=%d",
                shardKey, shardId, shard.primary, shard.term, shard.replicas, liveNodes.size());
        if (replicas.isEmpty()) {
            warn("routed write failed shard=%d reason=no live replica", shardId);
            return SqlResponse.error("No live replica for shard " + shardId);
        }
        long shardLogIndex = metadataStore.nextShardLogIndex(shardId);
        String requestId = UUID.randomUUID().toString();
        info("routed write dispatch requestId=%s shard=%d shardLogIndex=%d", requestId, shardId, shardLogIndex);
        List<ExecuteResponse> responses = sendRaftStyle(requestId, shard, shardLogIndex, sql, replicas);
        long successCount = responses.stream().filter(response -> response.ok).count();
        int quorum = quorumSize(shard.replicas.size());
        if (successCount >= quorum) {
            metadataStore.updateCommitIndex(shardId, shardLogIndex);
            info("routed write committed requestId=%s shard=%d success=%d quorum=%d commitIndex=%d",
                    requestId, shardId, successCount, quorum, shardLogIndex);
            return SqlResponse.ok("raft write shard " + shardId + " term " + shard.term
                    + " commitIndex " + shardLogIndex, responses);
        }
        warn("routed write quorum failed requestId=%s shard=%d success=%d quorum=%d", requestId, shardId, successCount, quorum);
        return SqlResponse.error("Raft quorum failed for shard " + shardId + ": "
                + successCount + "/" + quorum + " acknowledgements");
    }

    private SqlResponse broadcastWrite(String sql) {
        List<NodeInfo> liveNodes = metadataStore.liveNodes();
        if (liveNodes.isEmpty()) {
            warn("broadcast write failed reason=no live nodes sql=%s", oneLine(sql));
            return SqlResponse.error("No live nodes");
        }
        String requestId = UUID.randomUUID().toString();
        info("broadcast write start requestId=%s liveNodes=%d sql=%s", requestId, liveNodes.size(), oneLine(sql));
        List<ExecuteResponse> responses = sendToReplicas(requestId, -1, 0, 0, sql, liveNodes, false, 0);
        long successCount = responses.stream().filter(response -> response.ok).count();
        int quorum = quorumSize(liveNodes.size());
        info("broadcast write complete requestId=%s success=%d quorum=%d", requestId, successCount, quorum);
        return successCount >= quorum ? SqlResponse.ok("quorum broadcast", responses)
                : SqlResponse.error("Broadcast quorum failed: " + successCount + "/" + quorum);
    }

    private SqlResponse distributedRead(String originalSql, String dispatchSql, String explicitShardKey) {
        List<ExecuteResponse> responses = readResponses(dispatchSql, explicitShardKey);
        if (responses.isEmpty()) {
            warn("distributed read failed reason=no live nodes sql=%s", oneLine(dispatchSql));
            return SqlResponse.error("No live nodes");
        }
        String merged = queryPostProcessor.mergeSelect(originalSql, responses);
        String route = explicitShardKey != null && !explicitShardKey.isBlank() ? "read single shard" : "scatter-gather read";
        info("distributed read complete route=%s responses=%d mergedLines=%d", route, responses.size(), lineCount(merged));
        return SqlResponse.ok(route, responses, merged);
    }

    private SqlResponse distributedJoin(String sql) {
        List<ExecuteResponse> leftResponses = readResponses(queryPostProcessor.leftJoinSql(sql), null);
        List<ExecuteResponse> rightResponses = readResponses(queryPostProcessor.rightJoinSql(sql), null);
        List<ExecuteResponse> allResponses = new ArrayList<>();
        allResponses.addAll(leftResponses);
        allResponses.addAll(rightResponses);
        if (allResponses.isEmpty()) {
            warn("distributed join failed reason=no live nodes sql=%s", oneLine(sql));
            return SqlResponse.error("No live nodes");
        }
        String merged = queryPostProcessor.join(sql, leftResponses, rightResponses);
        info("distributed join complete responses=%d mergedLines=%d", allResponses.size(), lineCount(merged));
        return SqlResponse.ok("coordinator hash join", allResponses, merged);
    }

    private List<ExecuteResponse> readResponses(String sql, String explicitShardKey) {
        List<ExecuteResponse> responses = new ArrayList<>();
        List<NodeInfo> liveNodes = metadataStore.liveNodes();
        if (liveNodes.isEmpty()) {
            warn("read responses failed reason=no live nodes sql=%s", oneLine(sql));
            return responses;
        }
        if (explicitShardKey != null && !explicitShardKey.isBlank()) {
            int shardId = SqlUtils.hashToShard(explicitShardKey, config.shardCount);
            ShardMetadata shard = metadataStore.shard(shardId);
            NodeInfo node = replicaChooser.chooseForRead(shardId, shard.replicas, liveNodes);
            info("read single shard shardKey=%s shard=%d node=%s sql=%s", explicitShardKey, shardId, node.nodeId, oneLine(sql));
            responses.add(send(UUID.randomUUID().toString(), shardId, 0, sql, node, false, 0));
            return responses;
        }
        for (ShardMetadata shard : metadataStore.shards()) {
            NodeInfo node = replicaChooser.chooseForRead(shard.shardId, shard.replicas, liveNodes);
            debug("scatter read shard=%d node=%s sql=%s", shard.shardId, node.nodeId, oneLine(sql));
            responses.add(send(UUID.randomUUID().toString(), shard.shardId, 0, sql, node, false, 0));
        }
        return responses;
    }

    private List<ExecuteResponse> sendToReplicas(String requestId, int shardId, long shardLogIndex, String sql,
                                                 List<NodeInfo> replicas, boolean replay, long walSequence) {
        return sendToReplicas(requestId, shardId, shardLogIndex, 0, sql, replicas, replay, walSequence);
    }

    private List<ExecuteResponse> sendToReplicas(String requestId, int shardId, long shardLogIndex, long raftTerm,
                                                 String sql, List<NodeInfo> replicas, boolean replay, long walSequence) {
        List<ExecuteResponse> responses = new ArrayList<>();
        for (NodeInfo node : replicas) {
            responses.add(send(requestId, shardId, shardLogIndex, raftTerm, sql, node, replay, walSequence));
        }
        return responses;
    }

    private List<ExecuteResponse> sendRaftStyle(String requestId, ShardMetadata shard, long shardLogIndex,
                                                String sql, List<NodeInfo> replicas) {
        Map<String, NodeInfo> byId = replicas.stream().collect(Collectors.toMap(node -> node.nodeId, node -> node));
        List<ExecuteResponse> responses = new ArrayList<>();
        NodeInfo primary = byId.get(shard.primary);
        if (primary != null) {
            responses.add(send(requestId, shard.shardId, shardLogIndex, shard.term, sql, primary, false, 0));
        }
        for (NodeInfo replica : replicas) {
            if (!replica.nodeId.equals(shard.primary)) {
                responses.add(send(requestId, shard.shardId, shardLogIndex, shard.term, sql, replica, true, 0));
            }
        }
        return responses;
    }

    private ExecuteResponse send(String requestId, int shardId, long shardLogIndex, String sql, NodeInfo node,
                                 boolean replay, long walSequence) {
        return send(requestId, shardId, shardLogIndex, 0, sql, node, replay, walSequence);
    }

    private ExecuteResponse send(String requestId, int shardId, long shardLogIndex, long raftTerm, String sql,
                                 NodeInfo node, boolean replay, long walSequence) {
        try {
            ExecuteRequest request = new ExecuteRequest(requestId, shardId, shardLogIndex, raftTerm, sql, walSequence, replay);
            debug("send node=%s requestId=%s shard=%d shardLogIndex=%d raftTerm=%d replay=%s sql=%s",
                    node.nodeId, requestId, shardId, shardLogIndex, raftTerm, replay, oneLine(sql));
            return HttpUtil.postJson(node.baseUrl() + "/execute", request, ExecuteResponse.class);
        } catch (Exception e) {
            warn("send failed node=%s requestId=%s error=%s", node.nodeId, requestId, e.getMessage());
            return ExecuteResponse.error(node.nodeId, e.getMessage());
        }
    }

    private int quorumSize(int replicaCount) {
        return replicaCount / 2 + 1;
    }

    private List<NodeInfo> refreshAllLiveNodes() {
        List<NodeInfo> results = new ArrayList<>();
        for (NodeInfo node : metadataStore.liveNodes()) {
            try {
                info("refresh live node node=%s", node.nodeId);
                results.add(HttpUtil.postJson(node.baseUrl() + "/admin/refresh-shards", Map.of(), NodeInfo.class));
            } catch (Exception ignored) {
                warn("refresh live node failed node=%s", node.nodeId);
            }
        }
        return results;
    }

    private List<NodeInfo> snapshotAllLiveNodes() {
        List<NodeInfo> results = new ArrayList<>();
        for (NodeInfo node : metadataStore.liveNodes()) {
            try {
                info("snapshot live node node=%s", node.nodeId);
                HttpUtil.postJson(node.baseUrl() + "/admin/snapshot", Map.of(), Object.class);
                results.add(node);
            } catch (Exception ignored) {
                warn("snapshot live node failed node=%s", node.nodeId);
            }
        }
        return results;
    }

    public synchronized void stop() {
        info("lifecycle STOPPING");
        if (server != null) {
            server.stop(0);
            server = null;
        }
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    @Override
    public synchronized void close() throws IOException {
        stop();
        metadataStore.close();
    }

    private void debug(String format, Object... args) {
        writeLog("DEBUG", format, args);
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
        System.out.printf("%s %-5s [coordinator] %s%n", Instant.now(), level, String.format(format, args));
        System.out.flush();
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
