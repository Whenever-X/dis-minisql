package edu.minisql.distributed.coordinator;

import edu.minisql.distributed.common.HttpUtil;
import edu.minisql.distributed.config.ClusterConfig;
import edu.minisql.distributed.protocol.ExecuteResponse;
import edu.minisql.distributed.protocol.NodeInfo;
import edu.minisql.distributed.protocol.ShardMetadata;
import edu.minisql.distributed.protocol.SqlRequest;
import edu.minisql.distributed.protocol.SqlResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Locale;
import java.util.stream.Collectors;

public class CoordinatorCli {
    private final String coordinatorBaseUrl;
    private final BufferedReader in;
    private final PrintStream out;
    private final boolean interactive;

    public CoordinatorCli(ClusterConfig config) {
        this(coordinatorBaseUrl(config), System.in, System.out, System.console() != null);
    }

    public CoordinatorCli(String coordinatorBaseUrl, InputStream in, PrintStream out, boolean interactive) {
        this.coordinatorBaseUrl = stripTrailingSlash(coordinatorBaseUrl);
        this.in = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        this.out = out;
        this.interactive = interactive;
    }

    public void run() throws IOException {
        out.println("Distributed MiniSQL Client CLI");
        out.println("Connected coordinator: " + coordinatorBaseUrl);
        out.println("Type help for commands. Type quit or exit to stop the client.");
        while (true) {
            if (interactive) {
                out.print("dis-minisql> ");
                out.flush();
            }
            String line = in.readLine();
            if (line == null) {
                out.println("EOF received, exiting client.");
                return;
            }
            if (!handle(line.trim())) {
                return;
            }
        }
    }

    private boolean handle(String line) {
        if (line.isBlank()) {
            return true;
        }
        String command = firstToken(line).toLowerCase(Locale.ROOT);
        try {
            switch (command) {
                case "help":
                    printHelp();
                    return true;
                case "quit":
                case "exit":
                    out.println("Exiting client.");
                    return false;
                case "nodes":
                    printNodes(get("/nodes", NodeInfo[].class));
                    return true;
                case "metadata":
                    printShards("Shard metadata", get("/metadata", ShardMetadata[].class));
                    return true;
                case "health":
                    printHealth();
                    return true;
                case "rebalance":
                    ShardMetadata[] rebalanced = post("/admin/rebalance", Map.of(), ShardMetadata[].class);
                    out.println("Rebalance completed.");
                    printShards("Updated shard metadata", rebalanced);
                    return true;
                case "snapshot":
                    NodeInfo[] snapshotted = post("/admin/snapshot", Map.of(), NodeInfo[].class);
                    out.printf("Snapshot requested on %d live DataNode(s).%n", snapshotted.length);
                    printNodes(snapshotted);
                    return true;
                case "shard":
                    executeWithShardKey(line);
                    return true;
                default:
                    printSqlResponse(execute(line, null));
                    return true;
            }
        } catch (Exception e) {
            out.println("ERROR: " + e.getMessage());
            return true;
        }
    }

    private void printHelp() {
        out.println("Commands:");
        out.println("  <sql>                   Execute one SQL statement through the coordinator");
        out.println("  shard <key> <sql>       Execute SQL with an explicit shard key");
        out.println("  nodes                   Show live DataNodes");
        out.println("  metadata                Show shard metadata");
        out.println("  health                  Show cluster health");
        out.println("  rebalance               Recalculate shard replicas and refresh live nodes");
        out.println("  snapshot                Trigger snapshots on live DataNodes");
        out.println("  help                    Show this help");
        out.println("  quit | exit             Exit this client; the coordinator keeps running");
    }

    private void executeWithShardKey(String line) {
        String rest = line.substring(firstToken(line).length()).trim();
        int split = rest.indexOf(' ');
        if (split <= 0 || split == rest.length() - 1) {
            out.println("Usage: shard <key> <sql>");
            return;
        }
        String shardKey = rest.substring(0, split);
        String sql = rest.substring(split + 1).trim();
        printSqlResponse(execute(sql, shardKey));
    }

    private SqlResponse execute(String sql, String shardKey) {
        return post("/sql", new SqlRequest(sql, shardKey), SqlResponse.class);
    }

    private <T> T get(String path, Class<T> responseType) {
        return HttpUtil.getJson(coordinatorBaseUrl + path, responseType);
    }

    private <T> T post(String path, Object request, Class<T> responseType) {
        return HttpUtil.postJson(coordinatorBaseUrl + path, request, responseType);
    }

    private void printSqlResponse(SqlResponse response) {
        if (!response.ok) {
            out.println("ERROR: " + response.error);
            printResponseSummary(response);
            printMergedErrors(response);
            return;
        }
        out.println("OK: " + response.route);
        if (response.mergedOutput != null && !response.mergedOutput.isBlank()) {
            out.println(response.mergedOutput.stripTrailing());
            printResponseSummary(response);
            return;
        }
        if (response.route != null && response.route.toLowerCase(Locale.ROOT).contains("read")) {
            out.println("Empty result.");
        }
        printResponseSummary(response);
    }

    private void printMergedErrors(SqlResponse response) {
        Map<String, Integer> errors = new LinkedHashMap<>();
        for (ExecuteResponse nodeResponse : response.responses) {
            if (nodeResponse.ok) {
                continue;
            }
            String error = nodeResponse.error == null ? "" : nodeResponse.error.strip();
            if (!error.isBlank()) {
                errors.merge(error, 1, Integer::sum);
            }
        }
        errors.forEach((error, count) -> out.printf("Failure repeated on %d DataNode(s): %s%n", count, error));
    }

    private void printResponseSummary(SqlResponse response) {
        if (response.responses == null || response.responses.isEmpty()) {
            return;
        }
        long success = response.responses.stream().filter(nodeResponse -> nodeResponse.ok).count();
        long failed = response.responses.size() - success;
        out.printf("DataNode acknowledgements: %d ok, %d failed.%n", success, failed);
    }

    private void printHealth() {
        NodeInfo[] nodes = get("/nodes", NodeInfo[].class);
        ShardMetadata[] shards = get("/metadata", ShardMetadata[].class);
        long serving = Arrays.stream(nodes)
                .filter(node -> "SERVING".equalsIgnoreCase(node.replicaState))
                .count();
        long recovering = Arrays.stream(nodes)
                .filter(node -> "RECOVERING".equalsIgnoreCase(node.replicaState))
                .count();
        out.printf("Cluster health: %d live DataNode(s), %d shard(s).%n", nodes.length, shards.length);
        out.printf("Replica states: %d SERVING, %d RECOVERING.%n", serving, recovering);
        printNodes(nodes);
        printShards("Shard metadata", shards);
    }

    private void printNodes(NodeInfo[] nodes) {
        if (nodes.length == 0) {
            out.println("No live DataNodes registered.");
            return;
        }
        List<NodeInfo> sorted = new ArrayList<>(Arrays.asList(nodes));
        sorted.sort(Comparator.comparing(node -> node.nodeId));
        out.printf("Live DataNodes (%d):%n", sorted.size());
        out.printf("%-10s %-21s %-10s %-8s %-12s %s%n",
                "NODE", "ADDRESS", "STATE", "WAL", "SHARDS", "SHARD_LOG_INDEXES");
        for (NodeInfo node : sorted) {
            out.printf("%-10s %-21s %-10s %-8d %-12s %s%n",
                    node.nodeId,
                    node.host + ":" + node.port,
                    node.replicaState,
                    node.lastWalSequence,
                    formatIntegerList(new ArrayList<>(node.shards)),
                    formatShardLogIndexes(node.shardLogIndexes));
        }
    }

    private void printShards(String title, ShardMetadata[] shards) {
        if (shards.length == 0) {
            out.println(title + ": no shard metadata.");
            return;
        }
        List<ShardMetadata> sorted = new ArrayList<>(Arrays.asList(shards));
        sorted.sort(Comparator.comparingInt(shard -> shard.shardId));
        out.printf("%s (%d):%n", title, sorted.size());
        out.printf("%-6s %-10s %-6s %-8s %s%n", "SHARD", "PRIMARY", "TERM", "COMMIT", "REPLICAS");
        for (ShardMetadata shard : sorted) {
            out.printf("%-6d %-10s %-6d %-8d %s%n",
                    shard.shardId,
                    shard.primary,
                    shard.term,
                    shard.commitIndex,
                    String.join(",", shard.replicas));
        }
    }

    private String formatIntegerList(List<Integer> values) {
        values.sort(Integer::compareTo);
        return values.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(",", "[", "]"));
    }

    private String formatShardLogIndexes(Map<Integer, Long> indexes) {
        if (indexes == null || indexes.isEmpty()) {
            return "{}";
        }
        return indexes.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> entry.getKey() + ":" + entry.getValue())
                .collect(Collectors.joining(",", "{", "}"));
    }

    private String firstToken(String line) {
        int split = line.indexOf(' ');
        return split < 0 ? line : line.substring(0, split);
    }

    private static String coordinatorBaseUrl(ClusterConfig config) {
        String host = "0.0.0.0".equals(config.coordinatorHost) ? "127.0.0.1" : config.coordinatorHost;
        return "http://" + host + ":" + config.coordinatorPort;
    }

    private static String stripTrailingSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }
}
