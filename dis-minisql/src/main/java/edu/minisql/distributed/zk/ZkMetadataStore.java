package edu.minisql.distributed.zk;

import edu.minisql.distributed.common.Jsons;
import edu.minisql.distributed.config.ClusterConfig;
import edu.minisql.distributed.protocol.NodeInfo;
import edu.minisql.distributed.protocol.ShardMetadata;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class ZkMetadataStore implements Closeable {
    private final ZooKeeper zk;
    private final ZkPaths paths;

    public ZkMetadataStore(ClusterConfig config) {
        try {
            CountDownLatch connected = new CountDownLatch(1);
            this.zk = new ZooKeeper(config.zkConnect, config.sessionTimeoutMs, event -> {
                if (event.getState() == org.apache.zookeeper.Watcher.Event.KeeperState.SyncConnected) {
                    connected.countDown();
                }
            });
            connected.await();
            this.paths = new ZkPaths(config.clusterName);
            ensurePersistent(paths.root());
            ensurePersistent(paths.nodes());
            ensurePersistent(paths.shards());
            ensurePersistent(paths.shardLogIndexes());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to connect ZooKeeper", e);
        }
    }

    public void initializeShards(ClusterConfig config) {
        List<String> nodeIds = config.nodes.stream()
                .map(node -> node.nodeId)
                .sorted()
                .collect(Collectors.toList());
        if (nodeIds.isEmpty()) {
            throw new IllegalArgumentException("At least one data node is required");
        }
        for (int shardId = 0; shardId < config.shardCount; shardId++) {
            List<String> replicas = new ArrayList<>();
            for (int replica = 0; replica < Math.min(config.replicationFactor, nodeIds.size()); replica++) {
                replicas.add(nodeIds.get((shardId + replica) % nodeIds.size()));
            }
            ShardMetadata metadata = new ShardMetadata(shardId, replicas, replicas.get(0));
            upsertPersistentIfAbsent(paths.shard(shardId), Jsons.bytes(metadata));
            upsertPersistentIfAbsent(paths.shardLogIndex(shardId), "0".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    public long nextShardLogIndex(int shardId) {
        if (shardId < 0) {
            return 0L;
        }
        String path = paths.shardLogIndex(shardId);
        while (true) {
            try {
                Stat stat = new Stat();
                byte[] current = zk.getData(path, false, stat);
                long next = Long.parseLong(new String(current, java.nio.charset.StandardCharsets.UTF_8)) + 1;
                zk.setData(path, Long.toString(next).getBytes(java.nio.charset.StandardCharsets.UTF_8), stat.getVersion());
                return next;
            } catch (KeeperException.BadVersionException ignored) {
            } catch (KeeperException.NoNodeException e) {
                upsertPersistentIfAbsent(path, "0".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            } catch (Exception e) {
                throw new IllegalStateException("Failed to allocate shard log index for shard " + shardId, e);
            }
        }
    }

    public void registerNode(NodeInfo info) {
        upsertEphemeral(paths.node(info.nodeId), Jsons.bytes(info));
    }

    public void updateNode(NodeInfo info) {
        upsertEphemeral(paths.node(info.nodeId), Jsons.bytes(info));
    }

    public List<NodeInfo> liveNodes() {
        try {
            List<NodeInfo> result = new ArrayList<>();
            for (String child : zk.getChildren(paths.nodes(), false)) {
                byte[] data = zk.getData(paths.nodes() + "/" + child, false, null);
                result.add(Jsons.parse(data, NodeInfo.class));
            }
            result.sort(Comparator.comparing(node -> node.nodeId));
            return result;
        } catch (KeeperException.NoNodeException e) {
            return List.of();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read live nodes", e);
        }
    }

    public List<ShardMetadata> shards() {
        try {
            List<ShardMetadata> result = new ArrayList<>();
            for (String child : zk.getChildren(paths.shards(), false)) {
                byte[] data = zk.getData(paths.shards() + "/" + child, false, null);
                result.add(Jsons.parse(data, ShardMetadata.class));
            }
            result.sort(Comparator.comparingInt(shard -> shard.shardId));
            return result;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read shards", e);
        }
    }

    public ShardMetadata shard(int shardId) {
        try {
            byte[] data = zk.getData(paths.shard(shardId), false, null);
            return Jsons.parse(data, ShardMetadata.class);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read shard " + shardId, e);
        }
    }

    public void updateShard(ShardMetadata metadata) {
        upsertPersistent(paths.shard(metadata.shardId), Jsons.bytes(metadata));
    }

    public ShardMetadata electPrimary(int shardId, List<NodeInfo> liveNodes) {
        ShardMetadata metadata = shard(shardId);
        Set<String> liveServing = liveServingNodeIds(liveNodes);
        if (metadata.primary != null && liveServing.contains(metadata.primary)) {
            return metadata;
        }
        List<String> liveReplicas = metadata.replicas.stream()
                .filter(liveServing::contains)
                .collect(Collectors.toList());
        if (liveReplicas.isEmpty()) {
            return metadata;
        }
        String previousPrimary = metadata.primary;
        metadata.primary = chooseBalancedPrimary(liveReplicas, primaryCountsExcludingShard(shardId));
        if (!Objects.equals(previousPrimary, metadata.primary)) {
            metadata.term++;
            updateShard(metadata);
        }
        return metadata;
    }

    public List<ShardMetadata> rebalance(List<NodeInfo> liveNodes, int replicationFactor) {
        List<String> liveIds = liveNodes.stream()
                .filter(node -> "SERVING".equalsIgnoreCase(node.replicaState)
                        || "RECOVERING".equalsIgnoreCase(node.replicaState))
                .map(node -> node.nodeId)
                .sorted()
                .collect(Collectors.toList());
        if (liveIds.isEmpty()) {
            return shards();
        }
        List<ShardMetadata> updated = new ArrayList<>();
        Map<Integer, List<String>> previousReplicas = new HashMap<>();
        Map<Integer, String> previousPrimaries = new HashMap<>();
        for (ShardMetadata shard : shards()) {
            previousReplicas.put(shard.shardId, new ArrayList<>(shard.replicas));
            previousPrimaries.put(shard.shardId, shard.primary);
            List<String> replicas = new ArrayList<>();
            int replicaTarget = Math.min(replicationFactor, liveIds.size());
            for (int replica = 0; replica < replicaTarget; replica++) {
                replicas.add(liveIds.get((shard.shardId + replica) % liveIds.size()));
            }
            replicas.sort(String::compareTo);
            shard.replicas = replicas;
            updated.add(shard);
        }
        assignBalancedPrimaries(updated);
        for (ShardMetadata shard : updated) {
            boolean replicasChanged = !shard.replicas.equals(previousReplicas.get(shard.shardId));
            boolean primaryChanged = !Objects.equals(shard.primary, previousPrimaries.get(shard.shardId));
            if (replicasChanged || primaryChanged) {
                shard.term++;
            }
            updateShard(shard);
        }
        return updated;
    }

    private void assignBalancedPrimaries(List<ShardMetadata> shards) {
        Map<String, Integer> primaryCounts = new HashMap<>();
        shards.sort(Comparator.comparingInt(shard -> shard.shardId));
        for (ShardMetadata shard : shards) {
            if (shard.replicas.isEmpty()) {
                shard.primary = null;
                continue;
            }
            shard.primary = chooseBalancedPrimary(shard.replicas, primaryCounts);
            primaryCounts.merge(shard.primary, 1, Integer::sum);
        }
    }

    private String chooseBalancedPrimary(List<String> replicas, Map<String, Integer> primaryCounts) {
        return replicas.stream()
                .min(Comparator
                        .comparingInt((String nodeId) -> primaryCounts.getOrDefault(nodeId, 0))
                        .thenComparing(nodeId -> nodeId))
                .orElse(replicas.get(0));
    }

    private Map<String, Integer> primaryCountsExcludingShard(int excludedShardId) {
        Map<String, Integer> counts = new HashMap<>();
        for (ShardMetadata shard : shards()) {
            if (shard.shardId == excludedShardId || shard.primary == null) {
                continue;
            }
            counts.merge(shard.primary, 1, Integer::sum);
        }
        return counts;
    }

    private Set<String> liveServingNodeIds(List<NodeInfo> liveNodes) {
        return liveNodes.stream()
                .filter(node -> "SERVING".equalsIgnoreCase(node.replicaState))
                .map(node -> node.nodeId)
                .collect(Collectors.toSet());
    }

    public void updateCommitIndex(int shardId, long commitIndex) {
        ShardMetadata metadata = shard(shardId);
        if (commitIndex > metadata.commitIndex) {
            metadata.commitIndex = commitIndex;
            updateShard(metadata);
        }
    }

    public Set<Integer> shardsForNode(String nodeId) {
        return shards().stream()
                .filter(shard -> shard.replicas.contains(nodeId))
                .map(shard -> shard.shardId)
                .collect(java.util.stream.Collectors.toSet());
    }

    private void ensurePersistent(String path) throws KeeperException, InterruptedException {
        if (zk.exists(path, false) == null) {
            try {
                zk.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException ignored) {
            }
        }
    }

    private void upsertPersistent(String path, byte[] data) {
        try {
            Stat stat = zk.exists(path, false);
            if (stat == null) {
                zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                zk.setData(path, data, stat.getVersion());
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to write " + path, e);
        }
    }

    private void upsertPersistentIfAbsent(String path, byte[] data) {
        try {
            if (zk.exists(path, false) == null) {
                zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException.NodeExistsException ignored) {
        } catch (Exception e) {
            throw new IllegalStateException("Failed to write " + path, e);
        }
    }

    private void upsertEphemeral(String path, byte[] data) {
        try {
            Stat stat = zk.exists(path, false);
            if (stat == null) {
                zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } else {
                zk.setData(path, data, stat.getVersion());
            }
        } catch (KeeperException.NodeExistsException e) {
            try {
                zk.delete(path, -1);
                zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (Exception nested) {
                throw new IllegalStateException("Failed to replace ephemeral node " + path, nested);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to write " + path, e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            zk.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }
}
