package edu.minisql.distributed;

import edu.minisql.distributed.config.ClusterConfig;
import edu.minisql.distributed.coordinator.CoordinatorCli;
import edu.minisql.distributed.coordinator.CoordinatorServer;
import edu.minisql.distributed.datanode.DataNodeCli;
import edu.minisql.distributed.datanode.DataNodeServer;
import edu.minisql.distributed.zk.ZkMetadataStore;

import java.nio.file.Path;

public class DisMiniSql {
    @SuppressWarnings("resource")
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            usage();
            return;
        }
        String role = args[0];
        ClusterConfig config = ClusterConfig.load(Path.of(args[1]));
        switch (role) {
            case "coordinator":
                new CoordinatorServer(config).start();
                break;
            case "client":
                new CoordinatorCli(config).run();
                break;
            case "datanode":
                if (args.length < 3) {
                    throw new IllegalArgumentException("datanode requires nodeId");
                }
                DataNodeServer dataNode = new DataNodeServer(config, args[2]);
                dataNode.start();
                if (args.length >= 4 && "--cli".equals(args[3])) {
                    new DataNodeCli(dataNode).run();
                }
                break;
            case "init-zk":
                try (ZkMetadataStore store = new ZkMetadataStore(config)) {
                    store.initializeShards(config);
                    System.out.println("ZooKeeper metadata initialized.");
                }
                break;
            default:
                usage();
        }
    }

    private static void usage() {
        System.out.println("Usage:\n"
                + "  java -jar target/dis-minisql-1.0.0.jar init-zk <config.json>\n"
                + "  java -jar target/dis-minisql-1.0.0.jar coordinator <config.json>\n"
                + "  java -jar target/dis-minisql-1.0.0.jar client <config.json>\n"
                + "  java -jar target/dis-minisql-1.0.0.jar datanode <config.json> <nodeId> [--cli]\n");
    }
}
