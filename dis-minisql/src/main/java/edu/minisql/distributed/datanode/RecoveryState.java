package edu.minisql.distributed.datanode;

import java.util.ArrayList;
import java.util.List;

public class RecoveryState {
    public SnapshotManifest snapshotManifest;
    public List<String> snapshotSql = new ArrayList<>();
    public List<WalEntry> walEntries = new ArrayList<>();

    public RecoveryState() {
    }

    public RecoveryState(SnapshotManifest snapshotManifest, List<String> snapshotSql, List<WalEntry> walEntries) {
        this.snapshotManifest = snapshotManifest;
        this.snapshotSql = snapshotSql;
        this.walEntries = walEntries;
    }
}
