package edu.minisql.distributed.minisql;

import edu.minisql.distributed.common.SqlUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Long-running MiniSQL --repl process for one DataNode work directory.
 */
public class MiniSqlEngine {
    static final String END_MARKER = "__MINISQL_END__";

    private final Path binary;
    private final Path workDir;
    private final Duration timeout;
    private final String defaultDatabase;
    private final Path snapshotDatabasesDir;

    private Process process;
    private BufferedWriter stdin;
    private BufferedReader stdout;
    private long engineWalSequence = -1;
    private long engineCompactedWal = -1;
    private boolean invalidated = true;

    public MiniSqlEngine(Path binary, Path workDir, Path snapshotDatabasesDir, Duration timeout, String defaultDatabase) {
        this.binary = binary.toAbsolutePath().normalize();
        this.workDir = workDir.toAbsolutePath().normalize();
        this.snapshotDatabasesDir = snapshotDatabasesDir == null ? null : snapshotDatabasesDir.normalize();
        this.timeout = timeout;
        this.defaultDatabase = defaultDatabase;
    }

    public synchronized void resetAndReplay(List<String> replaySql, long walSequence, long compactedWal) {
        destroyProcess();
        try {
            prepareWorkDir();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to prepare MiniSQL work directory: " + workDir, e);
        }
        startProcess();
        for (String statement : expandStatements(replaySql, null)) {
            runStatement(statement);
        }
        engineWalSequence = walSequence;
        engineCompactedWal = compactedWal;
        invalidated = false;
    }

    public synchronized String executeStatement(String sql) {
        ensureAlive();
        return runStatement(SqlUtils.normalize(sql));
    }

    public synchronized void invalidate() {
        invalidated = true;
        destroyProcess();
    }

    public boolean isReady() {
        return !invalidated && process != null && process.isAlive();
    }

    public long engineWalSequence() {
        return engineWalSequence;
    }

    public long engineCompactedWal() {
        return engineCompactedWal;
    }

    public void setEngineWalSequence(long engineWalSequence) {
        this.engineWalSequence = engineWalSequence;
    }

    private void ensureAlive() {
        if (invalidated || process == null || !process.isAlive()) {
            throw new IllegalStateException("MiniSQL engine is not running; rebuild required");
        }
    }

    private List<String> expandStatements(List<String> replaySql, String sql) {
        List<String> statements = new ArrayList<>();
        if (replaySql != null) {
            for (String replay : replaySql) {
                maybeUseDefaultDatabase(statements, replay);
                statements.add(SqlUtils.normalize(replay));
            }
        }
        if (sql != null && !sql.isBlank()) {
            maybeUseDefaultDatabase(statements, sql);
            statements.add(SqlUtils.normalize(sql));
        }
        return statements;
    }

    private void prepareWorkDir() throws IOException {
        if (Files.exists(workDir)) {
            deleteDirectory(workDir);
        }
        Files.createDirectories(workDir);
        if (snapshotDatabasesDir != null && Files.exists(snapshotDatabasesDir)) {
            copyDirectory(snapshotDatabasesDir, workDir.resolve("databases"));
        }
    }

    private void startProcess() {
        try {
            ProcessBuilder builder = new ProcessBuilder(binary.toString(), "--repl")
                    .directory(workDir.toFile())
                    .redirectErrorStream(true);
            process = builder.start();
            stdin = new BufferedWriter(new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
            stdout = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start MiniSQL REPL: " + binary, e);
        }
    }

    private String runStatement(String sql) {
        try {
            stdin.write(sql);
            if (!sql.endsWith("\n")) {
                stdin.write('\n');
            }
            stdin.flush();
            return readUntilMarker();
        } catch (IOException e) {
            invalidated = true;
            destroyProcess();
            throw new IllegalStateException("MiniSQL REPL communication failed", e);
        }
    }

    private String readUntilMarker() throws IOException {
        StringBuilder output = new StringBuilder();
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            if (System.nanoTime() > deadline) {
                destroyProcess();
                throw new IllegalStateException("MiniSQL REPL timed out after " + timeout);
            }
            if (!stdout.ready()) {
                if (!process.isAlive()) {
                    int code = process.exitValue();
                    invalidated = true;
                    throw new IllegalStateException("MiniSQL process exited with code " + code + "\n" + output);
                }
                sleepBriefly();
                continue;
            }
            String line = stdout.readLine();
            if (line == null) {
                if (!process.isAlive()) {
                    invalidated = true;
                    throw new IllegalStateException("MiniSQL process closed stdout\n" + output);
                }
                sleepBriefly();
                continue;
            }
            if (END_MARKER.equals(line)) {
                break;
            }
            if (output.length() > 0) {
                output.append('\n');
            }
            output.append(line);
        }
        return output.toString();
    }

    private void sleepBriefly() {
        try {
            TimeUnit.MILLISECONDS.sleep(5);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("MiniSQL REPL read interrupted", e);
        }
    }

    private void destroyProcess() {
        if (process == null) {
            return;
        }
        try {
            if (process.isAlive()) {
                try {
                    if (stdin != null) {
                        stdin.write("exit;\n");
                        stdin.flush();
                    }
                } catch (IOException ignored) {
                }
                if (!process.waitFor(500, TimeUnit.MILLISECONDS)) {
                    process.destroyForcibly();
                    process.waitFor(2, TimeUnit.SECONDS);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        } finally {
            process = null;
            stdin = null;
            stdout = null;
        }
    }

    private void deleteDirectory(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (var paths = Files.walk(path)) {
            for (Path entry : paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
                Files.deleteIfExists(entry);
            }
        }
    }

    private void copyDirectory(Path from, Path to) throws IOException {
        Files.createDirectories(to);
        try (var paths = Files.walk(from)) {
            for (Path source : paths.collect(Collectors.toList())) {
                Path target = to.resolve(from.relativize(source));
                if (Files.isDirectory(source)) {
                    Files.createDirectories(target);
                } else {
                    Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }

    private void maybeUseDefaultDatabase(List<String> statements, String sql) {
        if (defaultDatabase == null || defaultDatabase.isBlank() || sql == null) {
            return;
        }
        String keyword = SqlUtils.firstKeyword(sql);
        if ("create".equals(keyword) && sql.toLowerCase().contains("database")) {
            return;
        }
        if ("drop".equals(keyword) && sql.toLowerCase().contains("database")) {
            return;
        }
        if ("use".equals(keyword) || "quit".equals(keyword) || "exit".equals(keyword)) {
            return;
        }
        statements.add("use " + defaultDatabase + ";");
    }
}
