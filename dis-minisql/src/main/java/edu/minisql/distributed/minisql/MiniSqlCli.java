package edu.minisql.distributed.minisql;

import edu.minisql.distributed.common.SqlUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * One-shot batch executor. Production DataNode path uses {@link MiniSqlEngine}.
 */
public class MiniSqlCli {
    private final Path binary;
    private final Path workDir;
    private final Duration timeout;
    private final String defaultDatabase;

    public MiniSqlCli(Path binary, Path workDir, Duration timeout, String defaultDatabase) {
        this.binary = binary.toAbsolutePath().normalize();
        this.workDir = workDir.toAbsolutePath().normalize();
        this.timeout = timeout;
        this.defaultDatabase = defaultDatabase;
    }

    public synchronized String execute(List<String> replaySql, String sql) {
        try {
            Files.createDirectories(workDir);
            Path execDir = Files.createTempDirectory(workDir, "minisql-exec-");
            try {
                return runBatch(execDir, replaySql, sql);
            } finally {
                deleteDirectory(execDir);
            }
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("Failed to start MiniSQL binary: " + binary, e);
        }
    }

    public synchronized String executeInDirectory(Path execDir, List<String> replaySql, String sql) {
        try {
            Files.createDirectories(execDir);
            return runBatch(execDir, replaySql, sql);
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("Failed to start MiniSQL binary: " + binary, e);
        }
    }

    private String runBatch(Path execDir, List<String> replaySql, String sql) throws IOException, InterruptedException {
        List<String> statements = new ArrayList<>();
        for (String replay : replaySql) {
            maybeUseDefaultDatabase(statements, replay);
            statements.add(SqlUtils.normalize(replay));
        }
        if (sql != null && !sql.isBlank()) {
            maybeUseDefaultDatabase(statements, sql);
            statements.add(SqlUtils.normalize(sql));
        }
        if (statements.isEmpty()) {
            return "";
        }
        Path batchFile = Files.createTempFile(execDir, "minisql-batch-", ".sql");
        Files.write(batchFile, statements, StandardCharsets.UTF_8);

        Process process = new ProcessBuilder(binary.toString(), "--batch", batchFile.toString())
                .directory(execDir.toFile())
                .redirectErrorStream(true)
                .start();
        boolean finished = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (!finished) {
            process.destroyForcibly();
            throw new IllegalStateException("MiniSQL execution timed out after " + timeout);
        }
        String output = readAll(process.getInputStream());
        if (process.exitValue() != 0) {
            throw new IllegalStateException("MiniSQL exited with code " + process.exitValue() + "\n" + output);
        }
        return output;
    }

    private String readAll(InputStream in) throws IOException {
        return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }

    private void deleteDirectory(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        for (Path p : Files.walk(path).sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
            Files.deleteIfExists(p);
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
        if ("use".equals(keyword) || "quit".equals(keyword)) {
            return;
        }
        statements.add("use " + defaultDatabase + ";");
    }
}
