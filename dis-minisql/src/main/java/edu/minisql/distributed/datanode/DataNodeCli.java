package edu.minisql.distributed.datanode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

public class DataNodeCli {
    private final DataNodeServer server;
    private final BufferedReader in;
    private final PrintStream out;
    private final boolean interactive;

    public DataNodeCli(DataNodeServer server) {
        this(server, System.in, System.out, System.console() != null);
    }

    public DataNodeCli(DataNodeServer server, InputStream in, PrintStream out, boolean interactive) {
        this.server = server;
        this.in = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        this.out = out;
        this.interactive = interactive;
    }

    public void run() throws IOException {
        out.println("Distributed MiniSQL DataNode Local CLI");
        out.println("Type SQL to execute on this DataNode only. Type quit or exit to leave the CLI.");
        while (true) {
            if (interactive) {
                out.print("datanode> ");
                out.flush();
            }
            String line = in.readLine();
            if (line == null) {
                out.println("EOF received, exiting DataNode CLI.");
                return;
            }
            String statement = line.trim();
            if (statement.isBlank()) {
                continue;
            }
            String command = firstToken(statement).toLowerCase(Locale.ROOT);
            if ("quit".equals(command) || "exit".equals(command)) {
                out.println("Exiting DataNode CLI. HTTP DataNode process keeps running.");
                return;
            }
            try {
                String output = server.executeLocalSql(statement);
                if (output == null || output.isBlank()) {
                    out.println("OK");
                } else {
                    out.println(output.stripTrailing());
                }
            } catch (Exception e) {
                out.println("ERROR: " + e.getMessage());
            }
        }
    }

    private String firstToken(String line) {
        int split = line.indexOf(' ');
        return split < 0 ? line : line.substring(0, split);
    }
}
