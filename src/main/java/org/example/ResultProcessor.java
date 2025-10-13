package org.example;

import org.apache.jena.graph.Node;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.NodeFactory;
import org.example.dictionary.DictionaryEncoder;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

public class ResultProcessor {
    private final DictionaryEncoder dict;
    private final Map<String, Integer> slotOf;
    private final List<String> projectVars;

    // Shared caches and executors
    private static final Map<String, Node> literalCache     = new ConcurrentHashMap<>(1000);
    private static final TypeMapper       typeMapper        = TypeMapper.getInstance();
    private static final ThreadLocal<StringBuilder> csvBuilder =
            ThreadLocal.withInitial(() -> new StringBuilder(8192));
    private static final ExecutorService  PAGE_EXECUTOR     = ForkJoinPool.commonPool();

    private static final int DECODE_CACHE_CAP =
            Integer.getInteger("rdfparquet.decodeCache", 100_000);

    public ResultProcessor(DictionaryEncoder dict,
                           Map<String, Integer> slotOf,
                           List<String> projectVars) {
        this.dict         = dict;
        this.slotOf       = slotOf;
        this.projectVars  = projectVars;
    }

    public List<List<String>> generatePage(List<int[]> rows, int page, int pageSize) {
        int start = page * pageSize;
        int end   = Math.min(start + pageSize, rows.size());
        if (start >= rows.size()) return Collections.emptyList();

        if (end - start > 100) {
            return generatePageParallel(rows, start, end);
        } else {
            return generatePageSequential(rows, start, end);
        }
    }

    private List<List<String>> generatePageSequential(List<int[]> rows, int start, int end) {
        List<List<String>> pageRows = new ArrayList<>(end - start);
        Map<Integer, String> decodeCache = newDecodeCache();

        for (int i = start; i < end; i++) {
            int[] row = rows.get(i);
            List<String> out = new ArrayList<>(projectVars.size());
            for (String v : projectVars) {
                int id = row[slotOf.get(v)];
                String raw = id >= 0 ? decodeCache.computeIfAbsent(id, dict::decode) : "";
                out.add(cleanLiteralValue(raw));
            }
            pageRows.add(out);
        }
        return pageRows;
    }

    private List<List<String>> generatePageParallel(List<int[]> rows, int start, int end) {
        int cpus      = Math.min(Runtime.getRuntime().availableProcessors(), 4);
        int chunkSize = (end - start + cpus - 1) / cpus;
        List<CompletableFuture<List<List<String>>>> futures = new ArrayList<>();

        for (int i = 0; i < cpus; i++) {
            int s = start + i * chunkSize;
            int e = Math.min(s + chunkSize, end);
            if (s >= end) break;
            futures.add(CompletableFuture.supplyAsync(
                    () -> generatePageSequential(rows, s, e),
                    PAGE_EXECUTOR
            ));
        }

        List<List<String>> result = new ArrayList<>();
        for (var f : futures) {
            try {
                result.addAll(f.get());
            } catch (Exception ex) {
                // on error, fallback to sequential
                return generatePageSequential(rows, start, end);
            }
        }
        return result;
    }

    public String generateCsv(List<int[]> rows, List<String> headers) {
        StringBuilder csv = csvBuilder.get();
        csv.setLength(0);

        // header row
        for (int i = 0; i < headers.size(); i++) {
            if (i > 0) csv.append(',');
            csv.append(headers.get(i));
        }
        csv.append('\n');

        // choose sequential or parallel
        if (rows.size() > 10_000) {
            return generateCsvParallel(rows, headers, csv);
        } else {
            return generateCsvSequential(rows, csv);
        }
    }

    private String generateCsvSequential(List<int[]> rows, StringBuilder csv) {
        Map<Integer, String> decodeCache = newDecodeCache();

        for (int[] row : rows) {
            for (int i = 0; i < projectVars.size(); i++) {
                if (i > 0) csv.append(',');
                String v = projectVars.get(i);
                int id = row[slotOf.get(v)];
                String raw = id >= 0 ? decodeCache.computeIfAbsent(id, dict::decode) : "";
                String clean = cleanLiteralValue(raw);
                appendCsvEscaped(csv, clean);
            }
            csv.append('\n');
        }
        return csv.toString();
    }

    private String generateCsvParallel(List<int[]> rows,
                                       List<String> headers,
                                       StringBuilder csv) {
        int cpus      = Math.min(Runtime.getRuntime().availableProcessors(), 4);
        int chunkSize = (rows.size() + cpus - 1) / cpus;
        List<CompletableFuture<String>> futures = new ArrayList<>();

        for (int i = 0; i < cpus; i++) {
            int s = i * chunkSize;
            int e = Math.min(s + chunkSize, rows.size());
            if (s >= rows.size()) break;
            futures.add(CompletableFuture.supplyAsync(() -> {
                StringBuilder chunkCsv = new StringBuilder(64 * 1024);
                Map<Integer, String> decodeCache = newDecodeCache();
                for (int j = s; j < e; j++) {
                    int[] row = rows.get(j);
                    for (int k = 0; k < projectVars.size(); k++) {
                        if (k > 0) chunkCsv.append(',');
                        String v = projectVars.get(k);
                        int id = row[slotOf.get(v)];
                        String raw = id >= 0 ? decodeCache.computeIfAbsent(id, dict::decode) : "";
                        String clean = cleanLiteralValue(raw);
                        if (needsCsvEscaping(clean)) {
                            chunkCsv.append('"').append(clean.replace("\"", "\"\"")).append('"');
                        } else {
                            chunkCsv.append(clean);
                        }
                    }
                    chunkCsv.append('\n');
                }
                return chunkCsv.toString();
            }, PAGE_EXECUTOR));
        }

        try {
            for (var f : futures) {
                csv.append(f.get());
            }
        } catch (Exception ex) {
            // fallback
            return generateCsvSequential(rows, new StringBuilder(csv.toString()));
        }
        return csv.toString();
    }

    public Path generateCsvToTempFile(List<int[]> rows, List<String> headers) throws IOException {
        Path tmp = Files.createTempFile("rdfparquet-results-", ".csv");

        try (BufferedWriter w = new BufferedWriter(
                new OutputStreamWriter(new BufferedOutputStream(Files.newOutputStream(tmp), 1 << 20),
                        StandardCharsets.UTF_8))) {

            // header
            for (int i = 0; i < headers.size(); i++) {
                if (i > 0) w.write(',');
                w.write(headers.get(i));
            }
            w.write('\n');

            // sequential streaming
            Map<Integer, String> decodeCache = newDecodeCache();
            StringBuilder cell = new StringBuilder(256);

            for (int[] row : rows) {
                for (int i = 0; i < projectVars.size(); i++) {
                    if (i > 0) w.write(',');
                    String v = projectVars.get(i);
                    int id = row[slotOf.get(v)];
                    String raw = id >= 0 ? decodeCache.computeIfAbsent(id, dict::decode) : "";
                    String clean = cleanLiteralValue(raw);

                    if (needsCsvEscaping(clean)) {
                        w.write('"');

                        for (int k = 0, n = clean.length(); k < n; k++) {
                            char ch = clean.charAt(k);
                            if (ch == '"') w.write("\"\"");
                            else w.write(ch);
                        }
                        w.write('"');
                    } else {
                        w.write(clean);
                    }
                }
                w.write('\n');
            }
        }

        return tmp;
    }


    private static void appendCsvEscaped(StringBuilder sb, String s) {
        if (!needsCsvEscaping(s)) {
            sb.append(s);
            return;
        }
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (ch == '"') sb.append("\"\"");
            else sb.append(ch);
        }
        sb.append('"');
    }

    private String cleanLiteralValue(String raw) {
        if (raw.startsWith("\"")) {
            Node node = parseLiteralCached(raw);
            return node != null ? node.getLiteralLexicalForm() : raw;
        }
        return raw;
    }

    private static boolean needsCsvEscaping(String s) {
        return s.indexOf('"') >= 0 || s.indexOf(',') >= 0 || s.indexOf('\n') >= 0 || s.indexOf('\r') >= 0;
    }

    private Node parseLiteralCached(String raw) {
        return literalCache.computeIfAbsent(raw, this::parseLiteral);
    }

    private Node parseLiteral(String raw) {
        try {
            if (!raw.startsWith("\"")) return null;
            int lastQuote = raw.lastIndexOf('"');
            if (lastQuote <= 0) return null;
            String lex = raw.substring(1, lastQuote);
            String rest = raw.substring(lastQuote + 1);
            if (rest.startsWith("@")) {
                return NodeFactory.createLiteral(lex, rest.substring(1));
            } else if (rest.startsWith("^^")) {
                String dt = raw.substring(lastQuote + 3);
                return NodeFactory.createLiteral(lex, null, typeMapper.getTypeByName(dt));
            } else {
                return NodeFactory.createLiteral(lex);
            }
        } catch (Exception e) {
            return null;
        }
    }

    private static Map<Integer, String> newDecodeCache() {
        if (DECODE_CACHE_CAP <= 0) return new HashMap<>();
        return new LinkedHashMap<Integer, String>(DECODE_CACHE_CAP, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, String> eldest) {
                return size() > DECODE_CACHE_CAP;
            }
        };
    }
}
