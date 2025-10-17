package org.example.dictionary;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;

import org.example.QueryExec;
import org.example.SparqlParser;
import org.example.SparqlParser.ParsedQuery;
import org.example.ResultProcessor;
import org.example.util.DataPaths;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

public class MainQueryApp {
    private static volatile HttpServer SERVER_REF = null;
    private static final ExecutorService HTTP_EXECUTOR =
            java.util.concurrent.Executors.newCachedThreadPool();

    // Last query state (used for paging + download)
    private static volatile Path lastCsvFile = null;          // temp csv file if generated
    private static volatile String lastCsvInMemory = "";      // fallback if streaming not available
    private static volatile int lastRowCount = 0;
    private static volatile double lastQueryTime = 0.0;
    private static volatile List<int[]> lastResults = null;
    private static volatile List<String> lastHeaders = null;
    private static volatile List<String> lastProjectVars = null;
    private static volatile Map<String, Integer> lastSlotOf = null;
    private static volatile DictionaryEncoder lastDict = null;

    private static final ExecutorService executor = ForkJoinPool.commonPool();
    private static final int PAGE_SIZE = Integer.getInteger("rdfparquet.pageSize", 1000);

    public static void main(String[] args) throws Exception {
        if (args.length > 1) {
            System.err.println("Usage: java -jar target/dictionary-server.jar [<port>]");
            System.exit(1);
        }
        int port = (args.length == 1) ? Integer.parseInt(args[0]) : 8080;
        String host = System.getProperty("host");
        if (host == null || host.isBlank()) host = "127.0.0.1";

        // Ensures data dir exists and dictionary is present
        DataPaths.ensureParquetDir();
        DataPaths.requireExists(DataPaths.dictPath(), "Dictionary");

        try {
            System.out.println("Loading dictionary from: " + DataPaths.dictPath());
            DictionaryLoader.load(DataPaths.dictPath().toString());
            int size = DictionaryEncoder.getInstance().getEncodeMap().size();
            System.out.println("Dictionary loaded. Total entries: " + size);
            printMemoryUsage();
        } catch (Exception e) {
            System.err.println("Warning: Could not load dictionary: " + e.getMessage());
        }

        InetSocketAddress addr = new InetSocketAddress(host, port);
        HttpServer server = HttpServer.create(addr, 0);
        SERVER_REF = server;

        server.createContext("/", MainQueryApp::handleRoot);
        server.createContext("/query", MainQueryApp::handleQuery);
        server.createContext("/download", MainQueryApp::handleDownload);
        server.createContext("/page", MainQueryApp::handlePage);
        server.createContext("/status", MainQueryApp::handleStatus);
        server.createContext("/shutdown", MainQueryApp::handleShutdown);

        System.out.printf("[Server] host=%s port=%d dataDir=%s%n", host, port, DataPaths.baseDir());
        System.out.printf("Listening → http://%s:%d/%n", host, port);

        server.setExecutor(HTTP_EXECUTOR);
        server.start();
    }

    private static void handleStatus(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendText(exchange, 405, "Method Not Allowed");
            return;
        }
        String json = "{\"status\":\"up\"}";
        exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
        byte[] b = json.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, b.length);
        try (OutputStream os = exchange.getResponseBody()) { os.write(b); }
    }

    private static void handleShutdown(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendText(exchange, 405, "Method Not Allowed");
            return;
        }
        String remoteHost = exchange.getRemoteAddress().getAddress().getHostAddress();
        if (!remoteHost.equals("127.0.0.1") && !remoteHost.equals("::1")) {
            sendText(exchange, 403, "Forbidden");
            return;
        }

        String msg = "Shutting down…";
        exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=utf-8");
        byte[] b = msg.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, b.length);
        try (OutputStream os = exchange.getResponseBody()) { os.write(b); }

        new Thread(() -> {
            try { Thread.sleep(200); } catch (InterruptedException ignored) {}
            try {
                if (SERVER_REF != null) {
                    System.out.println("[Server] Stopping…");
                    SERVER_REF.stop(0);
                }
            } catch (Throwable t) {
                t.printStackTrace();
            } finally {
                HTTP_EXECUTOR.shutdownNow();
                executor.shutdownNow();
                System.out.println("[Server] Stopped.");
                System.exit(0);
            }
        }, "server-shutdown").start();
    }

    private static void handleRoot(HttpExchange exchange) throws IOException {
        try {
            String html = htmlPage(getFormHtml("", null, null, null, 0, 0.0, false, 0, 0));
            send200(exchange, html, "text/html");
        } catch (Exception e) {
            sendError(exchange, "Internal server error: " + e.getMessage());
        }
    }

    private static void handleQuery(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            handleRoot(exchange);
            return;
        }

        String form = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        Map<String, String> params = parseForm(form);
        String query = params.getOrDefault("query", "").trim();

        List<List<String>> pageRows = Collections.emptyList();
        List<String> headers = null;
        String error = null;
        int rowCount = 0;
        long t1 = System.nanoTime();

        if (!query.isEmpty()) {
            try {
                DictionaryEncoder dict = DictionaryEncoder.getInstance();
                ParsedQuery parsed = SparqlParser.parse(query);

                // Run engine
                QueryExec exec = new QueryExec();
                List<int[]> rows = exec.execute(parsed);

                List<String> projectVars = exec.getProjectVars();
                headers = new ArrayList<>(projectVars.size());
                for (String v : projectVars) {
                    headers.add(v.startsWith("?") ? v.substring(1) : v);
                }
                Map<String, Integer> slotOf = exec.getSlotOf();

                // Store state for pagination & download
                lastResults     = rows;
                lastHeaders     = headers;
                lastProjectVars = projectVars;
                lastSlotOf      = slotOf;
                lastDict        = dict;
                lastRowCount    = rows.size();

                // First page render
                ResultProcessor processor = new ResultProcessor(dict, slotOf, projectVars);
                pageRows = processor.generatePage(rows, 0, PAGE_SIZE);
                rowCount = rows.size();

                // CSV generation (stream to temp file if available; else fallback)
                List<String> finalHeaders = headers;
                CompletableFuture<Void> csvFuture = CompletableFuture.runAsync(() -> {
                    try {
                        Method m = ResultProcessor.class.getMethod(
                                "generateCsvToTempFile", List.class, List.class);
                        Path tmp = (Path) m.invoke(processor, rows, finalHeaders);
                        lastCsvFile = tmp;
                        lastCsvInMemory = "";
                    } catch (NoSuchMethodException nsme) {
                        String csv = processor.generateCsv(rows, finalHeaders);
                        lastCsvInMemory = csv;
                        try {
                            Path tmp = Files.createTempFile("rdfparquet-results-", ".csv");
                            Files.writeString(tmp, csv, StandardCharsets.UTF_8);
                            lastCsvFile = tmp;
                        } catch (IOException ioe) {
                            lastCsvFile = null;
                        }
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                }, executor);

                try { csvFuture.get(30, TimeUnit.SECONDS); } catch (Exception ignore) {}

                printMemoryUsage();
            } catch (Exception e) {
                error = e.getMessage();
            }
        }

        long t2 = System.nanoTime();
        lastQueryTime = (t2 - t1) / 1e9;
        int totalPages = (rowCount + PAGE_SIZE - 1) / PAGE_SIZE;

        String html = htmlPage(getFormHtml(query, headers, pageRows, error, rowCount, lastQueryTime, true, 0, totalPages));
        send200(exchange, html, "text/html");
    }

    private static void handlePage(HttpExchange exchange) throws IOException {
        String query = exchange.getRequestURI().getQuery();
        Map<String, String> params = parseQuery(query);
        int page = Integer.parseInt(params.getOrDefault("page", "0"));

        if (lastResults == null || lastHeaders == null) {
            sendError(exchange, "No query results available");
            return;
        }

        ResultProcessor processor = new ResultProcessor(lastDict, lastSlotOf, lastProjectVars);
        List<List<String>> pageRows = processor.generatePage(lastResults, page, PAGE_SIZE);
        int totalPages = (lastRowCount + PAGE_SIZE - 1) / PAGE_SIZE;

        String html = htmlPage(getFormHtml("", lastHeaders, pageRows, null, lastRowCount, lastQueryTime, true, page, totalPages));
        send200(exchange, html, "text/html");
    }

    private static void handleDownload(HttpExchange exchange) throws IOException {
        if (lastCsvFile != null && Files.exists(lastCsvFile)) {
            exchange.getResponseHeaders().add("Content-Disposition", "attachment; filename=\"results.csv\"");
            exchange.getResponseHeaders().add("Content-Type", "text/csv; charset=utf-8");
            long len = Files.size(lastCsvFile);
            exchange.sendResponseHeaders(200, len);
            try (OutputStream os = exchange.getResponseBody()) {
                Files.copy(lastCsvFile, os);
            }
            return;
        }
        byte[] csv = lastCsvInMemory.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Disposition", "attachment; filename=\"results.csv\"");
        send200(exchange, csv, "text/csv");
    }

    private static String getFormHtml(String query,
                                      List<String> headers,
                                      List<List<String>> results,
                                      String error,
                                      int rowCount,
                                      double seconds,
                                      boolean enableDownload,
                                      int currentPage,
                                      int totalPages) {
        StringBuilder sb = new StringBuilder();
        sb.append("<h2 style='display:flex;align-items:center;gap:12px;'>")
                .append("RDF Parquet Query Engine")
                .append("<span id='status' style='display:inline-flex;align-items:center;gap:8px;font-size:14px;'>")
                .append("<span>Status:</span>")
                .append("<span id='status-dot' ")
                .append("style='display:inline-block;width:10px;height:10px;border-radius:50%;background:#aaa;'></span>")
                .append("<span id='status-text' style='color:#555;'>Checking…</span>")
                .append("</span>")
                .append("<form id='shutdown-form' method='post' action='/shutdown' style='margin-left:auto;'>")
                .append("<button type='button' id='shutdown-btn' ")
                .append("style='padding:6px 10px;border:1px solid #b00;background:#e74c3c;color:#fff;border-radius:6px;cursor:pointer;'>")
                .append("Shutdown")
                .append("</button>")
                .append("</form>")
                .append("</h2>")

                .append("<form method=\"post\" action=\"/query\">")
                .append("<label for=\"query\">SPARQL Query:</label><br>")
                .append("<textarea name=\"query\" style=\"width:90%;height:300px;font-family:monospace;\">")
                .append(query == null || query.isEmpty() ? defaultQuery() : escapeHtml(query))
                .append("</textarea><br>")
                .append("<button type=\"submit\">Run Query</button>")
                .append("</form>");

        if (error != null) {
            sb.append("<div class='error' style='color:red'><b>Error:</b> ")
                    .append(escapeHtml(error)).append("</div>");
        }

        if (results != null && !results.isEmpty() && headers != null) {
            sb.append("<div class='summary'><b>")
                    .append(rowCount).append("</b> results in <b>")
                    .append(String.format("%.3f", seconds)).append("</b> seconds</div>");

            if (enableDownload) {
                sb.append("<a href=\"/download\">Download Full CSV</a><br/><br>");
            }

            // pagination
            if (totalPages > 1) {
                sb.append("<div>Page ").append(currentPage + 1)
                        .append(" of ").append(totalPages).append("<br>");
                if (currentPage > 0) {
                    sb.append("<a href=\"/page?page=0\">First</a> | ")
                            .append("<a href=\"/page?page=").append(currentPage - 1).append("\">Previous</a> | ");
                }
                int start = Math.max(0, currentPage - 2);
                int end = Math.min(totalPages - 1, currentPage + 2);
                for (int i = start; i <= end; i++) {
                    if (i == currentPage) {
                        sb.append("<b>").append(i + 1).append("</b> ");
                    } else {
                        sb.append("<a href=\"/page?page=").append(i).append("\">")
                                .append(i + 1).append("</a> ");
                    }
                }
                if (currentPage < totalPages - 1) {
                    sb.append("| <a href=\"/page?page=").append(currentPage + 1).append("\">Next</a>")
                            .append(" | <a href=\"/page?page=").append(totalPages - 1).append("\">Last</a>");
                }
                sb.append("</div>");
            }

            // results table
            sb.append("<table border=\"1\" cellpadding=\"4\" style=\"border-collapse:collapse;margin-top:1em;\"><thead><tr>");
            for (String h : headers) {
                sb.append("<th>").append(escapeHtml(h)).append("</th>");
            }
            sb.append("</tr></thead><tbody>");
            for (List<String> row : results) {
                sb.append("<tr>");
                for (String cell : row) {
                    sb.append("<td>").append(escapeHtml(cell)).append("</td>");
                }
                sb.append("</tr>");
            }
            sb.append("</tbody></table>");
        }

        // Small inline script to poll /status and wire the Shutdown button
        sb.append("<script>")
                .append("async function pollStatus(){")
                .append(" try{")
                .append("  const r=await fetch('/status',{cache:'no-store'});")
                .append("  const ok=r.ok; const dot=document.getElementById('status-dot');")
                .append("  const txt=document.getElementById('status-text');")
                .append("  if(ok){ dot.style.background='#2ecc71'; txt.textContent='Online'; }")
                .append("  else { dot.style.background='#e74c3c'; txt.textContent='Offline'; }")
                .append(" }catch(e){")
                .append("  const dot=document.getElementById('status-dot');")
                .append("  const txt=document.getElementById('status-text');")
                .append("  dot.style.background='#e74c3c'; txt.textContent='Offline';")
                .append(" }")
                .append("}")
                .append("pollStatus(); setInterval(pollStatus,5000);")
                .append("document.getElementById('shutdown-btn').addEventListener('click', async () => {")
                .append(" if(!confirm('Shut down the server now?')) return;")
                .append(" try {")
                .append("   await fetch('/shutdown',{method:'POST'});")
                .append("   alert('Server is shutting down…');")
                .append("   setTimeout(()=>location.reload(),500);")
                .append(" } catch(e){ alert('Shutdown request failed: '+e); }")
                .append("});")
                .append("</script>");

        return sb.toString();
    }

    private static Map<String, String> parseForm(String form) {
        Map<String, String> map = new HashMap<>();
        if (form == null || form.isBlank()) return map;
        for (String pair : form.split("&")) {
            int idx = pair.indexOf('=');
            if (idx < 0) continue;
            try {
                String k = URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8);
                String v = URLDecoder.decode(pair.substring(idx+1), StandardCharsets.UTF_8);
                map.put(k, v);
            } catch (Exception ignored) {}
        }
        return map;
    }

    private static Map<String, String> parseQuery(String query) {
        Map<String, String> map = new HashMap<>();
        if (query == null || query.isBlank()) return map;
        for (String pair : query.split("&")) {
            int idx = pair.indexOf('=');
            if (idx < 0) continue;
            try {
                String k = URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8);
                String v = URLDecoder.decode(pair.substring(idx+1), StandardCharsets.UTF_8);
                map.put(k, v);
            } catch (Exception ignored) {}
        }
        return map;
    }

    private static String htmlPage(String content) {
        return "<!DOCTYPE html><html><head><meta charset='utf-8'><title>RDF Parquet Query Engine</title>"
                + "<style>body{font-family:Arial;margin:2em;}textarea{width:90%;height:300px;font-family:monospace;}"
                + "table{border-collapse:collapse;margin-top:2em;}th,td{border:1px solid #aaa;padding:4px;}th{background:#e0e0e0;}"
                + ".error{color:red;margin-top:1em}.summary{margin-bottom:1em;font-size:1.1em}</style></head><body>"
                + content + "</body></html>";
    }

    private static String defaultQuery() {
        return "PREFIX schema: <http://schema.org/>\n"
                + "PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                + "PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>\n\n"
                + "SELECT ?person ?birthDate\n"
                + "WHERE {\n"
                + "  ?person rdf:type schema:Person ;\n"
                + "          rdfs:label ?label ;\n"
                + "          schema:birthDate ?birthDate .\n"
                + "}";
    }

    private static void send200(HttpExchange ex, String html, String contentType) throws IOException {
        byte[] bytes = html.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", contentType + "; charset=utf-8");
        ex.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static void send200(HttpExchange ex, byte[] data, String contentType) throws IOException {
        ex.getResponseHeaders().add("Content-Type", contentType + "; charset=utf-8");
        ex.sendResponseHeaders(200, data.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(data);
        }
    }

    private static void sendText(HttpExchange ex, int code, String txt) throws IOException {
        ex.getResponseHeaders().add("Content-Type", "text/plain; charset=utf-8");
        byte[] b = txt.getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(code, b.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(b); }
    }

    private static void sendError(HttpExchange ex, String message) throws IOException {
        String html = htmlPage("<h2>Error</h2><p style='color:red;'>" + escapeHtml(message) + "</p>");
        send200(ex, html, "text/html");
    }

    private static void printMemoryUsage() {
        Runtime rt = Runtime.getRuntime();
        long used = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
        long max  = rt.maxMemory() / (1024 * 1024);
    }

    private static String escapeHtml(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;").replace("<", "&lt;")
                .replace(">", "&gt;").replace("\"", "&quot;");
    }
}
