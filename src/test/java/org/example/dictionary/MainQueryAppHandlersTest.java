package org.example.dictionary;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.example.encodedTriplet.EncodedTriplet;
import org.example.encodedTriplet.EncodedTripletParquetWriter;
import org.example.util.DataPaths;
import org.junit.jupiter.api.*;

import java.io.*;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

class MainQueryAppHandlersTest {

    private java.nio.file.Path tmpDir;
    private HttpServer server;
    private int port;

    private Method handleRoot;
    private Method handleQuery;
    private Method handlePage;
    private Method handleDownload;

    @BeforeEach
    void setup() throws Exception {
        // temp data dir & tiny dataset
        tmpDir = Files.createTempDirectory("rdfparquet-webtest-");
        System.setProperty("rdfparquet.dataDir", tmpDir.toString());
        Files.createDirectories(DataPaths.parquetDir());

        DictionaryEncoder dict = DictionaryEncoder.getInstance();
        dict.init(16);
        int sA = dict.encode("http://ex/sA");
        int p  = dict.encode("http://ex/p");
        int o1 = dict.encode("http://ex/o1");

        MessageType dictSchema = MessageTypeParser.parseMessageType(
                "message DictionaryEntry { required int32 id; required binary value (UTF8); }"
        );
        try (ParquetWriter<DictionaryEntry> w =
                     DictionaryParquetWriter.create(
                             new Path(DataPaths.dictPath().toString()),
                             dictSchema, CompressionCodecName.ZSTD)) {
            for (DictionaryEntry e : dict.getEntries()) w.write(e);
        }
        MessageType tripSchema = MessageTypeParser.parseMessageType(
                "message EncodedTriplet { required int32 subject; required int32 predicate; required int32 object; }"
        );
        // Write all 6 index files
        for (String f : List.of("spo","sop","pso","pos","osp","ops")) {
            try (ParquetWriter<EncodedTriplet> w =
                         EncodedTripletParquetWriter.create(
                                 new Path(DataPaths.parquetDir().resolve(f + ".parquet").toString()),
                                 tripSchema, CompressionCodecName.SNAPPY)) {
                w.write(new EncodedTriplet(sA,p,o1));
            }
        }

        // reflect handlers
        handleRoot = MainQueryApp.class.getDeclaredMethod("handleRoot", com.sun.net.httpserver.HttpExchange.class);
        handleQuery = MainQueryApp.class.getDeclaredMethod("handleQuery", com.sun.net.httpserver.HttpExchange.class);
        handlePage = MainQueryApp.class.getDeclaredMethod("handlePage", com.sun.net.httpserver.HttpExchange.class);
        handleDownload = MainQueryApp.class.getDeclaredMethod("handleDownload", com.sun.net.httpserver.HttpExchange.class);
        handleRoot.setAccessible(true);
        handleQuery.setAccessible(true);
        handlePage.setAccessible(true);
        handleDownload.setAccessible(true);

        // tiny server that delegates to the private handlers
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        port = server.getAddress().getPort();
        server.createContext("/", ex -> invoke(handleRoot, ex));
        server.createContext("/query", ex -> invoke(handleQuery, ex));
        server.createContext("/page", ex -> invoke(handlePage, ex));
        server.createContext("/download", ex -> invoke(handleDownload, ex));
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
    }

    private void invoke(Method m, HttpExchange ex) throws IOException {
        try {
            m.invoke(null, ex);
        } catch (Exception e) {
            e.printStackTrace();

            byte[] bytes = ("ERR: " + e.getClass().getSimpleName() + " - " + e.getMessage()).getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(500, bytes.length);
            try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) server.stop(0);
        System.clearProperty("rdfparquet.dataDir");
        try (var s = Files.walk(tmpDir)) {
            s.sorted(Comparator.reverseOrder()).forEach(p -> {
                try { Files.deleteIfExists(p); } catch (Exception ignored) {}
            });
        }
    }

    @Test
    void rootRendersFormAndDefaultQuery() throws Exception {
        var client = HttpClient.newHttpClient();
        var res = client.send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, res.statusCode());
        assertTrue(res.body().contains("RDF Parquet Query Engine"));
        assertTrue(res.body().contains("SPARQL Query"));
    }

    @Test
    void queryEndpointRunsAndShowsSummaryAndDownload() throws Exception {
        var client = HttpClient.newHttpClient();
        String form = "query=" + java.net.URLEncoder.encode("""
            SELECT ?s ?o
            WHERE { ?s <http://ex/p> ?o . }
        """, StandardCharsets.UTF_8);

        var req = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/query"))
                .POST(HttpRequest.BodyPublishers.ofString(form))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .build();

        var res = client.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, res.statusCode());
        assertTrue(res.body().contains("results in"), "Should show summary line");
        assertTrue(res.body().contains("/download"), "Should expose download link");
        assertTrue(res.body().contains("<table"), "Should render results table");
    }

    @Test
    void downloadReturnsCsv() throws Exception {
        // first, execute a query to populate lastCsv
        queryEndpointRunsAndShowsSummaryAndDownload();

        var client = HttpClient.newHttpClient();
        var res = client.send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/download")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, res.statusCode());
        assertTrue(res.headers().firstValue("Content-Type").orElse("").contains("text/csv"));
        assertTrue(res.body().toLowerCase(Locale.ROOT).contains("s"), "header likely includes s");
    }
}

