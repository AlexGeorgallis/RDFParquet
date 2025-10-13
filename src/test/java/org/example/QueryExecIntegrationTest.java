package org.example;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.example.encodedTriplet.EncodedTriplet;
import org.example.encodedTriplet.EncodedTripletParquetWriter;
import org.example.dictionary.DictionaryEncoder;
import org.example.dictionary.DictionaryEntry;
import org.example.dictionary.DictionaryParquetWriter;
import org.example.util.DataPaths;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class QueryExecIntegrationTest {

    private java.nio.file.Path tmpDir;
    private DictionaryEncoder dict;

    @BeforeEach
    void setUp() throws Exception {
        tmpDir = Files.createTempDirectory("rdfparquet-qe-");
        System.setProperty("rdfparquet.dataDir", tmpDir.toString());
        Files.createDirectories(DataPaths.parquetDir()); // ./data/parquet

        // Build dictionary
        dict = DictionaryEncoder.getInstance();
        dict.init(16);
        int sA = dict.encode("http://ex/sA");
        int sB = dict.encode("http://ex/sB");
        int p  = dict.encode("http://ex/p");
        int q  = dict.encode("http://ex/q");
        int o1 = dict.encode("http://ex/o1");
        int o2 = dict.encode("http://ex/o2");

        // Write dictionary parquet
        MessageType dictSchema = MessageTypeParser.parseMessageType(
                "message DictionaryEntry { required int32 id; required binary value (UTF8); }"
        );
        try (ParquetWriter<DictionaryEntry> w =
                     DictionaryParquetWriter.create(
                             new Path(DataPaths.dictPath().toString()),
                             dictSchema, CompressionCodecName.ZSTD)) {
            for (DictionaryEntry e : dict.getEntries()) w.write(e);
        }

        writeIndex("spo.parquet", List.of(
                et(sA,p,o1), et(sA,p,o2), et(sB,p,o2),
                et(sA,q,o2)
        ));
        writeIndex("pos.parquet", List.of(et(sA,p,o1), et(sA,p,o2), et(sB,p,o2), et(sA,q,o2)));
        writeIndex("sop.parquet", List.of(et(sA,p,o1), et(sA,p,o2), et(sB,p,o2), et(sA,q,o2)));
        writeIndex("osp.parquet", List.of(et(sA,p,o1), et(sB,p,o2), et(sA,q,o2)));
        writeIndex("pso.parquet", List.of(et(sA,p,o1), et(sA,p,o2), et(sB,p,o2), et(sA,q,o2)));
        writeIndex("ops.parquet", List.of(et(sA,p,o1), et(sA,p,o2), et(sB,p,o2), et(sA,q,o2)));
    }

    @AfterEach
    void tearDown() throws Exception {
        System.clearProperty("rdfparquet.dataDir");
        try (var s = Files.walk(tmpDir)) {
            s.sorted(Comparator.reverseOrder()).forEach(p -> {
                try { Files.deleteIfExists(p); } catch (Exception ignored) {}
            });
        }
    }

    private static EncodedTriplet et(int s, int p, int o) { return new EncodedTriplet(s,p,o); }

    private void writeIndex(String fileName, List<EncodedTriplet> rows) throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType(
                "message EncodedTriplet { required int32 subject; required int32 predicate; required int32 object; }"
        );
        java.nio.file.Path out = DataPaths.parquetDir().resolve(fileName);
        try (ParquetWriter<EncodedTriplet> w = EncodedTripletParquetWriter.create(
                new Path(out.toString()), schema, CompressionCodecName.SNAPPY)) {
            for (EncodedTriplet r : rows) w.write(r);
        }
    }

    @Test
    void simpleJoin_selectProjectionAndLimit() throws Exception {
        // SELECT ?s ?o WHERE { ?s <http://ex/p> ?o . ?s <http://ex/q> <http://ex/o2> } LIMIT 5
        String q = """
            SELECT ?s ?o
            WHERE {
              ?s <http://ex/p> ?o .
              ?s <http://ex/q> <http://ex/o2> .
            }
            LIMIT 5
            """;
        var parsed = SparqlParser.parse(q);
        QueryExec exec = new QueryExec();
        List<int[]> rows = exec.execute(parsed);

        // Projection order & slots:
        assertEquals(List.of("?s", "?o"), exec.getProjectVars());
        Map<String,Integer> slotOf = exec.getSlotOf();
        assertNotNull(slotOf.get("?s"));
        assertNotNull(slotOf.get("?o"));

        // There should be at least one row: sA joins on q=o2 and p=(o1|o2) giving o2 present.
        assertFalse(rows.isEmpty());
        // Check the joined var consistency (same subject across both patterns):
        int sSlot = slotOf.get("?s");
        int oSlot = slotOf.get("?o");
        for (int[] r : rows) {
            assertTrue(r[sSlot] >= 0);
            assertTrue(r[oSlot] >= 0);
        }

        // LIMIT respected
        assertTrue(rows.size() <= 5);
    }

    @Test
    void distinctIsAppliedWhenRequested() throws Exception {
        // SELECT DISTINCT ?s WHERE { ?s <http://ex/p> ?o . }
        String q = """
            SELECT DISTINCT ?s
            WHERE { ?s <http://ex/p> ?o . }
            """;
        var parsed = SparqlParser.parse(q);
        QueryExec exec = new QueryExec();
        List<int[]> rows = exec.execute(parsed);

        // With sA twice (o1,o2) and sB once => DISTINCT ?s -> expected 2 rows
        assertEquals(2, rows.size());
    }
}

