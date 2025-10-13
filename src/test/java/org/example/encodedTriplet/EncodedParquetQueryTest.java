package org.example.encodedTriplet;

import org.example.dictionary.DictionaryEncoder;
import org.example.dictionary.DictionaryEntry;
import org.example.dictionary.DictionaryParquetWriter;
import org.example.util.DataPaths;
import org.junit.jupiter.api.*;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.MessageType;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;

import java.nio.file.Files;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class EncodedParquetQueryTest {

    private java.nio.file.Path tmpDir;
    private DictionaryEncoder dict;

    @BeforeEach
    void setUp() throws Exception {
        tmpDir = Files.createTempDirectory("rdfparquet-test-");
        System.setProperty("rdfparquet.dataDir", tmpDir.toString());
        Files.createDirectories(DataPaths.parquetDir()); // ./data/parquet under tmp

        // Build a tiny dictionary
        dict = DictionaryEncoder.getInstance();
        dict.init(16);
        int sA = dict.encode("http://ex/sA");
        int sB = dict.encode("http://ex/sB");
        int pP = dict.encode("http://ex/p");
        int o1 = dict.encode("http://ex/o1");
        int o2 = dict.encode("http://ex/o2");

        // Write dictionary.parquet
        MessageType dictSchema = MessageTypeParser.parseMessageType(
                "message DictionaryEntry { required int32 id; required binary value (UTF8); }"
        );
        try (ParquetWriter<DictionaryEntry> w =
                     DictionaryParquetWriter.create(
                             new Path(DataPaths.dictPath().toString()),
                             dictSchema, CompressionCodecName.ZSTD)) {
            for (DictionaryEntry e : dict.getEntries()) w.write(e);
        }

        // SPO
        writeIndex("spo.parquet", List.of(
                et(sA, pP, o1),
                et(sA, pP, o2),
                et(sB, pP, o2)
        ));
        // SOP
        writeIndex("sop.parquet", List.of(
                et(sA, pP, o1),
                et(sA, pP, o2)
        ));
        // PSO
        writeIndex("pso.parquet", List.of(
                et(sA, pP, o1),
                et(sA, pP, o2),
                et(sB, pP, o2)
        ));
        // POS
        writeIndex("pos.parquet", List.of(
                et(sA, pP, o1),
                et(sA, pP, o2),
                et(sB, pP, o2)
        ));
        // OSP
        writeIndex("osp.parquet", List.of(
                et(sA, pP, o1),
                et(sB, pP, o2)
        ));
        // OPS
        writeIndex("ops.parquet", List.of(
                et(sA, pP, o1),
                et(sA, pP, o2),
                et(sB, pP, o2)
        ));
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

    private static EncodedTriplet et(int s, int p, int o) {
        return new EncodedTriplet(s, p, o);
    }

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
    void querySpoIds_exact() throws Exception {
        EncodedParquetQuery q = new EncodedParquetQuery();
        var tp = new org.example.SparqlParser.TriplePattern(
                "http://ex/sA", "http://ex/p", "http://ex/o2");
        var out = q.querySPOIds(tp);
        assertEquals(1, out.size());
        assertEquals(dict.encode("http://ex/o2"), out.get(0).getObject());
    }

    @Test
    void queryPoIds() throws Exception {
        EncodedParquetQuery q = new EncodedParquetQuery();
        var tp = new org.example.SparqlParser.TriplePattern(
                "?s", "http://ex/p", "http://ex/o2");
        var out = q.queryPOIds(tp);
        assertTrue(out.size() >= 2);
    }

    @Test
    void querySubjectOnlyIds() throws Exception {
        EncodedParquetQuery q = new EncodedParquetQuery();
        var tp = new org.example.SparqlParser.TriplePattern(
                "http://ex/sA", "http://ex/p", "?o");
        var out = q.querySubjectOnlyIds(tp);
        assertTrue(out.size() >= 2);
        int sA = dict.encode("http://ex/sA");
        assertTrue(out.stream().allMatch(r -> r.getSubject() == sA));
    }
}
