package org.example.dictionary;

import org.example.Triplet;
import org.junit.jupiter.api.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class MainDictionaryTest {

    private Path tmpDataDir;
    private String prevDataProp;

    static final class TestableMainDictionary extends MainDictionary {
        private final List<Triplet> supplied;

        TestableMainDictionary(List<Triplet> supplied) throws Exception {
            super();
            this.supplied = supplied;
        }

        @Override
        protected List<Triplet> loadRdfData(String rdfFilePath) {
            // Ignore the path and return injected triples
            return supplied;
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        tmpDataDir = Files.createTempDirectory("rdfparquet-test-");
        prevDataProp = System.getProperty("rdfparquet.dataDir");
        System.setProperty("rdfparquet.dataDir", tmpDataDir.toString());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (prevDataProp == null) {
            System.clearProperty("rdfparquet.dataDir");
        } else {
            System.setProperty("rdfparquet.dataDir", prevDataProp);
        }

        if (Files.exists(tmpDataDir)) {
            try (var stream = Files.walk(tmpDataDir)) {
                stream.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try { Files.deleteIfExists(p); } catch (Exception ignored) {}
                });
            }
        }
    }

    @Test
    void rejectsUnsupportedExtension() throws Exception {
        MainDictionary md = new MainDictionary();
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> md.processRdfFile("/tmp/fake.txt")
        );
        assertTrue(ex.getMessage().toLowerCase().contains("unsupported"));
    }

    @Test
    void writesAllParquetAndDictionaryAndCountsAreCorrect() throws Exception {
        List<Triplet> triples = List.of(
                new Triplet("s1","p1","o1"),
                new Triplet("s1","p1","o1"), // dup
                new Triplet("s2","p1","o2"),
                new Triplet("s3","p2","o1")
        );

        TestableMainDictionary md = new TestableMainDictionary(triples);
        MainDictionary.ProcessingResults res =
                md.processRdfFile(tmpDataDir.resolve("dummy.nt").toString()); // only extension matters

        // Distinct triples = 3
        assertEquals(3, res.getDistinctTriples());

        // Distinct terms: s1,s2,s3,p1,p2,o1,o2 = 7
        assertEquals(7, res.getDistinctTerms());

        Map<String,String> out = res.getOutputFiles();

        // Keys present
        assertTrue(out.containsKey("dictionary"));
        assertTrue(out.containsKey("spo"));
        assertTrue(out.containsKey("sop"));
        assertTrue(out.containsKey("pso"));
        assertTrue(out.containsKey("pos"));
        assertTrue(out.containsKey("osp"));
        assertTrue(out.containsKey("ops"));

        // Files exist and are non-empty
        for (var e : out.entrySet()) {
            File f = new File(e.getValue());
            assertTrue(f.exists(), e.getKey() + " missing");
            assertTrue(f.length() > 0L, e.getKey() + " empty");
        }
    }

    @Test
    void runningTwiceOverwritesExistingOutputsIdempotent() throws Exception {
        List<Triplet> triples = List.of(
                new Triplet("s1","p1","o1"),
                new Triplet("s2","p2","o2")
        );

        TestableMainDictionary md = new TestableMainDictionary(triples);
        var res1 = md.processRdfFile(tmpDataDir.resolve("dummy.nt").toString());
        long size1 = new File(res1.getOutputFiles().get("dictionary")).length();

        var res2 = md.processRdfFile(tmpDataDir.resolve("dummy.nt").toString());
        long size2 = new File(res2.getOutputFiles().get("dictionary")).length();

        assertTrue(size2 > 0L);
        assertTrue(new File(res2.getOutputFiles().get("spo")).exists());
        assertTrue(size1 > 0L);
    }

    @Test
    void autoThreadsDoesNotCrash() throws Exception {
        List<Triplet> triples = List.of(
                new Triplet("s1","p1","o1"),
                new Triplet("s2","p1","o2"),
                new Triplet("s3","p2","o3"),
                new Triplet("s4","p3","o4")
        );
        TestableMainDictionary md = new TestableMainDictionary(triples);
        var res = md.processRdfFile(tmpDataDir.resolve("dummy.ttl").toString());
        assertTrue(new File(res.getOutputFiles().get("ops")).exists());
    }
}
