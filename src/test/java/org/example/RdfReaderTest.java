package org.example;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RdfReaderTest {

    @TempDir
    Path temp;

    private Path writeTemp(String filename, String content) throws IOException {
        Path p = temp.resolve(filename);
        Files.createDirectories(p.getParent());
        try (BufferedWriter w = Files.newBufferedWriter(p, StandardCharsets.UTF_8)) {
            w.write(content);
        }
        return p;
    }

    @Test
    void parsesSimpleNTriples() throws Exception {
        String nt = """
            <http://ex/s1> <http://ex/p1> <http://ex/o1> .
        """;
        Path f = writeTemp("tiny.nt", nt);

        List<Triplet> out = new ArrayList<>();
        RdfReader.streamRdf(f.toString(), out::add);

        assertEquals(1, out.size());
        assertEquals("http://ex/s1", out.get(0).getSubject());
        assertEquals("http://ex/p1", out.get(0).getPredicate());
        assertEquals("http://ex/o1", out.get(0).getObject());
    }

    @Test
    void parsesTurtleWithLangAndDatatype() throws Exception {
        String ttl = """
            @prefix ex: <http://ex/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
            ex:s1 ex:p "γειά"@el .
            ex:s1 ex:age "42"^^xsd:int .
            ex:s2 ex:p ex:o2 .
            """;
        Path f = writeTemp("tiny.ttl", ttl);

        List<Triplet> out = new ArrayList<>();
        RdfReader.streamRdf(f.toString(), out::add);

        assertEquals(3, out.size());

        Triplet t0 = out.get(0);
        assertEquals("http://ex/s1", t0.getSubject());
        assertEquals("http://ex/p",   t0.getPredicate());
        assertTrue(t0.getObject().contains("@el"));

        Triplet t1 = out.get(1);
        assertEquals("http://ex/s1", t1.getSubject());
        assertEquals("http://ex/age", t1.getPredicate());
        assertTrue(t1.getObject().contains("^^"));

        Triplet t2 = out.get(2);
        assertEquals("http://ex/s2", t2.getSubject());
        assertEquals("http://ex/p",  t2.getPredicate());
        assertEquals("http://ex/o2", t2.getObject());
    }

    @Test
    void parsesRdfXmlWithLangAndDatatype() throws Exception {
        String rdfxml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <rdf:RDF
              xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
              xmlns:ex="http://ex/"
              xmlns:xsd="http://www.w3.org/2001/XMLSchema#">
              <rdf:Description rdf:about="http://ex/s1">
                <ex:p rdf:resource="http://ex/o1"/>
                <ex:age rdf:datatype="http://www.w3.org/2001/XMLSchema#int">42</ex:age>
                <ex:label xml:lang="el">γειά</ex:label>
              </rdf:Description>
            </rdf:RDF>
            """;
        Path f = writeTemp("tiny.rdf", rdfxml);

        List<Triplet> out = new ArrayList<>();
        RdfReader.streamRdf(f.toString(), out::add);

        // Expect: (s1 ex:p o1), (s1 ex:age "42"^^xsd:int), (s1 ex:label "γειά"@el)
        assertEquals(3, out.size());

        boolean sawP = false, sawAge = false, sawLabel = false;
        for (Triplet t : out) {
            assertEquals("http://ex/s1", t.getSubject());
            switch (t.getPredicate()) {
                case "http://ex/p" -> {
                    assertEquals("http://ex/o1", t.getObject());
                    sawP = true;
                }
                case "http://ex/age" -> {
                    assertTrue(t.getObject().contains("42"));
                    assertTrue(t.getObject().contains("http://www.w3.org/2001/XMLSchema#int"));
                    sawAge = true;
                }
                case "http://ex/label" -> {
                    assertTrue(t.getObject().contains("γειά"));
                    assertTrue(t.getObject().contains("@el"));
                    sawLabel = true;
                }
                default -> fail("Unexpected predicate: " + t.getPredicate());
            }
        }
        assertTrue(sawP && sawAge && sawLabel, "Missing expected RDF/XML triples");
    }

    @Test
    void streamsLargeNTriplesWithoutExploding() throws Exception {
        Path f = temp.resolve("bulk.nt");
        try (BufferedWriter w = Files.newBufferedWriter(f, StandardCharsets.UTF_8)) {
            for (int i = 0; i < 10_000; i++) {
                w.write("<http://ex/s" + i + "> <http://ex/p> <http://ex/o" + i + "> .\n");
            }
        }

        AtomicInteger count = new AtomicInteger();
        RdfReader.streamRdf(f.toString(), t -> {
            if (count.get() == 0) assertEquals("http://ex/s0", t.getSubject());
            count.incrementAndGet();
        });

        assertEquals(10_000, count.get());
    }

    @Test
    void rejectsMalformedInput() throws Exception {
        String bad = """
            <http://ex/s> <http://ex/p> "unterminated .
            """;
        Path f = writeTemp("bad.nt", bad);

        List<Triplet> sink = new ArrayList<>();
        Exception ex = assertThrows(Exception.class, () -> RdfReader.streamRdf(f.toString(), sink::add));
        assertTrue(ex.getMessage() != null && !ex.getMessage().isBlank());
        assertTrue(sink.isEmpty(), "No triples should have been emitted on malformed input");
    }

    @Test
    void failsForMissingFile() {
        List<Triplet> sink = new ArrayList<>();
        assertThrows(Exception.class, () -> RdfReader.streamRdf("/does/not/exist.ttl", sink::add));
    }
}
