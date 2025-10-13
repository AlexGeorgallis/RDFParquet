package org.example;

import org.example.SparqlParser.ParsedQuery;
import org.example.SparqlParser.TriplePattern;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SparqlParserTest {

    @Test
    void parsesBasicSelectWhere() {
        String q = """
            PREFIX ex: <http://ex/>
            SELECT ?s ?o
            WHERE { ?s ex:p ?o . }
            """;
        ParsedQuery pq = SparqlParser.parse(q);

        assertEquals(List.of("?s", "?o"), pq.selectVars);
        assertFalse(pq.distinct);
        assertEquals(-1, pq.limit);
        assertEquals(1, pq.patterns.size());

        TriplePattern tp = pq.patterns.get(0);
        assertEquals("?s", tp.subject);
        assertEquals("http://ex/p", tp.predicate);   // prefix expanded
        assertEquals("?o", tp.object);
    }

    @Test
    void expandsPrefixesInsideTriplesAndAngleIris() {
        String q = """
            PREFIX ex: <http://ex/>
            SELECT ?s
            WHERE {
              ex:s ex:p <http://ex/o> .
            }
            """;
        ParsedQuery pq = SparqlParser.parse(q);
        assertEquals(1, pq.patterns.size());
        TriplePattern tp = pq.patterns.get(0);
        assertEquals("http://ex/s", tp.subject);
        assertEquals("http://ex/p", tp.predicate);
        assertEquals("http://ex/o", tp.object); // angle brackets stripped
    }

    @Test
    void supportsDistinctAndLimit() {
        String q = """
            PREFIX ex: <http://ex/>
            SELECT DISTINCT ?s
            WHERE { ?s ex:p "hello" . }
            LIMIT 5
            """;
        ParsedQuery pq = SparqlParser.parse(q);

        assertTrue(pq.distinct);
        assertEquals(5, pq.limit);
        assertEquals(1, pq.patterns.size());

        TriplePattern tp = pq.patterns.get(0);
        assertEquals("?s", tp.subject);
        assertEquals("http://ex/p", tp.predicate);
        assertEquals("hello", tp.object);
    }

    @Test
    void expandsSemicolonSyntaxIntoMultipleTriples() {
        String q = """
            PREFIX ex: <http://ex/>
            SELECT ?s
            WHERE {
              ?s ex:p1 "a" ; ex:p2 ex:o2 .
            }
            """;
        ParsedQuery pq = SparqlParser.parse(q);
        assertEquals(2, pq.patterns.size());

        TriplePattern t0 = pq.patterns.get(0);
        assertEquals("?s", t0.subject);
        assertEquals("http://ex/p1", t0.predicate);
        assertEquals("a", t0.object); // quotes stripped

        TriplePattern t1 = pq.patterns.get(1);
        assertEquals("?s", t1.subject);
        assertEquals("http://ex/p2", t1.predicate);
        assertEquals("http://ex/o2", t1.object);
    }

    @Test
    void rejectsUnknownPrefix() {
        String q = """
            SELECT ?s WHERE { ex:s ex:p ex:o . }
            """;
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> SparqlParser.parse(q));
        assertTrue(ex.getMessage().contains("Unknown prefix"));
    }

    @Test
    void rejectsInvalidWhereBlock() {
        String q = """
            PREFIX ex: <http://ex/>
            SELECT ?s WHERE  ?s ex:p ex:o .   // missing braces
            """;
        assertThrows(IllegalArgumentException.class, () -> SparqlParser.parse(q));
    }
}
