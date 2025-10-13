package org.example.encodedTriplet;

import org.apache.hadoop.fs.Path; // Hadoop Path
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.hadoop.ParquetReader;
import org.example.SparqlParser.TriplePattern;
import org.example.dictionary.DictionaryEncoder;
import org.example.util.DataPaths;

import java.io.IOException;
import java.util.*;

public class EncodedParquetQuery {

    private final DictionaryEncoder dictionary;
    private final Map<String, Integer> encodeMap; // Cached dictionary map

    private final java.nio.file.Path SPO = DataPaths.spo();
    private final java.nio.file.Path SOP = DataPaths.sop();
    private final java.nio.file.Path PSO = DataPaths.pso();
    private final java.nio.file.Path POS = DataPaths.pos();
    private final java.nio.file.Path OPS = DataPaths.ops();
    private final java.nio.file.Path OSP = DataPaths.osp();
    private final java.nio.file.Path DICT = DataPaths.dictPath();

    public EncodedParquetQuery() {
        DataPaths.requireExists(DICT, "Dictionary");
        DataPaths.requireExists(SPO, "SPO parquet");
        DataPaths.requireExists(SOP, "SOP parquet");
        DataPaths.requireExists(PSO, "PSO parquet");
        DataPaths.requireExists(POS, "POS parquet");
        DataPaths.requireExists(OPS, "OPS parquet");
        DataPaths.requireExists(OSP, "OSP parquet");

        this.dictionary = DictionaryEncoder.getInstance();
        this.encodeMap = dictionary.getEncodeMap();
    }

    private List<EncodedTriplet> executeIds(java.nio.file.Path parquetPathNio, FilterCompat.Filter filter)
            throws IOException {
        long start = System.nanoTime();
        List<EncodedTriplet> out = new ArrayList<>();

        try (ParquetReader<EncodedTriplet> reader =
                     EncodedTripletParquetReader.create(new Path(parquetPathNio.toString()), filter)) {
            EncodedTriplet et;
            while ((et = reader.read()) != null) {
                out.add(new EncodedTriplet(et.getSubject(), et.getPredicate(), et.getObject()));
            }
        }
        long end = System.nanoTime();
        System.out.printf("Path: %s – %,d rows in %,f ms%n",
                parquetPathNio.toAbsolutePath(), out.size(), (end - start) / 1_000_000.0);
        return out;
    }

    public List<EncodedTriplet> querySPOIds(TriplePattern pattern) throws IOException {
        Integer s = encodeMap.get(pattern.subject);
        Integer p = encodeMap.get(pattern.predicate);
        Integer o = encodeMap.get(pattern.object);
        if (s == null || p == null || o == null) return List.of();

        FilterCompat.Filter filter = FilterCompat.get(
                FilterApi.and(
                        FilterApi.eq(FilterApi.intColumn("subject"), s),
                        FilterApi.and(
                                FilterApi.eq(FilterApi.intColumn("predicate"), p),
                                FilterApi.eq(FilterApi.intColumn("object"), o)
                        )
                )
        );
        return executeIds(SPO, filter);
    }

    public List<EncodedTriplet> queryPOIds(TriplePattern pattern) throws IOException {
        Integer p = encodeMap.get(pattern.predicate);
        Integer o = encodeMap.get(pattern.object);
        if (p == null || o == null) return List.of();

        FilterCompat.Filter filter = FilterCompat.get(
                FilterApi.and(
                        FilterApi.eq(FilterApi.intColumn("predicate"), p),
                        FilterApi.eq(FilterApi.intColumn("object"), o)
                )
        );
        return executeIds(POS, filter);
    }

    public List<EncodedTriplet> querySPIds(TriplePattern pattern) throws IOException {
        Integer s = encodeMap.get(pattern.subject);
        Integer p = encodeMap.get(pattern.predicate);
        if (s == null || p == null) return List.of();

        FilterCompat.Filter filter = FilterCompat.get(
                FilterApi.and(
                        FilterApi.eq(FilterApi.intColumn("subject"), s),
                        FilterApi.eq(FilterApi.intColumn("predicate"), p)
                )
        );
        return executeIds(SPO, filter);
    }

    public List<EncodedTriplet> querySOIds(TriplePattern pattern) throws IOException {
        Integer s = encodeMap.get(pattern.subject);
        Integer o = encodeMap.get(pattern.object);
        if (s == null || o == null) return List.of();

        FilterCompat.Filter filter = FilterCompat.get(
                FilterApi.and(
                        FilterApi.eq(FilterApi.intColumn("subject"), s),
                        FilterApi.eq(FilterApi.intColumn("object"), o)
                )
        );
        return executeIds(SOP, filter);
    }

    public List<EncodedTriplet> queryObjectOnlyIds(TriplePattern pattern) throws IOException {
        Integer o = encodeMap.get(pattern.object);
        if (o == null) return List.of();

        FilterCompat.Filter filter = FilterCompat.get(
                FilterApi.eq(FilterApi.intColumn("object"), o)
        );
        return executeIds(OSP, filter);
    }

    public List<EncodedTriplet> queryPredicateOnlyIds(TriplePattern pattern) throws IOException {
        Integer p = encodeMap.get(pattern.predicate);
        if (p == null) return List.of();

        FilterCompat.Filter filter = FilterCompat.get(
                FilterApi.eq(FilterApi.intColumn("predicate"), p)
        );
        return executeIds(PSO, filter);
    }

    public List<EncodedTriplet> querySubjectOnlyIds(TriplePattern pattern) throws IOException {
        Integer s = encodeMap.get(pattern.subject);
        if (s == null) return List.of();

        FilterCompat.Filter filter = FilterCompat.get(
                FilterApi.eq(FilterApi.intColumn("subject"), s)
        );
        return executeIds(SPO, filter);
    }

    public List<EncodedTriplet> queryAllIds() throws IOException {
        return executeIds(SPO, FilterCompat.NOOP);
    }
}
