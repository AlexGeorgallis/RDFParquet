package org.example.dictionary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.example.RdfReader;
import org.example.Triplet;
import org.example.encodedTriplet.EncodedTriplet;
import org.example.encodedTriplet.EncodedTripletParquetWriter;
import org.example.util.DataPaths;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class MainDictionary {

    private static final List<String> SUPPORTED_EXT = Arrays.asList(".nt", ".ttl", ".rdf");
    private static final String DEFAULT_OUTPUT_DIR = DataPaths.baseDir();

    public static class Config {
        private final String baseOutputDir;
        private final FileSystem fileSystem;
        private final Configuration hadoopConfig;

        public Config(String baseOutputDir, FileSystem fileSystem, Configuration hadoopConfig) {
            this.baseOutputDir = baseOutputDir;
            this.fileSystem = fileSystem;
            this.hadoopConfig = hadoopConfig;
        }

        public String getBaseOutputDir() { return baseOutputDir; }
        public FileSystem getFileSystem() { return fileSystem; }
        public Configuration getHadoopConfig() { return hadoopConfig; }

        public static Config defaultConfig() throws IOException {
            Configuration conf = new Configuration();
            return new Config(DEFAULT_OUTPUT_DIR, FileSystem.get(conf), conf);
        }
    }

    public static class ProcessingResults {
        private final int distinctTriples;
        private final int distinctTerms;
        private final Map<String, String> outputFiles;

        public ProcessingResults(int distinctTriples, int distinctTerms, Map<String, String> outputFiles) {
            this.distinctTriples = distinctTriples;
            this.distinctTerms = distinctTerms;
            this.outputFiles = new HashMap<>(outputFiles);
        }

        public int getDistinctTriples() { return distinctTriples; }
        public int getDistinctTerms() { return distinctTerms; }
        public Map<String, String> getOutputFiles() { return Collections.unmodifiableMap(outputFiles); }
    }

    private final Config config;

    public MainDictionary(Config config) {
        this.config = config;
    }

    public MainDictionary() throws IOException {
        this(Config.defaultConfig());
    }

    public ProcessingResults processRdfFile(String rdfFilePath) throws Exception {
        validateFileExtension(rdfFilePath);

        DataPaths.ensureParquetDir();
        File parquetDir = DataPaths.parquetDir().toFile();

        Map<String, String> outputFiles = createOutputFilePaths(parquetDir);

        System.out.println("[Loader] dataDir=" + DataPaths.baseDir());
        outputFiles.forEach((k, v) -> System.out.println("  " + k + " -> " + v));

        System.out.println("Loading RDF data...");
        List<Triplet> triplets = loadRdfData(rdfFilePath);
        System.out.println("Loaded " + triplets.size() + " triplets");

        System.out.println("Deduplicating triplets...");
        List<Triplet> uniqueTriplets = deduplicateTriples(triplets);
        System.out.println("Unique triplets: " + uniqueTriplets.size());

        System.out.println("Extracting terms...");
        Set<String> terms = extractTerms(uniqueTriplets);
        System.out.println("Unique terms: " + terms.size());

        System.out.println("Building dictionary...");
        DictionaryEncoder encoder = DictionaryEncoder.getInstance();
        encoder.init(terms.size());

        System.out.println("Encoding triplets...");
        List<EncodedTriplet> encodedTriplets = encodeTriples(uniqueTriplets, encoder);

        System.out.println("Writing parquet files...");
        writeParquetFiles(encodedTriplets, parquetDir);
        writeDictionaryParquet(encoder, outputFiles.get("dictionary"));

        return new ProcessingResults(
                uniqueTriplets.size(),
                terms.size(),
                outputFiles
        );
    }

    protected void validateFileExtension(String rdfFilePath) {
        int dot = rdfFilePath.lastIndexOf('.');
        if (dot < 0) throw new IllegalArgumentException("File has no extension: " + rdfFilePath);
        String ext = rdfFilePath.substring(dot).toLowerCase();
        if (!SUPPORTED_EXT.contains(ext)) {
            throw new IllegalArgumentException("Unsupported file type '" + ext + "'.");
        }
    }

    protected List<Triplet> loadRdfData(String rdfFilePath) throws Exception {
        List<Triplet> triplets = new ArrayList<>();
        RdfReader.streamRdf(rdfFilePath, triplets::add);
        return triplets;
    }

    protected List<Triplet> deduplicateTriples(List<Triplet> triplets) {
        return new ArrayList<>(new LinkedHashSet<>(triplets));
    }

    protected Set<String> extractTerms(List<Triplet> triplets) {
        Set<String> terms = new HashSet<>();
        for (Triplet t : triplets) {
            terms.add(t.getSubject());
            terms.add(t.getPredicate());
            terms.add(t.getObject());
        }
        return terms;
    }

    protected List<EncodedTriplet> encodeTriples(List<Triplet> triplets, DictionaryEncoder encoder) {
        return triplets.stream()
                .map(t -> new EncodedTriplet(
                        encoder.encode(t.getSubject()),
                        encoder.encode(t.getPredicate()),
                        encoder.encode(t.getObject())))
                .collect(Collectors.toList());
    }

    private Map<String, String> createOutputFilePaths(File parquetDir) {
        Map<String, String> paths = new HashMap<>();
        paths.put("dictionary", new File(parquetDir, "dictionary.parquet").getPath());
        paths.put("spo", new File(parquetDir, "spo.parquet").getPath());
        paths.put("sop", new File(parquetDir, "sop.parquet").getPath());
        paths.put("pso", new File(parquetDir, "pso.parquet").getPath());
        paths.put("pos", new File(parquetDir, "pos.parquet").getPath());
        paths.put("osp", new File(parquetDir, "osp.parquet").getPath());
        paths.put("ops", new File(parquetDir, "ops.parquet").getPath());
        return paths;
    }

    private void writeParquetFiles(List<EncodedTriplet> encoded, File parquetDir) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType(
                "message EncodedTriplet { required int32 subject; required int32 predicate; required int32 object; }"
        );

        // Output paths
        String spoPath = new File(parquetDir, "spo.parquet").getPath();
        String sopPath = new File(parquetDir, "sop.parquet").getPath();
        String psoPath = new File(parquetDir, "pso.parquet").getPath();
        String posPath = new File(parquetDir, "pos.parquet").getPath();
        String ospPath = new File(parquetDir, "osp.parquet").getPath();
        String opsPath = new File(parquetDir, "ops.parquet").getPath();

        deleteIfExists(spoPath);
        deleteIfExists(sopPath);
        deleteIfExists(psoPath);
        deleteIfExists(posPath);
        deleteIfExists(ospPath);
        deleteIfExists(opsPath);

        final EncodedTriplet[] base = encoded.toArray(new EncodedTriplet[0]);
        final int n = base.length;

        record TaskSpec(String name, java.util.function.IntBinaryOperator cmp, String out) {}

        List<TaskSpec> tasks = List.of(
                new TaskSpec("SPO", (i, j) -> {
                    int c = Integer.compare(base[i].getSubject(), base[j].getSubject());
                    if (c != 0) return c;
                    c = Integer.compare(base[i].getPredicate(), base[j].getPredicate());
                    if (c != 0) return c;
                    return Integer.compare(base[i].getObject(), base[j].getObject());
                }, spoPath),
                new TaskSpec("SOP", (i, j) -> {
                    int c = Integer.compare(base[i].getSubject(), base[j].getSubject());
                    if (c != 0) return c;
                    c = Integer.compare(base[i].getObject(), base[j].getObject());
                    if (c != 0) return c;
                    return Integer.compare(base[i].getPredicate(), base[j].getPredicate());
                }, sopPath),
                new TaskSpec("PSO", (i, j) -> {
                    int c = Integer.compare(base[i].getPredicate(), base[j].getPredicate());
                    if (c != 0) return c;
                    c = Integer.compare(base[i].getSubject(), base[j].getSubject());
                    if (c != 0) return c;
                    return Integer.compare(base[i].getObject(), base[j].getObject());
                }, psoPath),
                new TaskSpec("POS", (i, j) -> {
                    int c = Integer.compare(base[i].getPredicate(), base[j].getPredicate());
                    if (c != 0) return c;
                    c = Integer.compare(base[i].getObject(), base[j].getObject());
                    if (c != 0) return c;
                    return Integer.compare(base[i].getSubject(), base[j].getSubject());
                }, posPath),
                new TaskSpec("OSP", (i, j) -> {
                    int c = Integer.compare(base[i].getObject(), base[j].getObject());
                    if (c != 0) return c;
                    c = Integer.compare(base[i].getSubject(), base[j].getSubject());
                    if (c != 0) return c;
                    return Integer.compare(base[i].getPredicate(), base[j].getPredicate());
                }, ospPath),
                new TaskSpec("OPS", (i, j) -> {
                    int c = Integer.compare(base[i].getObject(), base[j].getObject());
                    if (c != 0) return c;
                    c = Integer.compare(base[i].getPredicate(), base[j].getPredicate());
                    if (c != 0) return c;
                    return Integer.compare(base[i].getSubject(), base[j].getSubject());
                }, opsPath)
        );

        // Auto-pick threads
        int threads = Math.min(tasks.size(), Math.max(2, Runtime.getRuntime().availableProcessors()));
        System.out.println("[Loader] writerThreads=" + threads);

        var pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (TaskSpec t : tasks) {
                futures.add(pool.submit(() -> {
                    System.out.println("Writing " + t.name() + " sorted parquet...");

                    int[] idx = new int[n];
                    for (int i = 0; i < n; i++) idx[i] = i;
                    sortIndex(idx, t.cmp());

                    try (ParquetWriter<EncodedTriplet> writer = EncodedTripletParquetWriter.create(
                            new Path(t.out()), schema, CompressionCodecName.SNAPPY)) {
                        for (int k = 0; k < n; k++) {
                            writer.write(base[idx[k]]);
                        }
                    }
                    return null;
                }));
            }
            for (var f : futures) f.get();
        } catch (Exception e) {
            throw new IOException("Parallel write failed", e);
        } finally {
            pool.shutdown();
            try {
                if (!pool.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)) pool.shutdownNow();
            } catch (InterruptedException ie) {
                pool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private void writeDictionaryParquet(DictionaryEncoder encoder, String dictPath) throws IOException {
        deleteIfExists(dictPath);
        MessageType dictSchema = MessageTypeParser.parseMessageType(
                "message DictionaryEntry { required int32 id; required binary value (UTF8); }"
        );
        try (ParquetWriter<DictionaryEntry> writer = DictionaryParquetWriter.create(
                new Path(dictPath), dictSchema, CompressionCodecName.ZSTD)) {

            List<DictionaryEntry> entries = new ArrayList<>(encoder.getEntries());
            entries.sort(Comparator.comparingInt(DictionaryEntry::getId));
            for (DictionaryEntry entry : entries) {
                writer.write(entry);
            }
        }
    }

    private void deleteIfExists(String path) throws IOException {
        java.nio.file.Path p = java.nio.file.Paths.get(path);
        try {
            java.nio.file.Files.deleteIfExists(p);
            return;
        } catch (IOException ioe) {
            // fall back to Hadoop FS if local delete fails
        }
        Path hp = new Path(path);
        if (config.getFileSystem().exists(hp)) {
            if (!config.getFileSystem().delete(hp, false)) {
                throw new IOException("Failed to delete existing file: " + path);
            }
        }
    }

    protected DictionaryEncoder buildDictionary(Set<String> terms) {
        DictionaryEncoder encoder = DictionaryEncoder.getInstance();
        encoder.init(terms.size());
        return encoder;
    }

    // Int-index quicksort with comparator over base[]
    private static void sortIndex(int[] idx, java.util.function.IntBinaryOperator cmp) {
        quicksort(idx, 0, idx.length - 1, cmp);
    }
    private static void quicksort(int[] a, int lo, int hi, java.util.function.IntBinaryOperator cmp) {
        while (lo < hi) {
            int i = lo, j = hi;
            int pivot = a[lo + ((hi - lo) >>> 1)];
            while (i <= j) {
                while (cmp.applyAsInt(a[i], pivot) < 0) i++;
                while (cmp.applyAsInt(a[j], pivot) > 0) j--;
                if (i <= j) { swap(a, i, j); i++; j--; }
            }
            if (j - lo < hi - i) {
                if (lo < j) quicksort(a, lo, j, cmp);
                lo = i;
            } else {
                if (i < hi) quicksort(a, i, hi, cmp);
                hi = j;
            }
        }
    }
    private static void swap(int[] a, int i, int j) {
        int t = a[i]; a[i] = a[j]; a[j] = t;
    }

    public static void main(String[] args) {
        if (args.length != 1) { printUsage(); System.exit(1); }
        try {
            System.out.println("Starting RDF processing...");
            MainDictionary proc = new MainDictionary();
            ProcessingResults res = proc.processRdfFile(args[0]);

            System.out.println("Processing completed successfully!");
            System.out.println("Distinct triples: " + res.getDistinctTriples());
            System.out.println("Distinct terms: " + res.getDistinctTerms());
            System.out.println("Output files:");
            res.getOutputFiles().forEach((k, v) -> System.out.println("  " + k + ": " + v));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(3);
        }
    }

    private static void printUsage() {
        System.err.println("Usage:");
        System.err.println("  java -jar dictionary-loader.jar <input.(nt|ttl|rdf)>");
        System.err.println();
        System.err.println("Notes:");
        System.err.println("  - Output directory defaults to ./data/parquet (override via DataPaths if needed).");
    }
}
