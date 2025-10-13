package org.example.util;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class DataPaths {
    private DataPaths() {}

    public static String baseDir() {
        String fromProp = System.getProperty("rdfparquet.dataDir");
        if (fromProp != null && !fromProp.isBlank()) {
            return Paths.get(fromProp).toAbsolutePath().toString();
        }

        String fromEnv = System.getenv("RDFPARQUET_DATA_DIR");
        if (fromEnv != null && !fromEnv.isBlank()) {
            return Paths.get(fromEnv).toAbsolutePath().toString();
        }

        Path cwdData = Paths.get(System.getProperty("user.dir"), "data");
        if (canCreateOrWrite(cwdData)) {
            return cwdData.toAbsolutePath().toString();
        }

        // Fallback: user home
        Path homeData = Paths.get(System.getProperty("user.home"), "rdfparquet-data");
        return homeData.toAbsolutePath().toString();
    }

    public static Path parquetDir() {
        return Paths.get(baseDir(), "parquet");
    }

    public static Path dictPath() {
        return parquetDir().resolve("dictionary.parquet");
    }

    public static Path spo() { return parquetDir().resolve("spo.parquet"); }
    public static Path sop() { return parquetDir().resolve("sop.parquet"); }
    public static Path pso() { return parquetDir().resolve("pso.parquet"); }
    public static Path pos() { return parquetDir().resolve("pos.parquet"); }
    public static Path osp() { return parquetDir().resolve("osp.parquet"); }
    public static Path ops() { return parquetDir().resolve("ops.parquet"); }

    public static void ensureParquetDir() {
        try {
            Files.createDirectories(parquetDir());
        } catch (Exception e) {
            throw new RuntimeException("Failed to create data dir: " + parquetDir(), e);
        }
    }

    /** Throw a clear error if a required file is missing. */
    public static void requireExists(Path p, String what) {
        if (!Files.exists(p)) {
            throw new IllegalStateException(
                    what + " not found at " + p + ". Did you run the loader with the same dataDir?"
            );
        }
    }

    // ---------- helpers ----------

    private static boolean canCreateOrWrite(Path dir) {
        try {

            if (!Files.exists(dir)) {
                Files.createDirectories(dir);
                return true;
            }
            if (!Files.isDirectory(dir)) return false;
            Path probe = dir.resolve(".write-check-" + System.nanoTime());
            Files.createFile(probe);
            Files.deleteIfExists(probe);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
