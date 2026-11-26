// Intrinsic characteristics of the codecs (section 4 of the assignment), on one machine, without Spark jobs,
// HDFS or network: compression ratio, compression and decompression speed.
// lz4, snappy and zstd use the classes Spark uses for the shuffle (zstd level 1 = Spark's default,
// level 3 = Hadoop's default for files); gzip uses java.util.zip (level 6, the default of Hadoop's GzipCodec).
// One thread and the data in memory, so the time measured is CPU time.
//   java -cp "$SPARK_HOME/jars/*" bench/CodecBench.java <text file> <out.csv>

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.zip.*;
import org.apache.spark.SparkConf;
import org.apache.spark.io.*;

public class CodecBench {

    static final String[] CODECS = {"lz4", "snappy", "zstd-1", "zstd-3", "gzip"};

    static OutputStream compress(String codec, OutputStream out) throws IOException {
        SparkConf conf = new SparkConf(false);
        switch (codec) {
            case "lz4": return new LZ4CompressionCodec(conf).compressedOutputStream(out);
            case "snappy": return new SnappyCompressionCodec(conf).compressedOutputStream(out);
            case "gzip": return new GZIPOutputStream(out, 65536);
            default:   // zstd-N
                conf.set("spark.io.compression.zstd.level", codec.substring(5));
                return new ZStdCompressionCodec(conf).compressedOutputStream(out);
        }
    }

    static InputStream decompress(String codec, InputStream in) throws IOException {
        SparkConf conf = new SparkConf(false);
        switch (codec) {
            case "lz4": return new LZ4CompressionCodec(conf).compressedInputStream(in);
            case "snappy": return new SnappyCompressionCodec(conf).compressedInputStream(in);
            case "gzip": return new GZIPInputStream(in, 65536);
            default: return new ZStdCompressionCodec(conf).compressedInputStream(in);
        }
    }

    public static void main(String[] args) throws Exception {
        Map<String, byte[]> inputs = new LinkedHashMap<>();
        inputs.put("lineitem", Files.readAllBytes(Paths.get(args[0])));
        byte[] random = new byte[64 << 20];          // random bytes cannot be compressed: worst case
        new Random(42).nextBytes(random);
        inputs.put("random", random);

        try (PrintWriter csv = new PrintWriter(args[1])) {
            csv.println("input,codec,repetition,input_bytes,compressed_bytes,compress_s,decompress_s");
            for (String input : inputs.keySet()) {
                byte[] data = inputs.get(input);
                for (String codec : CODECS) {
                    for (int rep = 0; rep < 7; rep++) {        // the first 2 warm up the JVM, not recorded
                        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                        long t0 = System.nanoTime();
                        try (OutputStream out = compress(codec, buffer)) {
                            out.write(data);
                        }
                        long t1 = System.nanoTime();
                        byte[] restored;
                        try (InputStream in = decompress(codec, new ByteArrayInputStream(buffer.toByteArray()))) {
                            restored = in.readAllBytes();
                        }
                        long t2 = System.nanoTime();
                        if (!Arrays.equals(data, restored)) {
                            throw new IllegalStateException(codec + ": decompressed data is different");
                        }
                        if (rep >= 2) {
                            csv.printf(Locale.ROOT, "%s,%s,%d,%d,%d,%.4f,%.4f%n", input, codec, rep - 1,
                                data.length, buffer.size(), (t1 - t0) / 1e9, (t2 - t1) / 1e9);
                        }
                    }
                    System.out.println(input + " " + codec + " done");
                }
            }
        }
    }
}
