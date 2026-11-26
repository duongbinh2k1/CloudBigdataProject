// Codec micro-benchmark on one node, with Spark's own codec classes (gzip: java.util.zip)
//   java -cp "$SPARK_HOME/jars/*" bench/CodecBench.java <out.csv> <text file>

import java.io.*;
import java.lang.management.*;
import java.nio.file.*;
import java.util.*;
import java.util.zip.*;
import org.apache.spark.SparkConf;
import org.apache.spark.io.*;

public class CodecBench {

    static final String[] CODECS = {"lz4", "snappy", "lzf", "zstd-1", "zstd-3", "zstd-9", "gzip-6"};

    static OutputStream compress(String codec, OutputStream out) throws IOException {
        if (codec.startsWith("gzip")) {
            return new GZIPOutputStream(out, 65536) { { def.setLevel(6); } };
        }
        return sparkCodec(codec).compressedOutputStream(out);
    }

    static InputStream decompress(String codec, InputStream in) throws IOException {
        if (codec.startsWith("gzip")) {
            return new GZIPInputStream(in, 65536);
        }
        return sparkCodec(codec).compressedInputStream(in);
    }

    static CompressionCodec sparkCodec(String codec) {
        SparkConf conf = new SparkConf(false);
        if (codec.equals("lz4")) return new LZ4CompressionCodec(conf);
        if (codec.equals("snappy")) return new SnappyCompressionCodec(conf);
        if (codec.equals("lzf")) return new LZFCompressionCodec(conf);
        conf.set("spark.io.compression.zstd.level", codec.substring(5));   // zstd-N
        return new ZStdCompressionCodec(conf);
    }

    static final ThreadMXBean THREAD = ManagementFactory.getThreadMXBean();   // CPU time of this thread

    /** Returns {compressed bytes, compression ns, decompression ns, compression CPU ns, decompression CPU ns}. */
    static long[] roundTrip(String codec, byte[] data) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        long t0 = System.nanoTime();
        long c0 = THREAD.getCurrentThreadCpuTime();
        try (OutputStream out = compress(codec, buffer)) {
            for (int off = 0; off < data.length; off += 65536) {     // write in 64 KB chunks
                out.write(data, off, Math.min(65536, data.length - off));
            }
        }
        long compressNs = System.nanoTime() - t0;
        long compressCpuNs = THREAD.getCurrentThreadCpuTime() - c0;

        byte[] compressed = buffer.toByteArray();
        t0 = System.nanoTime();
        c0 = THREAD.getCurrentThreadCpuTime();
        byte[] restored;
        try (InputStream in = decompress(codec, new ByteArrayInputStream(compressed))) {
            restored = in.readAllBytes();
        }
        long decompressNs = System.nanoTime() - t0;
        long decompressCpuNs = THREAD.getCurrentThreadCpuTime() - c0;

        if (!Arrays.equals(data, restored)) {
            throw new IOException(codec + ": decompressed data is different");
        }
        return new long[] {compressed.length, compressNs, decompressNs, compressCpuNs, decompressCpuNs};
    }

    public static void main(String[] args) throws Exception {
        byte[] random = new byte[64 * 1024 * 1024];       // random bytes cannot be compressed
        new Random(42).nextBytes(random);
        Map<String, byte[]> inputs = new LinkedHashMap<>();
        inputs.put("csv", Files.readAllBytes(Paths.get(args[1])));
        inputs.put("random", random);

        try (PrintWriter csv = new PrintWriter(args[0])) {
            csv.println("input,codec,iteration,input_bytes,compressed_bytes,comp_wall_ns,decomp_wall_ns,comp_cpu_ns,decomp_cpu_ns");
            for (String input : inputs.keySet()) {
                byte[] data = inputs.get(input);
                for (String codec : CODECS) {
                    roundTrip(codec, data);
                    roundTrip(codec, data);
                    for (int i = 0; i < 5; i++) {
                        long[] r = roundTrip(codec, data);
                        csv.println(input + "," + codec + "," + i + "," + data.length + "," + r[0] + "," + r[1] + "," + r[2] + "," + r[3] + "," + r[4]);
                    }
                    System.out.println(input + " " + codec + " done");
                }
            }
        }
    }
}
