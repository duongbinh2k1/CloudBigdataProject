// compression ratio and compression / decompression time of lz4 and zstd on one file
// usage: java -cp "$SPARK_HOME/jars/*" bench/CodecBench.java <text file> <out.csv>

import java.io.*;
import java.nio.file.*;
import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.io.*;

public class CodecBench {

    static final String[] CODECS = {"lz4", "zstd-1", "zstd-3"};

    static CompressionCodec codec(String name) {
        SparkConf conf = new SparkConf(false);
        if (name.equals("lz4")) {
            return new LZ4CompressionCodec(conf);
        }
        conf.set("spark.io.compression.zstd.level", name.substring(5));
        return new ZStdCompressionCodec(conf);
    }

    public static void main(String[] args) throws Exception {
        byte[] data = Files.readAllBytes(Paths.get(args[0]));
        try (PrintWriter csv = new PrintWriter(args[1])) {
            csv.println("codec,repetition,input_bytes,compressed_bytes,compress_s,decompress_s");
            for (String name : CODECS) {
                CompressionCodec codec = codec(name);
                for (int rep = 0; rep < 7; rep++) {   // 2 warm up runs + 5 measured
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                    long t0 = System.nanoTime();
                    try (OutputStream out = codec.compressedOutputStream(buffer)) {
                        out.write(data);
                    }
                    long t1 = System.nanoTime();
                    byte[] restored;
                    try (InputStream in = codec.compressedInputStream(new ByteArrayInputStream(buffer.toByteArray()))) {
                        restored = in.readAllBytes();
                    }
                    long t2 = System.nanoTime();
                    if (!Arrays.equals(data, restored)) {
                        throw new IllegalStateException(name + ": decompressed data is different");
                    }
                    if (rep >= 2) {
                        csv.printf(Locale.ROOT, "%s,%d,%d,%d,%.4f,%.4f%n", name, rep - 1, data.length,
                            buffer.size(), (t1 - t0) / 1e9, (t2 - t1) / 1e9);
                    }
                }
                System.out.println(name + " done");
            }
        }
    }
}
