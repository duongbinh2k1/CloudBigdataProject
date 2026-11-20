package compression.common;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.ZStandardCodec;
import org.apache.spark.api.java.JavaRDD;

// Hadoop codecs for the files on HDFS. When reading, Hadoop finds the codec from the file
// extension (.gz, .snappy, .lz4, .zst). zstd needs Hadoop's native library (only on EC2).
public class Codecs {

    public static Class<? extends CompressionCodec> codecClass(String name) {
        switch (name) {
            case "none": return null;
            case "gzip": return GzipCodec.class;
            case "snappy": return SnappyCodec.class;
            case "lz4": return Lz4Codec.class;
            case "zstd": return ZStandardCodec.class;
            default: throw new IllegalArgumentException("unknown codec: " + name);
        }
    }

    public static void save(JavaRDD<String> rdd, String path, String codec) throws IOException {
        Path out = new Path(path);
        FileSystem fs = out.getFileSystem(rdd.context().hadoopConfiguration());
        fs.delete(out, true);
        Class<? extends CompressionCodec> cls = codecClass(codec);
        if (cls == null) {
            rdd.saveAsTextFile(path);
        } else {
            rdd.saveAsTextFile(path, cls);
        }
    }
}
