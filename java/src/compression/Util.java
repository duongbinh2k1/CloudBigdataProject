package compression;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.ZStandardCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Util {

    // print the app id, used by run_experiments.py
    static JavaSparkContext start(String name) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName(name));
        System.out.println("APP_ID=" + sc.sc().applicationId());
        return sc;
    }

    // write the output with the storage codec (textFile reads it back using the file extension)
    static void save(JavaRDD<String> rdd, String path, String codec) throws Exception {
        new Path(path).getFileSystem(rdd.context().hadoopConfiguration()).delete(new Path(path), true);
        switch (codec) {
            case "none": rdd.saveAsTextFile(path); break;
            case "lz4":  rdd.saveAsTextFile(path, Lz4Codec.class); break;
            case "zstd": rdd.saveAsTextFile(path, ZStandardCodec.class); break;
            default: throw new IllegalArgumentException("unknown codec: " + codec);
        }
    }

    // sha256 applied several times
    static String sha256(String text, int rounds) throws Exception {
        MessageDigest sha = MessageDigest.getInstance("SHA-256");
        String current = text;
        for (int i = 0; i < rounds; i++) {
            StringBuilder hex = new StringBuilder();
            for (byte b : sha.digest(current.getBytes(StandardCharsets.UTF_8))) {
                hex.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
            }
            current = hex.toString();
        }
        return current;
    }
}
