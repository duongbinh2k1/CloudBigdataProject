package compression.apps;

import compression.common.Codecs;
import compression.common.Hashing;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

// App 2 (CPU-bound): SHA-256 64 times on each line, then count and max per bucket (partkey % 1000).
// reduceByKey combines on the map side, so almost nothing is shuffled.
// spark-submit --class compression.apps.CpuBound app.jar <lineitem dir> <output dir> <codec> [rounds]
public class CpuBound {

    public static void main(String[] args) throws Exception {
        String input = args[0];
        String output = args[1];
        String codec = args[2];
        int rounds = args.length > 3 ? Integer.parseInt(args[3]) : 64;

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("cpu_bound"));
        System.out.println("APP_ID=" + sc.sc().applicationId());

        JavaRDD<String> lines = sc.textFile(input);
        JavaRDD<String> result = lines
            .mapToPair(line -> {
                String[] f = line.split("\\|");
                long bucket = Long.parseLong(f[1]) % 1000;
                String digest = Hashing.iteratedSha256(f[0] + "|" + f[3] + "|" + f[15], rounds);
                return new Tuple2<>(bucket, new Tuple2<>(1L, Hashing.prefix(digest)));
            })
            .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, Math.max(a._2, b._2)), sc.defaultParallelism())
            .map(t -> t._1 + "|" + t._2._1 + "|" + t._2._2);

        Codecs.save(result, output, codec);
        sc.close();
    }
}
