package compression;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

// App 2, CPU-bound: SHA-256 applied 64 times to every line of lineitem, then the largest digest per
// ship mode (column 14). reduceByKey combines on the map side, so almost nothing is shuffled.
// spark-submit --class compression.CpuBound app.jar <lineitem dir> <output dir> <codec>
public class CpuBound {

    static final int ROUNDS = 64;

    public static void main(String[] args) throws Exception {
        JavaSparkContext sc = Util.start("cpu_bound");

        JavaRDD<String> result = sc.textFile(args[0])
            .mapToPair(line -> new Tuple2<>(line.split("\\|")[14], Util.sha256(line, ROUNDS)))
            .reduceByKey((a, b) -> a.compareTo(b) > 0 ? a : b)    // 7 keys: tiny shuffle
            .map(t -> t._1 + "|" + t._2);

        Util.save(result, args[1], args[2]);
        sc.close();
    }
}
