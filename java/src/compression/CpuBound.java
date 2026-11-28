package compression;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

// cpu bound app: sha256 (64 rounds) on each line, max hash per ship mode
public class CpuBound {

    static final int ROUNDS = 64;

    public static void main(String[] args) throws Exception {
        JavaSparkContext sc = Util.start("cpu_bound");

        JavaRDD<String> result = sc.textFile(args[0])
            .mapToPair(line -> new Tuple2<>(line.split("\\|")[14], Util.sha256(line, ROUNDS)))
            .reduceByKey((a, b) -> a.compareTo(b) > 0 ? a : b)
            .map(t -> t._1 + "|" + t._2);

        Util.save(result, args[1], args[2]);
        sc.close();
    }
}
