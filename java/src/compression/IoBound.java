package compression;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

// App 1, I/O-bound: sorts lineitem by ship date (column 10).
// All the data is read, shuffled and written again, with almost no computation.
// spark-submit --class compression.IoBound app.jar <lineitem dir> <output dir> <codec>
public class IoBound {

    public static void main(String[] args) throws Exception {
        JavaSparkContext sc = Util.start("io_bound");

        JavaRDD<String> sorted = sc.textFile(args[0])
            .mapToPair(line -> new Tuple2<>(line.split("\\|")[10], line))
            .sortByKey()                                   // shuffle of the whole table
            .values();

        Util.save(sorted, args[1], args[2]);
        sc.close();
    }
}
