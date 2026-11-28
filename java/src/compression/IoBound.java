package compression;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

// io bound app: sort lineitem by ship date and write it back
public class IoBound {

    public static void main(String[] args) throws Exception {
        JavaSparkContext sc = Util.start("io_bound");

        JavaRDD<String> sorted = sc.textFile(args[0])
            .mapToPair(line -> new Tuple2<>(line.split("\\|")[10], line))
            .sortByKey()
            .values();

        Util.save(sorted, args[1], args[2]);
        sc.close();
    }
}
