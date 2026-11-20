package compression.apps;

import compression.common.Codecs;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

// App 1 (I/O-bound): sort lineitem by ship date. All the data is read, shuffled and written.
// spark-submit --class compression.apps.IoBound app.jar <lineitem dir> <output dir> <codec>
public class IoBound {

    // ship date | order key | line number, e.g. "1996-03-13|000000000042|3"
    static String sortKey(String line) {
        String[] f = line.split("\\|");
        return f[10] + "|" + String.format("%012d", Long.parseLong(f[0])) + "|" + f[3];
    }

    public static void main(String[] args) throws Exception {
        String input = args[0];
        String output = args[1];
        String codec = args[2];

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("io_bound"));
        System.out.println("APP_ID=" + sc.sc().applicationId());

        JavaRDD<String> lines = sc.textFile(input);
        JavaRDD<String> sorted = lines
            .mapToPair(line -> new Tuple2<>(sortKey(line), line))
            .sortByKey(true, sc.defaultParallelism())
            .values();

        Codecs.save(sorted, output, codec);
        sc.close();
    }
}
