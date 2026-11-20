package compression.datagen;

import compression.common.Codecs;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

// Generates orders and lineitem for a scale factor (SF 1 = about 1 GB) and writes one copy per codec
// in <base>/sf<SF>/<codec>/. Each partition = 150,000 orders = one file (lineitem ~85 MB < one
// 128 MB HDFS block), because compressed text files cannot be split: like this every codec has
// the same number of tasks.
// spark-submit --class compression.datagen.Generator app.jar <sf> <base dir> <codec,codec,...>
public class Generator {

    static final long SEED = 42;
    static final long ORDERS_PER_FILE = 150_000;

    public static void main(String[] args) throws Exception {
        String sfText = args[0];
        double sf = Double.parseDouble(sfText);
        String base = args[1];
        String[] codecs = args[2].split(",");

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("generate-sf" + sfText));

        long totalOrders = Rows.orderCount(sf);
        int files = (int) ((totalOrders + ORDERS_PER_FILE - 1) / ORDERS_PER_FILE);
        List<Integer> partitionIds = new ArrayList<>();
        for (int p = 0; p < files; p++) {
            partitionIds.add(p);
        }
        JavaRDD<Integer> partitions = sc.parallelize(partitionIds, files);

        JavaRDD<String> orders = partitions.flatMap(p -> keys(p, totalOrders)
            .mapToObj(key -> Rows.order(SEED, sf, key)).iterator());
        JavaRDD<String> lineitem = partitions.flatMap(p -> keys(p, totalOrders)
            .boxed().flatMap(key -> Rows.lineItems(SEED, sf, key).stream()).iterator());

        for (String codec : codecs) {
            String dir = base + "/sf" + sfText + "/" + codec;
            Codecs.save(orders, dir + "/orders", codec);
            Codecs.save(lineitem, dir + "/lineitem", codec);
            System.out.println("written " + dir);
        }
        sc.close();
    }

    // order keys of partition p
    static LongStream keys(int p, long totalOrders) {
        long first = p * ORDERS_PER_FILE + 1;
        long last = Math.min(totalOrders, (p + 1) * ORDERS_PER_FILE);
        return LongStream.rangeClosed(first, last);
    }
}
