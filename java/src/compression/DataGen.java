package compression;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

// Generates the two tables of the TPC-H benchmark used by the apps, as text (columns separated by '|'):
//   orders   (1.5 million rows per SF): orderkey|custkey|status|totalprice|orderdate|priority|clerk|shippriority|comment
//   lineitem (1 to 7 rows per order):  orderkey|partkey|suppkey|linenumber|quantity|price|discount|tax|
//                                       returnflag|linestatus|shipdate|commitdate|receiptdate|shipinstruct|shipmode|comment
// SF 1 = about 1 GB of text. One copy per storage codec in <base>/sf<SF>/<codec>/{orders,lineitem}.
//
// Every order has its own random generator, seeded with the order key: the data is always the same,
// whatever the number of machines. 150,000 orders per file: each lineitem file (~75 MB) is smaller than
// one HDFS block, so every codec gives the same number of tasks (compressed text files cannot be split).
//
// spark-submit --class compression.DataGen app.jar <sf> <base dir> <codec,codec,...>
public class DataGen {

    static final long ORDERS_PER_SF = 1_500_000;
    static final long ORDERS_PER_FILE = 150_000;
    static final LocalDate START = LocalDate.parse("1992-01-01");
    static final LocalDate CUTOFF = LocalDate.parse("1995-06-17");   // the "current date" of TPC-H
    static final String[] PRIORITIES = {"1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"};
    static final String[] INSTRUCTIONS = {"DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"};
    static final String[] MODES = {"REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"};
    static final String[] FLAGS = {"R", "A"};
    static final String[] WORDS = ("furiously slyly carefully blithely quickly fluffily slowly quietly ruthlessly "
        + "thinly closely doggedly daringly bravely stealthily permanently enticingly idly busily regularly finally "
        + "ironically evenly boldly silently pending special express unusual regular final ironic even bold silent "
        + "packages requests accounts deposits foxes ideas theodolites pinto beans instructions dependencies excuses "
        + "platelets asymptotes courts dolphins multipliers sauternes warthogs frets dinos attainments somas patterns "
        + "sleep wake are cajole haggle nag use boost affix detect integrate maintain nod was lose solve thrash "
        + "promise engage hinder print breach eat grow impress mold poach serve run dazzle snooze doze unwind "
        + "above across after against along among around at before behind beneath beside between beyond").split(" ");

    public static void main(String[] args) throws Exception {
        double sf = Double.parseDouble(args[0]);
        String base = args[1] + "/sf" + args[0];
        long orders = Math.round(ORDERS_PER_SF * sf);
        int files = (int) ((orders + ORDERS_PER_FILE - 1) / ORDERS_PER_FILE);

        JavaSparkContext sc = Util.start("datagen-sf" + args[0]);
        List<Integer> fileNumbers = new ArrayList<>();
        for (int i = 0; i < files; i++) {
            fileNumbers.add(i);
        }
        JavaRDD<Integer> perFile = sc.parallelize(fileNumbers, files);   // one task = one file
        JavaRDD<String> orderRows = perFile.flatMap(i -> ordersOfFile(i, orders).iterator());
        JavaRDD<String> lineRows = perFile.flatMap(i -> lineItemsOfFile(i, orders).iterator());

        for (String codec : args[2].split(",")) {
            Util.save(orderRows, base + "/" + codec + "/orders", codec);
            Util.save(lineRows, base + "/" + codec + "/lineitem", codec);
            System.out.println("written " + base + "/" + codec);
        }
        sc.close();
    }

    // rows of the orders table in file i (order keys i * 150,000 + 1 ... (i + 1) * 150,000)
    static List<String> ordersOfFile(int i, long orders) {
        List<String> rows = new ArrayList<>();
        for (long key = i * ORDERS_PER_FILE + 1; key <= Math.min(orders, (i + 1) * ORDERS_PER_FILE); key++) {
            rows.add(order(key));
        }
        return rows;
    }

    static List<String> lineItemsOfFile(int i, long orders) {
        List<String> rows = new ArrayList<>();
        for (long key = i * ORDERS_PER_FILE + 1; key <= Math.min(orders, (i + 1) * ORDERS_PER_FILE); key++) {
            rows.addAll(lineItems(key));
        }
        return rows;
    }

    static String order(long key) {
        SplittableRandom r = new SplittableRandom(key);
        LocalDate date = START.plusDays(r.nextInt(2405));      // first draw: the same in lineItems()
        String status = "P";
        if (date.plusDays(121).isBefore(CUTOFF)) {
            status = "F";
        } else if (date.isAfter(CUTOFF)) {
            status = "O";
        }
        String customer = "" + (1 + r.nextInt(150_000));
        String price = money(r.nextInt(100_000, 50_000_000));
        String priority = pick(r, PRIORITIES);
        String clerk = "Clerk#" + String.format("%09d", 1 + r.nextInt(1_000));
        return key + "|" + customer + "|" + status + "|" + price + "|" + date + "|" + priority + "|" + clerk
            + "|0|" + comment(r, 3, 12);
    }

    static List<String> lineItems(long key) {
        SplittableRandom r = new SplittableRandom(key);
        LocalDate orderDate = START.plusDays(r.nextInt(2405));
        int count = 1 + r.nextInt(7);
        List<String> rows = new ArrayList<>();
        for (int n = 1; n <= count; n++) {
            int quantity = 1 + r.nextInt(50);
            LocalDate ship = orderDate.plusDays(1 + r.nextInt(121));
            LocalDate commit = orderDate.plusDays(30 + r.nextInt(61));
            LocalDate receipt = ship.plusDays(1 + r.nextInt(30));
            int part = 1 + r.nextInt(200_000);
            int supplier = 1 + r.nextInt(10_000);
            String price = money(quantity * (long) r.nextInt(90_000, 200_000));
            String discount = "0.0" + r.nextInt(10);
            String tax = "0.0" + r.nextInt(9);
            String returnFlag = "N";
            if (!receipt.isAfter(CUTOFF)) {
                returnFlag = pick(r, FLAGS);
            }
            String status = "F";
            if (ship.isAfter(CUTOFF)) {
                status = "O";
            }
            rows.add(key + "|" + part + "|" + supplier + "|" + n + "|" + quantity + "|" + price + "|" + discount + "|"
                + tax + "|" + returnFlag + "|" + status + "|" + ship + "|" + commit + "|" + receipt + "|"
                + pick(r, INSTRUCTIONS) + "|" + pick(r, MODES) + "|" + comment(r, 2, 6));
        }
        return rows;
    }

    static String pick(SplittableRandom r, String[] choices) {
        return choices[r.nextInt(choices.length)];
    }

    // between min and max words of the vocabulary
    static String comment(SplittableRandom r, int min, int max) {
        int n = min + r.nextInt(max - min + 1);
        String text = WORDS[r.nextInt(WORDS.length)];
        for (int i = 1; i < n; i++) {
            text = text + " " + WORDS[r.nextInt(WORDS.length)];
        }
        return text;
    }

    // 123456 cents -> "1234.56"
    static String money(long cents) {
        if (cents % 100 < 10) {
            return cents / 100 + ".0" + cents % 100;
        }
        return cents / 100 + "." + cents % 100;
    }
}
