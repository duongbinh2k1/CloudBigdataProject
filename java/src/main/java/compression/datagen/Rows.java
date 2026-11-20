package compression.datagen;

import static compression.common.Hashing.uniform;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/*
 * Rows of the TPC-H tables orders and lineitem, as text lines separated by '|'.
 * orders:   o_orderkey|o_custkey|o_orderstatus|o_totalprice|o_orderdate|o_orderpriority|o_clerk|o_shippriority|o_comment
 * lineitem: l_orderkey|l_partkey|l_suppkey|l_linenumber|l_quantity|l_extendedprice|l_discount|l_tax|
 *           l_returnflag|l_linestatus|l_shipdate|l_commitdate|l_receiptdate|l_shipinstruct|l_shipmode|l_comment
 */
public class Rows {

    public static final long ORDERS_PER_SF = 1_500_000;
    public static final long CUSTOMERS_PER_SF = 150_000;
    public static final long PARTS_PER_SF = 200_000;
    public static final long SUPPLIERS_PER_SF = 10_000;
    public static final long CLERKS_PER_SF = 1_000;
    public static final int MAX_LINES_PER_ORDER = 7;

    static final char SEP = '|';
    private static final LocalDate START = LocalDate.parse("1992-01-01");
    private static final int ORDER_DATE_SPAN_DAYS = 2_405;     // last order date: 1998-08-02
    private static final LocalDate CUTOFF = LocalDate.parse("1995-06-17");   // "current date" in TPC-H

    private static final String[] PRIORITIES = {"1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"};
    private static final String[] SHIP_INSTRUCT = {"DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"};
    private static final String[] SHIP_MODES = {"REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"};
    private static final String[] RETURN_FLAGS = {"R", "A"};
    // words for the comment columns
    private static final String[] VOCABULARY = (
        "furiously slyly carefully blithely quickly fluffily slowly quietly ruthlessly thinly closely "
        + "doggedly daringly bravely stealthily permanently enticingly idly busily regularly finally "
        + "ironically evenly boldly silently furious sly careful blithe quick fluffy slow quiet ruthless "
        + "thin close dogged daring brave stealthy permanent enticing idle busy regular final ironic even "
        + "bold silent pending special express unusual packages requests accounts deposits foxes ideas "
        + "theodolites pinto beans instructions dependencies excuses platelets asymptotes courts dolphins "
        + "multipliers sauternes warthogs frets dinos attainments somas patterns forges braids frays "
        + "warhorses dugouts epitaphs pearls tithes waters orbits gifts sheaves depths sentiments decoys "
        + "realms pains grouches escapades sleep wake are cajole haggle nag use boost affix detect "
        + "integrate maintain nod was lose sublate solve thrash promise engage hinder print x-ray breach "
        + "eat grow impress mold poach serve run dazzle snooze doze unwind kindle play hang believe doubt "
        + "above across after against along among around at before behind beneath beside between beyond "
        + "despite during for from inside into near of on outside over past since through to toward under "
        + "until upon with within").split(" ");

    private static final String[] O_COMMENT_WORDS = salts("o_comment.w", 12);
    private static final String[] L_COMMENT_WORDS = salts("l_comment.w", 6);

    private static String[] salts(String prefix, int n) {
        String[] out = new String[n];
        for (int i = 0; i < n; i++) {
            out[i] = prefix + i;
        }
        return out;
    }

    static long scaled(long perSf, double sf) {
        return Math.max(1, Math.round(perSf * sf));
    }

    public static long orderCount(double sf) {
        return scaled(ORDERS_PER_SF, sf);
    }

    static LocalDate orderDate(long seed, long orderKey) {
        return START.plusDays(uniform(seed, "o_orderdate", orderKey, 0, ORDER_DATE_SPAN_DAYS));
    }

    public static String order(long seed, double sf, long key) {
        LocalDate date = orderDate(seed, key);
        String status = date.plusDays(121).isBefore(CUTOFF) ? "F" : date.isAfter(CUTOFF) ? "O" : "P";
        long clerk = uniform(seed, "o_clerk", key, 1, scaled(CLERKS_PER_SF, sf));
        StringBuilder sb = new StringBuilder(160);
        sb.append(key).append(SEP)
            .append(uniform(seed, "o_custkey", key, 1, scaled(CUSTOMERS_PER_SF, sf))).append(SEP)
            .append(status).append(SEP);
        money(sb, uniform(seed, "o_totalprice", key, 100_000, 50_000_000)).append(SEP)
            .append(date).append(SEP)
            .append(pick(seed, "o_orderpriority", key, PRIORITIES)).append(SEP)
            .append("Clerk#").append(String.format("%09d", clerk)).append(SEP)
            .append(0).append(SEP);
        text(sb, seed, O_COMMENT_WORDS, "o_comment.n", key, 3, 12);
        return sb.toString();
    }

    // 1 to 7 lines per order
    public static List<String> lineItems(long seed, double sf, long orderKey) {
        LocalDate orderDate = orderDate(seed, orderKey);
        int lines = (int) uniform(seed, "n_lines", orderKey, 1, MAX_LINES_PER_ORDER);
        long parts = scaled(PARTS_PER_SF, sf);
        long suppliers = scaled(SUPPLIERS_PER_SF, sf);
        List<String> out = new ArrayList<>(lines);
        for (int n = 1; n <= lines; n++) {
            long key = orderKey * 8 + n;             // unique per (order, line)
            long partKey = uniform(seed, "l_partkey", key, 1, parts);
            long quantity = uniform(seed, "l_quantity", key, 1, 50);
            long retailCents = 90_000 + (partKey / 10) % 20_001 + 100 * (partKey % 1_000);   // TPC-H formula
            LocalDate ship = orderDate.plusDays(uniform(seed, "l_shipdate", key, 1, 121));
            LocalDate commit = orderDate.plusDays(uniform(seed, "l_commitdate", key, 30, 90));
            LocalDate receipt = ship.plusDays(uniform(seed, "l_receiptdate", key, 1, 30));
            String returnFlag = receipt.isAfter(CUTOFF) ? "N" : pick(seed, "l_returnflag", key, RETURN_FLAGS);

            StringBuilder sb = new StringBuilder(160);
            sb.append(orderKey).append(SEP).append(partKey).append(SEP)
                .append(uniform(seed, "l_suppkey", key, 1, suppliers)).append(SEP)
                .append(n).append(SEP).append(quantity).append(SEP);
            money(sb, quantity * retailCents).append(SEP);
            percent(sb, uniform(seed, "l_discount", key, 0, 10)).append(SEP);
            percent(sb, uniform(seed, "l_tax", key, 0, 8)).append(SEP);
            sb.append(returnFlag).append(SEP)
                .append(ship.isAfter(CUTOFF) ? "O" : "F").append(SEP)
                .append(ship).append(SEP).append(commit).append(SEP).append(receipt).append(SEP)
                .append(pick(seed, "l_shipinstruct", key, SHIP_INSTRUCT)).append(SEP)
                .append(pick(seed, "l_shipmode", key, SHIP_MODES)).append(SEP);
            text(sb, seed, L_COMMENT_WORDS, "l_comment.n", key, 2, 6);
            out.add(sb.toString());
        }
        return out;
    }

    private static String pick(long seed, String column, long key, String[] choices) {
        return choices[(int) uniform(seed, column, key, 0, choices.length - 1)];
    }

    private static void text(StringBuilder sb, long seed, String[] wordSalts, String countSalt, long key,
                              int minWords, int maxWords) {
        int n = (int) uniform(seed, countSalt, key, minWords, maxWords);
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                sb.append(' ');
            }
            sb.append(VOCABULARY[(int) uniform(seed, wordSalts[i], key, 0, VOCABULARY.length - 1)]);
        }
    }

    // 12345 cents -> "123.45"
    private static StringBuilder money(StringBuilder sb, long cents) {
        long rest = cents % 100;
        return sb.append(cents / 100).append('.').append(rest < 10 ? "0" : "").append(rest);
    }

    // 4 -> "0.04"
    private static StringBuilder percent(StringBuilder sb, long pct) {
        return sb.append("0.").append(pct < 10 ? "0" : "").append(pct);
    }
}
