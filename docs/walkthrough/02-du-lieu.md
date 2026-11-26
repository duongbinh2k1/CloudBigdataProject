# 02 — Dữ liệu: `Hashing`, `Rows`, `Generator`, `Codecs`

Phần này giải thích dữ liệu được tạo ra thế nào, từng dòng code. Bốn file:

| File | Vai trò |
|---|---|
| `java/src/main/java/compression/common/Hashing.java` | hàm băm cho ra số "ngẫu nhiên nhưng cố định" + SHA-256 lặp (dùng cho app) |
| `java/src/main/java/compression/datagen/Rows.java` | tạo **một** dòng `orders` hoặc các dòng `lineitem` của một đơn hàng |
| `java/src/main/java/compression/datagen/Generator.java` | job Spark tạo **cả bảng**, ghi lên HDFS, mỗi codec một bản |
| `java/src/main/java/compression/common/Codecs.java` | tên codec → lớp codec Hadoop; ghi một RDD thành file text có nén |

## 1. Vì sao tự sinh dữ liệu, và dữ liệu trông như thế nào

Đề yêu cầu nhiều kích thước dữ liệu (§5.4) và kết quả tái lập được (§2). Tự sinh thì:
- chọn kích thước tuỳ ý bằng một con số **SF** (*scale factor*): SF 1 ≈ 1 GB text, SF 20 ≈ 19 GB;
- sinh lại lúc nào cũng ra **đúng từng byte** như trước;
- dữ liệu "giống dữ liệu doanh nghiệp" (ngày tháng, mã, giá, chữ) nên nén được như dữ liệu thật. Dữ liệu ngẫu nhiên hoàn toàn thì gần như không nén được, đo nén trên đó vô nghĩa.

Mô phỏng hai bảng của **TPC-H**, bộ benchmark chuẩn cho cơ sở dữ liệu: `orders` (đơn hàng) và `lineitem` (các dòng hàng trong đơn). Mỗi dòng là text, các cột cách nhau bằng `|`.

`orders` (1 500 000 × SF dòng), 9 cột:

| # | Cột | Ý nghĩa | Ví dụ |
|---|---|---|---|
| 0 | `o_orderkey` | mã đơn (1, 2, 3, …) | `42` |
| 1 | `o_custkey` | mã khách | `118768` |
| 2 | `o_orderstatus` | F (xong), O (đang mở), P (một phần) | `F` |
| 3 | `o_totalprice` | tổng tiền | `173665.47` |
| 4 | `o_orderdate` | ngày đặt | `1996-01-02` |
| 5 | `o_orderpriority` | độ ưu tiên | `2-HIGH` |
| 6 | `o_clerk` | nhân viên | `Clerk#000000951` |
| 7 | `o_shippriority` | luôn 0 | `0` |
| 8 | `o_comment` | 3–12 từ | `furiously pending deposits …` |

`lineitem` (1–7 dòng mỗi đơn, ≈ 6 000 000 × SF dòng), 16 cột:

| # | Cột | Ý nghĩa |
|---|---|---|
| 0 | `l_orderkey` | mã đơn (trỏ về `orders`) |
| 1 | `l_partkey` | mã hàng |
| 2 | `l_suppkey` | mã nhà cung cấp |
| 3 | `l_linenumber` | số thứ tự dòng trong đơn (1..7) |
| 4 | `l_quantity` | số lượng (1..50) |
| 5 | `l_extendedprice` | giá = số lượng × đơn giá |
| 6 | `l_discount` | chiết khấu 0.00..0.10 |
| 7 | `l_tax` | thuế 0.00..0.08 |
| 8 | `l_returnflag` | R, A, N |
| 9 | `l_linestatus` | O, F |
| 10 | `l_shipdate` | ngày giao |
| 11 | `l_commitdate` | ngày cam kết |
| 12 | `l_receiptdate` | ngày nhận |
| 13 | `l_shipinstruct` | cách giao |
| 14 | `l_shipmode` | phương tiện |
| 15 | `l_comment` | 2–6 từ |

Các app dùng cột theo **số thứ tự** (`f[10]` = ngày giao…), nên bảng trên dùng để đọc code ở phần 03.

## 2. `Hashing.java`: số "ngẫu nhiên" nhưng cố định

### Ý tưởng

Nếu dùng `new Random(42)` rồi gọi `nextInt()` liên tục, giá trị của dòng thứ k phụ thuộc **thứ tự** sinh: chia việc cho 10 partition chạy song song thì thứ tự khác → dữ liệu khác. Thay vào đó, **mỗi ô** của bảng được tính từ **ba thứ cố định**: seed (42), tên cột, mã dòng. Cùng ba thứ đó → luôn cùng giá trị, bất kể ai tính, lúc nào, ở partition nào.

### Từng dòng

```java
static long mix(long z) {
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
    return z ^ (z >>> 31);
}
```
- Hàm trộn **SplitMix64** (một công thức nổi tiếng, có sẵn trong thư viện chuẩn Java `SplittableRandom`): lấy một số 64 bit, trộn các bit của nó.
- `z >>> 30`: dịch phải 30 bit (không giữ dấu). `^`: XOR. `* hằng số lẻ lớn`: nhân, tràn số 64 bit là bình thường.
- Đầu vào chỉ khác 1 bit thì đầu ra khác khoảng một nửa số bit → trông như ngẫu nhiên.

```java
public static long hash(long seed, String column, long key) {
    long h = mix(seed ^ 0x9e3779b97f4a7c15L);
    h = mix(h ^ column.hashCode());
    h = mix(h ^ key);
    return h >>> 1;
}
```
- Trộn lần lượt seed, mã băm của tên cột (`"o_custkey".hashCode()`), rồi khoá dòng.
- `0x9e3779b97f4a7c15`: hằng số "tỉ lệ vàng" của SplitMix64, để seed 0 cũng cho kết quả tốt.
- `>>> 1`: bỏ bit dấu → luôn là số **không âm**.
- Tên cột tham gia vào hàm băm, nên `o_custkey` và `o_totalprice` của cùng một đơn là hai số độc lập.

```java
public static long uniform(long seed, String column, long key, long low, long high) {
    return low + Math.floorMod(hash(seed, column, key), high - low + 1);
}
```
- Đưa số băm về khoảng `[low, high]` (kể cả hai đầu): chia lấy dư cho số giá trị có thể, rồi cộng `low`.
- Ví dụ `uniform(42, "l_quantity", k, 1, 50)` → một số từ 1 đến 50.

```java
public static String iteratedSha256(String value, int rounds) throws Exception {
    MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
    String current = value;
    for (int i = 0; i < rounds; i++) {
        byte[] digest = sha256.digest(current.getBytes(StandardCharsets.UTF_8));
        StringBuilder hex = new StringBuilder();
        for (byte b : digest) {
            hex.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
        }
        current = hex.toString();
    }
    return current;
}
```
- **Không dùng để sinh dữ liệu**, mà dùng trong app `CpuBound` và `Balanced` để tạo **việc cho CPU**.
- SHA-256 (hàm băm mật mã, trong thư viện chuẩn Java) băm chuỗi thành 32 byte; đổi 32 byte thành 64 ký tự hex (`(b >> 4) & 0xf` = 4 bit cao, `b & 0xf` = 4 bit thấp); rồi băm tiếp chuỗi hex đó. Lặp `rounds` lần (64 ở `CpuBound`, 8 ở `Balanced`).
- Kết quả không có ý nghĩa nghiệp vụ; mục đích là bắt CPU làm việc thật và đo được.

```java
public static long prefix(String hexDigest) {
    return Long.parseLong(hexDigest.substring(0, 15), 16);
}
```
- Lấy 15 ký tự hex đầu (15 × 4 = 60 bit, vừa một `long`) đổi thành số. Các app lấy **max** của số này: so sánh số rẻ hơn so sánh chuỗi.

## 3. `Rows.java`: tạo một dòng

### Hằng số

```java
public static final long ORDERS_PER_SF = 1_500_000;     // số đơn hàng với SF 1
public static final long CUSTOMERS_PER_SF = 150_000;    // số khách hàng với SF 1
public static final long PARTS_PER_SF = 200_000;        // số mặt hàng
public static final long SUPPLIERS_PER_SF = 10_000;     // số nhà cung cấp
public static final long CLERKS_PER_SF = 1_000;         // số nhân viên
public static final int MAX_LINES_PER_ORDER = 7;
```
Theo đúng tỉ lệ của TPC-H. `1_500_000` là cách viết số có dấu gạch dưới cho dễ đọc của Java.

```java
private static final LocalDate START = LocalDate.parse("1992-01-01");
private static final int ORDER_DATE_SPAN_DAYS = 2_405;     // ngày đặt cuối: 1998-08-02
private static final LocalDate CUTOFF = LocalDate.parse("1995-06-17");   // "ngày hiện tại" của TPC-H
```
Ngày đặt hàng nằm trong 1992–1998. `CUTOFF` dùng để quyết định trạng thái (đã xong hay chưa).

```java
private static final String[] PRIORITIES = {"1-URGENT", "2-HIGH", ...};
private static final String[] SHIP_INSTRUCT = {...};
private static final String[] SHIP_MODES = {...};
private static final String[] RETURN_FLAGS = {"R", "A"};
private static final String[] VOCABULARY = ("furiously slyly carefully ...").split(" ");
```
Các danh sách giá trị có thể có. Cột chỉ có vài giá trị lặp lại (độ ưu tiên, phương tiện) và comment ghép từ một bộ từ cố định: đó là lý do dữ liệu **nén được** như dữ liệu thật.

```java
private static final String[] O_COMMENT_WORDS = salts("o_comment.w", 12);   // "o_comment.w0" … "o_comment.w11"
private static final String[] L_COMMENT_WORDS = salts("l_comment.w", 6);
```
Tên "cột" riêng cho **từng vị trí từ** trong comment, để từ thứ 1 và từ thứ 2 là hai lựa chọn độc lập.

```java
static long scaled(long perSf, double sf) {
    return Math.max(1, Math.round(perSf * sf));
}
public static long orderCount(double sf) {
    return scaled(ORDERS_PER_SF, sf);       // SF 0.1 → 150 000 đơn ; SF 20 → 30 000 000 đơn
}
```

```java
static LocalDate orderDate(long seed, long orderKey) {
    return START.plusDays(uniform(seed, "o_orderdate", orderKey, 0, ORDER_DATE_SPAN_DAYS));
}
```
Ngày đặt của đơn k = 1992-01-01 + (số từ 0 đến 2405). Hàm này dùng ở **cả hai bảng** (xem dưới).

### `order(seed, sf, key)`: một dòng `orders`

```java
LocalDate date = orderDate(seed, key);
String status = date.plusDays(121).isBefore(CUTOFF) ? "F" : date.isAfter(CUTOFF) ? "O" : "P";
```
Trạng thái theo ngày: đặt trước CUTOFF hơn 121 ngày → đã xong (F); đặt sau CUTOFF → còn mở (O); ở giữa → một phần (P).

```java
long clerk = uniform(seed, "o_clerk", key, 1, scaled(CLERKS_PER_SF, sf));
StringBuilder sb = new StringBuilder(160);           // một dòng khoảng 120 ký tự
sb.append(key).append(SEP)                                                         // cột 0: mã đơn
  .append(uniform(seed, "o_custkey", key, 1, scaled(CUSTOMERS_PER_SF, sf))).append(SEP) // cột 1: mã khách
  .append(status).append(SEP);                                                     // cột 2
money(sb, uniform(seed, "o_totalprice", key, 100_000, 50_000_000)).append(SEP)     // cột 3: 1000.00 .. 500000.00
  .append(date).append(SEP)                                                        // cột 4: yyyy-mm-dd
  .append(pick(seed, "o_orderpriority", key, PRIORITIES)).append(SEP)              // cột 5
  .append("Clerk#").append(String.format("%09d", clerk)).append(SEP)               // cột 6: Clerk#000000951
  .append(0).append(SEP);                                                          // cột 7
text(sb, seed, O_COMMENT_WORDS, "o_comment.n", key, 3, 12);                        // cột 8: 3..12 từ
return sb.toString();
```
`StringBuilder` nối chuỗi nhanh hơn dùng `+` nhiều lần.

### `lineItems(seed, sf, orderKey)`: các dòng `lineitem` của một đơn

```java
LocalDate orderDate = orderDate(seed, orderKey);   // CÙNG hàm với bảng orders → khớp nhau, không cần join
int lines = (int) uniform(seed, "n_lines", orderKey, 1, MAX_LINES_PER_ORDER);   // 1..7 dòng
long parts = scaled(PARTS_PER_SF, sf);
long suppliers = scaled(SUPPLIERS_PER_SF, sf);
List<String> out = new ArrayList<>(lines);
for (int n = 1; n <= lines; n++) {
    long key = orderKey * 8 + n;             // khoá riêng cho (đơn, dòng): đơn 42 dòng 3 → 339
```
Mỗi đơn có tối đa 7 dòng, nên `orderKey * 8 + n` không bao giờ trùng giữa hai đơn.

```java
    long partKey = uniform(seed, "l_partkey", key, 1, parts);
    long quantity = uniform(seed, "l_quantity", key, 1, 50);
    long retailCents = 90_000 + (partKey / 10) % 20_001 + 100 * (partKey % 1_000);   // công thức TPC-H
```
Đơn giá (tính bằng cent) theo công thức của TPC-H: cùng mặt hàng luôn cùng đơn giá.

```java
    LocalDate ship = orderDate.plusDays(uniform(seed, "l_shipdate", key, 1, 121));      // giao: 1..121 ngày sau khi đặt
    LocalDate commit = orderDate.plusDays(uniform(seed, "l_commitdate", key, 30, 90));  // cam kết: 30..90 ngày sau
    LocalDate receipt = ship.plusDays(uniform(seed, "l_receiptdate", key, 1, 30));      // nhận: 1..30 ngày sau khi giao
    String returnFlag = receipt.isAfter(CUTOFF) ? "N" : pick(seed, "l_returnflag", key, RETURN_FLAGS);
```

```java
    sb.append(orderKey).append(SEP).append(partKey).append(SEP)       // cột 0, 1
      .append(uniform(seed, "l_suppkey", key, 1, suppliers)).append(SEP)  // cột 2
      .append(n).append(SEP).append(quantity).append(SEP);            // cột 3, 4
    money(sb, quantity * retailCents).append(SEP);                    // cột 5: giá = số lượng × đơn giá
    percent(sb, uniform(seed, "l_discount", key, 0, 10)).append(SEP); // cột 6: 0.00..0.10
    percent(sb, uniform(seed, "l_tax", key, 0, 8)).append(SEP);       // cột 7: 0.00..0.08
    sb.append(returnFlag).append(SEP)                                 // cột 8
      .append(ship.isAfter(CUTOFF) ? "O" : "F").append(SEP)           // cột 9
      .append(ship).append(SEP).append(commit).append(SEP).append(receipt).append(SEP)   // cột 10, 11, 12
      .append(pick(seed, "l_shipinstruct", key, SHIP_INSTRUCT)).append(SEP)              // cột 13
      .append(pick(seed, "l_shipmode", key, SHIP_MODES)).append(SEP);                    // cột 14
    text(sb, seed, L_COMMENT_WORDS, "l_comment.n", key, 2, 6);        // cột 15: 2..6 từ
    out.add(sb.toString());
}
return out;
```

### Các hàm phụ

```java
private static String pick(long seed, String column, long key, String[] choices) {
    return choices[(int) uniform(seed, column, key, 0, choices.length - 1)];   // chọn 1 phần tử trong danh sách
}
private static void text(StringBuilder sb, long seed, String[] wordSalts, String countSalt, long key,
                         int minWords, int maxWords) {
    int n = (int) uniform(seed, countSalt, key, minWords, maxWords);   // số từ
    for (int i = 0; i < n; i++) {
        if (i > 0) sb.append(' ');
        sb.append(VOCABULARY[(int) uniform(seed, wordSalts[i], key, 0, VOCABULARY.length - 1)]);  // từ thứ i
    }
}
private static StringBuilder money(StringBuilder sb, long cents) {   // 12345 → "123.45", 12305 → "123.05"
    long rest = cents % 100;
    return sb.append(cents / 100).append('.').append(rest < 10 ? "0" : "").append(rest);
}
private static StringBuilder percent(StringBuilder sb, long pct) {   // 4 → "0.04", 10 → "0.10"
    return sb.append("0.").append(pct < 10 ? "0" : "").append(pct);
}
```
Tiền lưu bằng **cent (số nguyên)** rồi mới định dạng, để không có sai số làm tròn của số thực.

## 4. `Generator.java`: sinh cả bảng bằng Spark

Chạy: `spark-submit --class compression.datagen.Generator java/build/cloud-bigdata-project.jar 1 hdfs://master:9000/data none,gzip,snappy,lz4,zstd`

```java
static final long SEED = 42;                  // seed cố định → dữ liệu cố định
static final long ORDERS_PER_FILE = 150_000;  // số đơn mỗi file
```

**Vì sao 150 000 đơn mỗi file?** 150 000 đơn ≈ 600 000 dòng lineitem ≈ **85 MB** text, nhỏ hơn 1 block HDFS (128 MB).
- File nén (gzip…) không chia nhỏ được → 1 file = 1 task đọc.
- File không nén < 1 block → cũng 1 file = 1 task đọc.
- → **mọi codec có cùng số task**, nên khi so sánh, chỉ có nén thay đổi chứ không phải mức song song. Nếu dùng 1 file lớn, bản không nén sẽ được đọc song song bởi nhiều task, còn bản gzip chỉ 1 task: so sánh sai.

```java
String sfText = args[0];                  // "1" — giữ nguyên dạng chữ để đặt tên thư mục sf1, sf0.1
double sf = Double.parseDouble(sfText);
String base = args[1];                    // hdfs://master:9000/data
String[] codecs = args[2].split(",");     // ["none", "gzip", ...]

JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("generate-sf" + sfText));
```
`JavaSparkContext`: điểm vào của Spark trong Java. `setAppName`: tên hiện trên giao diện Spark. Địa chỉ master (`spark://master:7077`) lấy từ `cluster/spark/spark-defaults.conf`.

```java
long totalOrders = Rows.orderCount(sf);                                     // SF 1 → 1 500 000
int files = (int) ((totalOrders + ORDERS_PER_FILE - 1) / ORDERS_PER_FILE);  // chia làm tròn lên → 10 file
List<Integer> partitionIds = new ArrayList<>();
for (int p = 0; p < files; p++) partitionIds.add(p);                        // [0, 1, …, 9]
JavaRDD<Integer> partitions = sc.parallelize(partitionIds, files);         // RDD 10 partition, mỗi partition 1 số
```
`parallelize(list, n)` biến một danh sách trong driver thành RDD có n partition. Ở đây mỗi partition chỉ chứa **số thứ tự của chính nó**; dữ liệu thật được sinh ngay trong executor ở bước sau, không phải gửi từ driver.

```java
JavaRDD<String> orders = partitions.flatMap(p -> keys(p, totalOrders)
    .mapToObj(key -> Rows.order(SEED, sf, key)).iterator());
```
- `flatMap`: mỗi phần tử (số partition p) → nhiều phần tử (các dòng).
- `keys(p, …)`: dãy mã đơn của partition p, ví dụ p = 0 → 1..150 000; p = 1 → 150 001..300 000.
- `.mapToObj(key -> Rows.order(...))`: mỗi mã → một dòng text.
- `.iterator()`: trả về **iterator**, tức các dòng được sinh **lần lượt khi Spark cần ghi**, không giữ 150 000 dòng trong RAM cùng lúc.

```java
JavaRDD<String> lineitem = partitions.flatMap(p -> keys(p, totalOrders)
    .boxed().flatMap(key -> Rows.lineItems(SEED, sf, key).stream()).iterator());
```
Giống trên, nhưng mỗi đơn cho 1–7 dòng: `.boxed()` đổi `LongStream` thành `Stream<Long>` để dùng được `flatMap` của Java Stream.

```java
for (String codec : codecs) {
    String dir = base + "/sf" + sfText + "/" + codec;        // hdfs://master:9000/data/sf1/gzip
    Codecs.save(orders, dir + "/orders", codec);             // → .../orders/part-00000.gz … part-00009.gz
    Codecs.save(lineitem, dir + "/lineitem", codec);
    System.out.println("written " + dir);                    // run_all.sh / make status thấy dòng này
}
sc.close();
```
Mỗi codec, Spark **sinh lại** các dòng rồi ghi (RDD không được lưu tạm). Nhờ hàm băm tất định nên lần sinh nào cũng ra cùng dữ liệu: **mọi bản (none, gzip, …) chứa đúng cùng dữ liệu**, chỉ khác cách nén.

```java
static LongStream keys(int p, long totalOrders) {
    long first = p * ORDERS_PER_FILE + 1;
    long last = Math.min(totalOrders, (p + 1) * ORDERS_PER_FILE);   // partition cuối có thể ít hơn
    return LongStream.rangeClosed(first, last);
}
```

**Kết quả trên HDFS** (SF 1):
```
hdfs://master:9000/data/sf1/
  none/orders/part-00000 … part-00009        none/lineitem/part-00000 … part-00009
  gzip/orders/part-00000.gz …                gzip/lineitem/part-00000.gz …
  snappy/…/part-0000x.snappy   lz4/…/part-0000x.lz4   zstd/…/part-0000x.zst
```
Sau đó `run_all.sh` chạy `hdfs dfs -du -s "hdfs://master:9000/data/sf1/*/*"` và lưu vào `results/<env>/datasets/sf1.txt`, mỗi dòng: `<kích thước 1 bản> <kích thước gồm mọi bản sao> <đường dẫn>`. Đây là nguồn số liệu cho **tỉ lệ nén lưu trữ**.

## 5. `Codecs.java`: ghi file có nén

```java
public static Class<? extends CompressionCodec> codecClass(String name) {
    switch (name) {
        case "none": return null;
        case "gzip": return GzipCodec.class;        // java.util.zip của JDK
        case "snappy": return SnappyCodec.class;    // thư viện snappy-java (có sẵn trong Spark)
        case "lz4": return Lz4Codec.class;          // thư viện lz4-java (có sẵn trong Spark)
        case "zstd": return ZStandardCodec.class;   // thư viện native libhadoop.so + libzstd
        default: throw new IllegalArgumentException("unknown codec: " + name);
    }
}
```
Đổi tên codec trong YAML thành **lớp Java của Hadoop** làm việc nén.

```java
public static void save(JavaRDD<String> rdd, String path, String codec) throws IOException {
    Path out = new Path(path);
    FileSystem fs = out.getFileSystem(rdd.context().hadoopConfiguration());
    fs.delete(out, true);                       // xoá thư mục cũ nếu có (true = xoá cả bên trong)
    Class<? extends CompressionCodec> cls = codecClass(codec);
    if (cls == null) {
        rdd.saveAsTextFile(path);               // không nén
    } else {
        rdd.saveAsTextFile(path, cls);          // nén: mỗi partition → một file part-NNNNN.<đuôi>
    }
}
```
- `saveAsTextFile` báo lỗi nếu thư mục đã tồn tại, nên phải xoá trước để chạy lại được.
- Mỗi partition ghi thành một file; đuôi file do codec quyết định (`.gz`, `.snappy`, `.lz4`, `.zst`).
- **Khi đọc**, `sc.textFile(...)` nhìn đuôi file rồi tự chọn codec giải nén (cơ chế `CompressionCodecFactory` của Hadoop). Vì vậy **app không cần biết dữ liệu nén bằng gì**: chỉ cần trỏ vào đúng thư mục.

**zstd đặc biệt ở chỗ nào?** `ZStandardCodec` không viết bằng Java mà gọi thư viện C (`libhadoop.so` + `libzstd`). Bản Hadoop chính thức chỉ có `libhadoop.so` cho Linux x86_64. Hai dòng `spark.driver.extraLibraryPath` / `spark.executor.extraLibraryPath` trong `cluster/spark/spark-defaults.conf` chỉ cho Spark chỗ tìm thư viện; gói `libzstd1` do `install.sh` cài. Trên Mac (ARM) thư viện không tải được, nên YAML của local không có zstd lưu trữ.
