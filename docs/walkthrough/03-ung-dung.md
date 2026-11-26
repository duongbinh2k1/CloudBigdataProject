# 03 — Ba ứng dụng Spark và cách build

Ba ứng dụng là **thứ được đo**. Mỗi app đại diện cho một kiểu tải mà đề yêu cầu (§3): I/O-bound, CPU-bound, cân bằng. Cả ba đều:
- đọc dữ liệu từ HDFS bằng `sc.textFile` (giải nén tự động theo đuôi file → **nén lưu trữ khi đọc**);
- có ít nhất một **shuffle** (→ có dữ liệu để **nén truyền thông**);
- ghi kết quả bằng cùng codec lưu trữ (`Codecs.save` → **nén lưu trữ khi ghi**).

Codec **shuffle** không xuất hiện trong code Java: `run_experiments.py` truyền nó bằng `--conf` khi `spark-submit`. Nhờ vậy cùng một jar chạy được mọi cấu hình.

## 1. Khung chung của cả ba app

```java
public static void main(String[] args) throws Exception {
    String input = args[0];           // tham số theo vị trí, như ví dụ WordCount trong slide
    String output = args[1];
    String codec = args[2];

    JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("io_bound"));
    System.out.println("APP_ID=" + sc.sc().applicationId());
    ...
    Codecs.save(result, output, codec);
    sc.close();
}
```
- `new SparkConf()` đọc cấu hình từ `cluster/spark/spark-defaults.conf` (địa chỉ master, event log…) và các `--conf` của `spark-submit` (codec shuffle).
- `sc.sc().applicationId()`: mã ứng dụng do Spark Master cấp, dạng `app-20261126120501-0003`. App **in ra màn hình** dòng `APP_ID=...`; `run_experiments.py` đọc dòng này để biết phải hỏi History Server về ứng dụng nào.
- `sc.close()`: dừng ứng dụng, giải phóng executor, ghi nốt event log.

## 2. App 1 — `IoBound`: sắp xếp

**Bài toán:** sắp xếp toàn bộ bảng `lineitem` theo (ngày giao, mã đơn, số dòng) rồi ghi lại.

```java
static String sortKey(String line) {
    String[] f = line.split("\\|");                  // tách cột; "\\|" vì | là ký tự đặc biệt trong regex
    return f[10] + "|" + String.format("%012d", Long.parseLong(f[0])) + "|" + f[3];
}
```
- `f[10]` = ngày giao (`1996-03-13`), `f[0]` = mã đơn, `f[3]` = số dòng.
- Khoá ví dụ: `1996-03-13|000000000042|3`. `%012d` thêm số 0 phía trước cho đủ 12 chữ số, để so sánh **chuỗi** cho kết quả giống so sánh **số** (`"000000000042" < "000000000100"`, còn `"42" > "100"`).

```java
JavaRDD<String> lines = sc.textFile(input);                     // (1) đọc
JavaRDD<String> sorted = lines
    .mapToPair(line -> new Tuple2<>(sortKey(line), line))       // (2) mỗi dòng → (khoá, dòng)
    .sortByKey(true, sc.defaultParallelism())                    // (3) sắp xếp tăng dần → SHUFFLE
    .values();                                                   // (4) bỏ khoá, giữ dòng
Codecs.save(sorted, output, codec);                              // (5) ghi
```
- `Tuple2<>(a, b)`: cặp (khoá, giá trị) của Scala, cách Spark Java biểu diễn cặp.
- `sortByKey(true, n)`: sắp xếp tăng dần, kết quả chia thành n partition. `sc.defaultParallelism()` = tổng số core của các executor (mặc định).

**Spark làm gì bên trong `sortByKey`:**
1. **Job lấy mẫu**: đọc input một lượt, lấy mẫu khoá để chọn n−1 "ranh giới", sao cho mỗi khoảng có lượng dữ liệu gần bằng nhau (*range partitioning*). → input bị **đọc 2 lần**.
2. **Stage map**: đọc input lần nữa, tạo (khoá, dòng), xác định mỗi cặp thuộc khoảng nào, ghi xuống đĩa local theo từng khoảng (**shuffle write: toàn bộ dữ liệu**).
3. **Stage reduce**: mỗi task lấy khoảng của mình từ mọi máy (**shuffle read qua mạng**), sắp xếp, rồi `saveAsTextFile`.

**Vì sao là I/O-bound:** mỗi dòng chỉ tách chuỗi và ghép khoá (rất rẻ), nhưng toàn bộ dữ liệu bị đọc 2 lần, shuffle 1 lần, ghi 1 lần (×3 bản sao HDFS). Thời gian chủ yếu là đọc/ghi đĩa và truyền mạng.

## 3. App 2 — `CpuBound`: băm nhiều lần

**Bài toán:** với mỗi dòng `lineitem`, băm SHA-256 64 lần; rồi với mỗi nhóm (mã hàng % 1000), đếm số dòng và lấy giá trị băm lớn nhất.

```java
int rounds = args.length > 3 ? Integer.parseInt(args[3]) : 64;    // số vòng: tham số thứ 4, mặc định 64

JavaRDD<String> result = sc.textFile(input)
    .mapToPair(line -> {
        String[] f = line.split("\\|");
        long bucket = Long.parseLong(f[1]) % 1000;                // nhóm = mã hàng % 1000 → 1000 nhóm
        String digest = Hashing.iteratedSha256(f[0] + "|" + f[3] + "|" + f[15], rounds);   // 64 lần SHA-256
        return new Tuple2<>(bucket, new Tuple2<>(1L, Hashing.prefix(digest)));   // (nhóm, (1, số băm))
    })
    .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, Math.max(a._2, b._2)), sc.defaultParallelism())
    .map(t -> t._1 + "|" + t._2._1 + "|" + t._2._2);              // "nhóm|số dòng|max"
Codecs.save(result, output, codec);
```
- Chuỗi được băm: mã đơn | số dòng | comment, để mỗi dòng cho một kết quả khác nhau.
- `reduceByKey(hàm gộp, n)`: gộp mọi giá trị **cùng khoá** bằng hàm gộp: cộng số đếm (`a._1 + b._1`), lấy max (`Math.max(a._2, b._2)`). `._1`, `._2` là phần tử thứ nhất/thứ hai của `Tuple2`.

**Spark làm gì bên trong `reduceByKey`:** **gộp trước ở phía map** (*map-side combine*, giống **combiner** trong slide Hadoop). Mỗi task đọc khoảng 85 MB nhưng sau khi gộp chỉ còn tối đa 1000 cặp (nhóm, (đếm, max)). Shuffle chỉ gửi các cặp đã gộp → **shuffle gần như bằng 0** (vài trăm KB).

**Vì sao là CPU-bound:** mỗi dòng tốn 64 lần SHA-256 (khoảng hàng chục micro-giây), nhân với 6 triệu dòng mỗi SF. Đĩa chỉ đọc input một lần, mạng gần như không dùng. Đã đo ở dev: dữ liệu di chuyển trên mỗi giây CPU thấp nhất trong ba app.

**Vì sao 64 vòng:** chọn bằng thử nghiệm. Với 32 vòng, `CpuBound` và `Balanced` có độ "nặng CPU" quá gần nhau; 64 vòng tách ba app thành ba bậc rõ ràng. Có thể đổi bằng `rounds` trong YAML.

## 4. App 3 — `Balanced`: join hai bảng rồi tổng hợp

**Bài toán** (giống truy vấn Q3 của TPC-H): ghép mỗi dòng hàng với đơn hàng của nó, rồi với mỗi (khách hàng, độ ưu tiên) tính tổng doanh thu, số dòng, tổng số ngày trễ giao, và max một giá trị băm (8 vòng SHA-256).

```java
static long revenue(String[] f) {
    long cents = Long.parseLong(f[5].replace(".", ""));   // giá "1234.56" → 123456 cent
    long discount = Long.parseLong(f[6].substring(2));     // "0.04" → bỏ "0." → 4 (%)
    long tax = Long.parseLong(f[7].substring(2));          // "0.08" → 8 (%)
    return cents * (100 - discount) * (100 + tax);         // giá × (1 − chiết khấu) × (1 + thuế), nhân 10 000
}
```
Doanh thu = giá × (1 − chiết khấu) × (1 + thuế). Tính bằng **số nguyên** (đơn vị = 1/1 000 000 đồng) để cộng hàng triệu số mà không có sai số làm tròn, và kết quả không phụ thuộc thứ tự cộng.

```java
static long delayDays(String[] f) {
    return ChronoUnit.DAYS.between(LocalDate.parse(f[10]), LocalDate.parse(f[12]));   // ngày nhận − ngày giao
}
```

```java
int rounds = args.length > 4 ? Integer.parseInt(args[4]) : 8;
int partitions = sc.defaultParallelism();

// orderkey -> (doanh thu, số ngày trễ, comment)
JavaPairRDD<Long, Tuple3<Long, Long, String>> lines = sc.textFile(lineitemDir).mapToPair(l -> {
    String[] f = l.split("\\|");
    return new Tuple2<>(Long.parseLong(f[0]), new Tuple3<>(revenue(f), delayDays(f), f[15]));
});
// orderkey -> ("custkey|priority", comment)
JavaPairRDD<Long, Tuple2<String, String>> orders = sc.textFile(ordersDir).mapToPair(o -> {
    String[] f = o.split("\\|");
    return new Tuple2<>(Long.parseLong(f[0]), new Tuple2<>(f[1] + "|" + f[5], f[8]));
});
```
Hai bảng đều được đổi thành cặp (mã đơn, thông tin cần), để join theo mã đơn. `JavaPairRDD` là RDD của các cặp. `Tuple3` là bộ 3 phần tử.

```java
JavaRDD<String> result = lines.join(orders, partitions)            // SHUFFLE cả hai bảng
    .mapToPair(t -> {
        Tuple3<Long, Long, String> line = t._2._1;                 // phần lineitem
        Tuple2<String, String> order = t._2._2;                    // phần orders
        long prefix = Hashing.prefix(Hashing.iteratedSha256(line._3() + " " + order._2, rounds));
        // khoá mới: "custkey|priority" ; giá trị: (doanh thu, 1 dòng, số ngày trễ, max băm)
        return new Tuple2<>(order._1, new Tuple4<>(line._1(), 1L, line._2(), prefix));
    })
    .reduceByKey((a, b) -> new Tuple4<>(a._1() + b._1(), a._2() + b._2(), a._3() + b._3(),
        Math.max(a._4(), b._4())), partitions)                     // SHUFFLE nhỏ (đã gộp ở phía map)
    .map(t -> t._1 + "|" + t._2._1() + "|" + t._2._2() + "|" + t._2._3() + "|" + t._2._4());
Codecs.save(result, output, codec);
```
- `join(orders, n)`: kết quả là `(mã đơn, (giá trị lineitem, giá trị orders))`, nên `t._2._1` là phần lineitem, `t._2._2` là phần orders.
- Dòng output: `custkey|priority|tổng doanh thu|số dòng|tổng ngày trễ|max băm`.

**Spark làm gì bên trong `join`:** cả hai RDD được chia lại theo mã đơn (**hash partitioning**: partition = hash(mã đơn) % n). Mọi dòng của `lineitem` và `orders` có cùng mã đơn về cùng một task, task đó ghép chúng. → **shuffle toàn bộ cả hai bảng** (sau khi đã bỏ các cột không cần).

**Vì sao là cân bằng:** shuffle lớn (cả hai bảng) nên tốn đĩa và mạng, nhưng mỗi dòng còn phải tách chuỗi, tính doanh thu, tính ngày, băm 8 lần nên cũng tốn CPU. Ở dev: dữ liệu di chuyển trên mỗi giây CPU nằm giữa hai app kia.

## 5. So sánh ba app

| | `IoBound` | `CpuBound` | `Balanced` |
|---|---|---|---|
| Đọc | `lineitem` (2 lần: lấy mẫu + thật) | `lineitem` | `lineitem` + `orders` |
| Tính trên mỗi dòng | tách chuỗi | tách chuỗi + 64 × SHA-256 | tách chuỗi, ngày, doanh thu, 8 × SHA-256 |
| Shuffle | toàn bộ dữ liệu | gần 0 (gộp phía map) | cả hai bảng (join) + nhỏ (reduceByKey) |
| Ghi | toàn bộ dữ liệu (đã sắp xếp) | 1000 dòng | vài trăm nghìn dòng |
| Phép shuffle | `sortByKey` | `reduceByKey` | `join`, `reduceByKey` |
| Kiểu tải | I/O | CPU | cân bằng |

## 6. Build: `java/build.sh`

```bash
#!/bin/bash
set -e                                   # lệnh nào lỗi thì dừng ngay
cd "$(dirname "$0")"                     # vào thư mục java/ (dù gọi script từ đâu)
rm -rf build && mkdir -p build/classes   # xoá bản build cũ
javac --release 11 -cp "$SPARK_HOME/jars/*" -d build/classes $(find src -name '*.java')
jar cf build/cloud-bigdata-project.jar -C build/classes .
echo "built java/build/cloud-bigdata-project.jar"
```
- `javac`: trình biên dịch Java. `--release 11`: tạo bytecode cho Java 11. `-cp "$SPARK_HOME/jars/*"`: classpath = mọi jar của Spark (chứa cả thư viện Hadoop), để biên dịch được `JavaSparkContext`, `GzipCodec`… `-d build/classes`: chỗ đặt file `.class`. `$(find src -name '*.java')`: danh sách mọi file nguồn.
- `jar cf <file> -C <thư mục> .`: đóng gói mọi `.class` thành file `.jar` (giống slide: `jar cf wc.jar …`).
- Không dùng Maven/Gradle: chỉ cần JDK và Spark, đúng như cách môn học làm.
- Jar không chứa Spark (Spark có sẵn trên cluster), nên chỉ khoảng 18 KB.

## 7. Chạy một app bằng tay

Trong `make shell` (local) hoặc `make shell ENV=aws`:

```bash
cd /opt/work
bash java/build.sh
spark-submit --class compression.apps.IoBound \
    --conf spark.shuffle.compress=true --conf spark.io.compression.codec=lz4 \
    java/build/cloud-bigdata-project.jar \
    hdfs://master:9000/data/sf0.1/gzip/lineitem hdfs://master:9000/results/thu gzip
hdfs dfs -ls hdfs://master:9000/results/thu          # các file part-0000x.gz
```
- `--class`: lớp có hàm `main`.
- `--conf`: cấu hình Spark cho riêng lần chạy này (ở đây: nén shuffle bằng LZ4).
- Sau tên jar là các tham số của app (`args[0]`, `args[1]`, `args[2]`).
- Xem chi tiết lần chạy: http://localhost:18080 → chọn app → tab **Stages** (Input, Shuffle Read, Shuffle Write, thời gian từng stage).
