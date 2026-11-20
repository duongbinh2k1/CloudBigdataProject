# Experimental Evaluation of Data Compression in Spark

InfraCloud project (USTH / INP-ENSEEIHT). We measure the effect of compression
(Gzip, Snappy, LZ4, Zstd, none) in Spark at two levels:

- **storage**: text files on HDFS written with a Hadoop codec,
- **communication**: shuffle data compressed by Spark,

with three applications (I/O-bound, CPU-bound, balanced) and four dataset sizes.

Plan: [docs/PLAN.md](docs/PLAN.md)
