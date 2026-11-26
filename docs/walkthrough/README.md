# Đọc hiểu toàn bộ project, từng dòng code

Đọc theo thứ tự: đề bài trước, rồi thiết kế, rồi code.

| # | File | Nội dung |
|---|---|---|
| 0 | [00-de-bai.md](00-de-bai.md) | đề bài từng mục (§1–§7): đề nói gì, nghĩa là phải làm gì, project đáp ứng ở đâu; bảng kiểm tra yêu cầu (gồm chỗ còn thiếu) |
| 1 | [01-tong-quan.md](01-tong-quan.md) | câu hỏi của project, khái niệm nền (HDFS, Spark, shuffle, nén), kiến trúc, toàn bộ quy trình, một lần đo đi qua những gì |
| 2 | [02-du-lieu.md](02-du-lieu.md) | dữ liệu TPC-H: `Hashing`, `Rows`, `Generator`, `Codecs` |
| 3 | [03-ung-dung.md](03-ung-dung.md) | ba ứng dụng Spark (`IoBound`, `CpuBound`, `Balanced`), Spark làm gì bên trong, `build.sh` |
| 4 | [04-ha-tang.md](04-ha-tang.md) | `cluster/` (cài đặt, cấu hình, khởi động), cluster local `dev/`, Terraform, Ansible, `Makefile` |
| 5 | [05-do-luong.md](05-do-luong.md) | `run_all.sh`, `run_experiments.py`, `metrics.py` (History Server, `sar`), micro-benchmark |
| 6 | [06-phan-tich.md](06-phan-tich.md) | `report.py`: mỗi phép tính, mỗi đồ thị, mỗi bảng; 7 câu hỏi |

Tóm tắt và câu hỏi bảo vệ: [../GUIDE.md](../GUIDE.md), [../QA.md](../QA.md).
