Nhìn vào hai file mẫu bạn cung cấp, đây không đơn thuần là một bài toán **"Find & Replace"** chuỗi văn bản. Đây là một quá trình **"Template-based Refactoring"** (Cấu trúc lại mã nguồn dựa trên khuôn mẫu).

Để đạt được kết quả từ bản gốc (Hive SQL) sang bản đích (PySpark), tool của bạn cần thực hiện 5 khối công việc kỹ thuật trọng yếu sau:

---

### 1. Phân tích và Tách khối (SQL Parsing & Segmentation)
File gốc là một tập hợp các câu lệnh SQL liên tiếp. Tool phải nhận diện được đâu là ranh giới giữa các lệnh.
* **Thách thức:** Phải phân biệt được dấu `;` kết thúc câu lệnh và dấu `;` nằm trong chuỗi hoặc comment.
* **Xử lý Comment:** Bạn yêu cầu đưa comment ra ngoài `spark.sql`. Tool cần "bóc" các dòng bắt đầu bằng `--` và chuyển thành `#` hoặc `##` trong Python trước khi viết lệnh `spark.sql` tiếp theo.

### 2. Quản lý và Ánh xạ Biến (Variable Mapping)
Đây là phần phức tạp nhất vì nguồn gốc của biến bị thay đổi:
* **Hive:** Tất cả là `${var_name}`.
* **Spark:** Một số biến lấy từ `params["..."]` (như `raw_schema`), nhưng một số lại là biến Python trực tiếp (như `batch_date` lấy từ hàm `run_etl`).
* **Task:** Bạn cần một bảng tra cứu (Mapping Table) trong file YAML để biết: "Nếu gặp `${batch_date}` trong SQL, hãy đổi thành `{batch_date}` trong f-string Python".

### 3. Thay thế Hàm và Logic Đặc thù (Function & Logic Substitution)
Có những logic trong Hive không tồn tại hoặc được xử lý khác trong Spark:
* **Biến hệ thống:** `${batch_timestamp}` trong Hive được thay bằng hàm `current_timestamp()` của Spark SQL.
* **Logic rẽ nhánh:** Đoạn `alter table ... drop if exists partition` được thay thế bằng một hàm helper custom là `drop_partition_day(...)`. Tool của bạn phải đủ thông minh để nhận diện pattern "Drop Partition" và thay bằng lời gọi hàm tương ứng thay vì giữ nguyên câu SQL.

### 4. Boilerplate & Wrapper Generation
Tool phải tự sinh ra phần "khung" (Boilerplate) mà file SQL không có:
* Phần header: `import sys`, `from etl_common_function import ...`.
* Phần khởi tạo: Định nghĩa `source_name`, `table_name` dựa trên tên file.
* Phần kết thúc: `batch_end(...)`, `spark.stop()`.

### 5. Cấu trúc lại Đường dẫn (Path Refactoring)
Hãy nhìn vào sự khác biệt về `location`:
* **Hive:** `.../fra_connected_parties_i.${batch_date}.dat`.
* **Spark:** `.../{batch_date}/ConnectedParties_Data_{batch_date}.TXT`.
* **Đánh giá:** Nếu quy tắc đổi tên file/đường dẫn này là cố định, bạn có thể cấu hình trong YAML. Nếu nó thay đổi tùy theo file, đây sẽ là điểm khó nhất vì cần logic "thông minh" để đoán định format đường dẫn mới.

---

### 🥊 Intellectual Sparring: Điểm "bất khả thi" nếu chỉ dùng Rule-base đơn giản

Dựa trên cấu hình máy **i7-9750H / 32GB RAM**, năng lực tính toán của bạn không thiếu, nhưng logic của tool đang có một lỗ hổng:

1.  **Sự "mất mát" thông tin:** Trong file `.py` mẫu, hàm `drop_partition_day` sử dụng `params["retention_raw_delta"]`, nhưng trong file `.sql` gốc, giá trị này lại là `${retain_day}`. Tool của bạn làm sao biết được `retain_day` tương ứng với key `retention_raw_delta` trong từ điển `params`? 
    * *Kết luận:* Bạn bắt buộc phải có một file cấu hình mapping cực kỳ chi tiết cho từng loại script.
2.  **Lệnh `source`:** File SQL có dòng `source /.../all_tez_para.sql;`. Trong file Python, nó biến mất và được thay bằng `set_parameter()`. Tool cần có danh sách các lệnh "cần loại bỏ" khi sang Spark.
3.  **Thay đổi định dạng file:** File gốc là `.dat`, file đích là `.TXT`. Nếu đây là yêu cầu từ hệ thống mới, tool của bạn cần một bộ luật đổi đuôi file (Extension mapping).

### 💡 Lời khuyên cho Phase 1
Để đạt được kết quả như file mẫu, tôi khuyên bạn nên bắt đầu với **Jinja2 Template**. 
* Bạn tạo một file mẫu `.py` với các chỗ trống (placeholder).
* Tool Python sẽ đọc file SQL, parse các thành phần, sau đó "đổ" (render) vào template Jinja2. Cách này sẽ sạch sẽ hơn là cộng chuỗi (string concatenation) thủ công.

**Bước tiếp theo:**
Bạn muốn tôi viết thử đoạn code Python dùng **Regex kết hợp Jinja2** để thực hiện việc chuyển đổi DDL (như lệnh `drop table`) từ file mẫu này sang định dạng f-string không? Hay bạn muốn xử lý phần "bóc tách comment" trước?