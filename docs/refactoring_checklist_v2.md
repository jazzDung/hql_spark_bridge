Đây là bản tài liệu kỹ thuật **refactoring_checking** được thiết kế dưới góc độ của một **"Intellectual Sparring Partner"**. Tài liệu này tổng hợp toàn bộ các quy tắc logic, giải pháp kỹ thuật và các phương án dự phòng để đảm bảo tool **BeeVolt** của bạn đạt độ tin cậy tuyệt đối trên hệ thống **i7-9750H / 32GB RAM**.

---

# 🛠 BeeVolt Refactoring & Logic Checking Blueprint

Tài liệu này đóng vai trò là "la bàn" kỹ thuật để phát triển công cụ chuyển đổi HiveQL sang SparkSQL, tập trung vào tính chính xác của dữ liệu và khả năng xử lý các trường hợp ngoại lệ (edge cases).

---

## 1. Cơ sở hạ tầng & Quản lý Tài nguyên (Infrastructure)
Dựa trên cấu hình máy **jazz_dung**:
* **WSL2/Docker Limit:** Giới hạn tài nguyên ở mức `memory=16GB` và `processors=6` trong `.wslconfig` để tránh treo máy khi chạy Spark.
* **Storage Management:** Tận dụng 1.82TB lưu trữ để lưu trữ Docker images và dữ liệu mẫu (Sample data).
* **Python Version:** Sử dụng **Python 3.11.5** cô lập trong `venv` để tránh xung đột với các bản 3.7, 3.9, 3.14 hiện có trên Windows.

---

## 2. Logic Khám phá Metadata (The "Sandwich" Strategy)
Tool phải tự động nhận diện bối cảnh dữ liệu từ đường dẫn `LOCATION` trong file SQL.
* **Quy tắc bóc tách:**
    * `Source Name`: Substring đầu tiên trước dấu `_`.
    * `Extraction Mode`: Substring cuối cùng trước phần mở rộng `.dat`.
    * `Table Name`: Tất cả phần nằm giữa `Source` và `Mode`.
* **Edge Case:** Nếu tên file không tuân thủ quy tắc (ví dụ chỉ có 1 dấu `_`), tool phải fallback về cấu hình mặc định trong file YAML thay vì crash.

---

## 3. Chiến lược Schema & Casting (DDL-Driven)
Đây là "trái tim" để đảm bảo tính nhất quán dữ liệu.

### Phương án Chính: Static DDL Parsing (Khuyên dùng cho Phase 1)
* **Logic:** Đọc file `.sql` chứa lệnh `CREATE TABLE` của Hive để lấy Target Schema.
* **Ưu điểm:** Không cần VPN, tốc độ xử lý nhanh, dễ dàng kiểm soát phiên bản (Version Control).
* **Thực thi:** Sử dụng `sqlglot` để parse kiểu dữ liệu Hive (ví dụ: `STRING`, `BIGINT`) sang kiểu tương ứng của Spark.

### Phương án Alternative: Live Metastore Connection
* **Logic:** Kết nối trực tiếp vào Hive Metastore qua JDBC/Thrift để lấy schema thực tế.
* **Ưu điểm:** Chính xác tuyệt đối với môi trường Production, tự động cập nhật nếu bảng bị `ALTER`.
* **Nhược điểm:** Yêu cầu VPN ổn định và tốn tài nguyên duy trì kết nối.

> **Quy tắc Cast tường minh:** Mọi cột `Timestamp` từ nguồn DB phải được `.cast("string")` nếu DDL của Hive quy định là `STRING` để đảm bảo không lệch múi giờ và đúng format hệ thống cũ.

---

## 4. Công cụ Biên dịch (Transpilation Engine)
### Quản lý Biến (Variable Mapping)
* **Mapping Table:** Xây dựng từ điển ánh xạ biến giữa Hive và Python:
    * `${batch_date}` $\rightarrow$ `batch_date` (f-string).
    * `${last_date}` $\rightarrow$ `last_date` (f-string).
    * `${batch_timestamp}` $\rightarrow$ `current_timestamp().cast("string")`.
    * `${raw_schema}` $\rightarrow$ `params["raw_schema"]`.
    * `${com_schema}` $\rightarrow$ `params["com_schema"]`.
    * `${cur_schema}` $\rightarrow$ `params["cur_schema"]`.
* **Variable Resolver:** Giải mã các biến môi trường phức tạp (ví dụ: `${itl_data_path}`) thông qua file cấu hình YAML tập trung.

### Xử lý Logic SQL
* **Preservation:** Giữ nguyên các hàm `nvl()`, `trim()`, `coalesce()` từ script gốc để tránh sai lệch dữ liệu.
* **Segmentation:** Tách query thành các khối `spark.sql()` riêng biệt, đẩy comment ra ngoài làm chú thích Python (`#`) để tăng độ sạch của code.

---

## 5. Danh sách Kiểm tra Edge Cases (Checklist)
* [ ] **Comment lồng nhau:** Xử lý trường hợp `--` nằm bên trong chuỗi string của SQL.
* [ ] **Tranh chấp Table Name:** Khi chạy song song (Phase 2), tự động thêm hậu tố (suffix) cho các bảng tạm để tránh ghi đè dữ liệu.
* [ ] **JDBC Driver:** Đảm bảo container Spark có đủ thư viện `.jar` của MSSQL/Oracle để thực hiện lệnh `.load()`.
* [ ] **Empty Strings vs Nulls:** Đồng bộ hóa cách Hive và Spark xử lý giá trị rỗng trong file `.dat` so với đọc trực tiếp từ DB.

---

## 6. Giai đoạn Kiểm thử (Validation Phase)
1.  **Dry Run:** Chỉ sinh code Python, không thực thi, kiểm tra bằng mắt thường.
2.  **Syntax Check:** Sử dụng `spark.sql("EXPLAIN ...")` để kiểm tra cú pháp mà không cần chạy dữ liệu thật.
3.  **Data Integrity (Phase 2):** Sử dụng *Great Expectations* để so sánh `count(*)` và `checksum` giữa kết quả của Hive cũ và Spark mới.

---



