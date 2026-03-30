Đây là bản mô tả và yêu cầu kỹ thuật (Technical Specification) dành cho dự án **HQL Spark Bridge** mở rộng. Tài liệu này được thiết kế để team có cái nhìn xuyên suốt từ khâu bóc tách Metadata (Excel) đến khâu sinh mã tự động (Airflow DAG), dựa trên nền tảng kỹ thuật hiện có.

---

# 📑 Tài liệu Mô tả & Yêu cầu Kỹ thuật: HQL Spark Bridge Engine

## 1. Context & Tầm nhìn Dự án (The "Bridge" Vision)
Dự án **HQL Spark Bridge** không chỉ dừng lại ở việc chuyển đổi ngôn ngữ (Transpilation) từ HiveQL sang SparkSQL. Mục tiêu mở rộng là trở thành một **Pipeline Factory**: Tự động hóa việc tạo lập toàn bộ luồng dữ liệu (Data Pipeline) từ khâu khai báo (Excel Metadata) đến khâu điều phối (Airflow Orchestration).

Hệ thống sẽ tận dụng tối đa cấu hình máy **i7-9750H / 32GB RAM** để xử lý hàng loạt Metadata và sinh code nhanh chóng, chính xác.

---

## 2. Phân tích Các Thành phần Đầu vào (Input Context)

Hệ thống dựa trên 3 nguồn dữ liệu chính để ra quyết định:

### 2.1. Metadata Job (`job (21).xlsx`)
* **Job Definition:** Chứa danh sách định danh các Task. Mỗi dòng đại diện cho một đơn vị thực thi (Operator).
* **Các trường khóa:** `JOB_NAME`, `JOB_TYPE`, `COMMAND`.
* **Ý nghĩa:** Xác định "Cái gì" sẽ chạy và chạy "Như thế nào" (Shell script hay Spark command).

### 2.2. Metadata Flow (`jobflow (29).xlsx`)
* **Flow Definition:** Định nghĩa "vỏ" của DAG như `SCHEDULE_INTERVAL`, `OWNER`, và các tham số global.
* **Job Dependency:** Thiết lập đồ thị có hướng (DAG). Định nghĩa quan hệ cha-con giữa các Job thông qua `PARENT_JOB_NAME` và `CHILD_JOB_NAME`.
* **Flow Pre-set:** Chứa các biến môi trường hoặc tham số đặc thù cho từng luồng chạy.

### 2.3. Quy tắc bóc tách tên (Naming Convention)
Dưới đây chỉ là ví dụ: Trong thực tế hoàn toàn có thể khác nhau.
`{SCHEMA}_{SOURCE}_{TABLE_NAME}`
* **Cơ chế bóc tách:**
    * `Schema`: Substring đầu tiên trước dấu gạch dưới (`_`).
    * `Source`: Substring thứ hai.
    * `Table Name`: Phần còn lại cho đến cuối chuỗi.

---

## 3. Logic Xử lý & Phân loại Luồng (Execution Profiles)

Điểm khác biệt của tool nằm ở việc tự động nhận diện "Chiến thuật thực thi" dựa trên **Source Name** bóc tách được:

| Loại Nguồn (Source) | Đặc điểm Nhận dạng | Logic Sinh Job đầu tiên                                               | Ví dụ |
| :--- | :--- |:----------------------------------------------------------------------| :--- |
| **API** | Source = `tomsapi`, v.v. | dùng HTTP Operator hoặc script Python API.                            | `tomsapi_account_accountlist_post` |
| **HDFS (File)** | Source = `KDI`, v.v. | Tạo Task kéo file từ HDFS/Landing Zone về Staging.                    | `stg_kdi_transaction_load` |
| **Direct DB** | Source = `K2`, `K2_BANK` | Bỏ qua bước kéo file, đi thẳng vào Job RAW (truy vấn JDBC trực tiếp). | `raw_k2_bank_customer` |



---

## 4. Yêu cầu Kỹ thuật Đầu ra (Output Specs)

Sản phẩm cuối cùng là một file Python (`.py`) tương thích với **Airflow 2.x** có các đặc điểm:
* **Tính linh hoạt (Dynamic):** Các Task được khởi tạo thông qua vòng lặp duyệt Metadata, không viết code cứng (Hard-code).
* **Dependency Mapping:** Các quan hệ cha-con được thiết lập chính xác theo bảng `Dependent job dependency`.
* **Tái sử dụng Module:** File DAG sẽ gọi các function xử lý SQL từ module `HQL Spark Bridge` đã xây dựng trước đó để thực hiện Transformation.

---

## 5. Các Mô-đun Cần Triển khai (Implementation Strategy)

### 5.1. Mô-đun `NamingResolver`
* **Nhiệm vụ:** Trích xuất thông tin từ Job Name.
* **Đầu ra:** Trả về một Dictionary chứa `schema`, `source`, `table`.

### 5.2. Mô-đun `ProfileEngine`
* **Nhiệm vụ:** Đọc file cấu hình `execution_profiles.yaml` để map `Source` với `Operator` tương ứng.
* **Quy tắc:** Nếu `Source` là API thì dùng `SimpleHttpOperator`, nếu là DB thì dùng `SparkSubmitOperator` với JDBC.

### 5.3. Mô-đun `DAGGenerator` (Jinja2 Template)
* **Nhiệm vụ:** Sử dụng Template engine để đổ dữ liệu Metadata vào mẫu code Python.
* **Lợi ích:** Dễ dàng thay đổi format DAG mà không cần sửa logic xử lý chính.

---

## 6. 🥊 Intellectual Sparring: Các Điểm Chạm Cần Lưu Ý

Dưới góc độ phản biện kỹ thuật, team cần xử lý các rủi ro sau:
1.  **Tính nhất quán của Metadata:** Nếu file Excel khai báo Job A phụ thuộc Job B, nhưng Job B không tồn tại trong danh sách Job Definition, tool phải báo lỗi ngay lúc biên dịch (Compile-time) thay vì để DAG lỗi lúc chạy (Runtime).
2.  **Xử lý tham số động:** Các biến như `${batch_date}` trong `COMMAND` cần được chuyển đổi sang định dạng Jinja của Airflow là `{{ ds }}` để đảm bảo tính kế thừa.
3.  **Hiệu suất xử lý Excel:** Với 32GB RAM, chúng ta nên load toàn bộ file Excel vào bộ nhớ thông qua `pandas` một lần duy nhất để tối ưu tốc độ.

---

**Kết luận:** Dự án này sẽ biến các file Excel Metadata khô khan thành một hệ thống Pipeline sống động, giảm thiểu 90% công sức viết code thủ công và đảm bảo mọi Job đều tuân thủ đúng chuẩn của **HQL Spark Bridge**.