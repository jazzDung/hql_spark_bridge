1. Sơ đồ Kiến trúc Tổng thể (The Big Picture)
Luồng dữ liệu (Data flow) của chương trình sẽ đi qua 4 giai đoạn độc lập:
[Giai đoạn 1: Ingestion & Parsing] -> [Giai đoạn 2: Context Extraction] -> [Giai đoạn 3: AST Transformation (3 nhánh)] -> [Giai đoạn 4: Code Generation]
2. Thiết kế chi tiết từng Giai đoạn
Giai đoạn 1: Parse 1 lần duy nhất (Single Source of Truth)
Đừng đọc và sửa chuỗi string (regex) ở giai đoạn này. Hãy dùng sqlglot để đọc script HiveQL và biến nó thành một danh sách các cây AST. Toàn bộ các nhánh sau này sẽ sử dụng chung cây AST này làm nguyên liệu gốc.
•
Đầu vào: raw_k2_bank.sql (Hive)
•
Đầu ra: List[AST_Nodes] (Các object Python của sqlglot).
Giai đoạn 2: Xây dựng Context Object
Trích xuất các thông tin ngữ cảnh từ cây AST để các module sau dễ dùng, thay vì phải parse lại. Ví dụ: Tạo class JobContext chứa source_name, table_name, list_of_columns, is_partitioned,...
Giai đoạn 3: Phân nhánh bằng Strategy Pattern (Lõi của hệ thống)
Đây là nơi bạn xử lý yêu cầu "1 thành 3" của mình. Tạo một lớp Base (BaseTransformer) và 3 lớp con kế thừa từ nó tương ứng với 3 kết quả bạn mong muốn:
A. SqlNotebookTransformer (Cho mục tiêu 3: Chạy thủ công trên Notebook)
•
Nhiệm vụ: Trả ra một script thuần SQL.
•
Logic: Gọi variable_loader (đọc file variable.yaml), chọn profile là dev hoặc uat.
•
Duyệt qua cây AST, tìm các node Variable (như ${raw_schema}) và thay thế trực tiếp bằng giá trị thật (ví dụ: raw).
•
Gọi node.sql(dialect="spark").
B. BasicPySparkTransformer (Cho mục tiêu 1: Giữ nguyên logic SQL cho BA)
•
Nhiệm vụ: Trả ra script PySpark bọc SQL.
•
Logic: Gọi variable_loader, chọn profile là pyspark.
•
Duyệt AST, đổi ${raw_schema} thành chuỗi {params["raw_schema"]}.
•
Gọi node.sql(dialect="spark") để lấy chuỗi SQL đã được dịch.
•
Gửi các chuỗi SQL này vào file Jinja template cơ bản (basic_template.py.j2).
C. OptimizedPySparkTransformer (Cho mục tiêu 2: Tối ưu hiệu năng, dùng DataFrame)
•
Nhiệm vụ: Phá vỡ cấu trúc SQL cũ, chuyển sang PySpark DataFrame API (như .format("jdbc").load()).
•
Logic: Lớp này sẽ phức tạp nhất. Bạn cần sử dụng mô hình Visitor để can thiệp sâu vào AST.
◦
Nếu gặp node Create External Table (file text): Đọc config configs/rules/custom_table_a.yaml xem có rule đổi sang đọc trực tiếp từ DB không. Nếu có, sinh ra code Python dạng df = spark.read.jdbc(...).
◦
Nếu gặp node Insert Into: Tách phần SELECT ra để tạo DataFrame, sau đó sinh ra đoạn code df.write.insertInto(...).
•
Gửi các biến Python sinh ra vào file Jinja template nâng cao (optimized_template.py.j2).
3. Cấu trúc Thư mục Code (Module hóa)
Với kiến trúc trên, thư mục dự án của bạn nên được thiết kế như sau để người khác dễ dàng đọc hiểu và đóng góp:
src/
├── core/                   # Chứa logic nền tảng
│   ├── config_loader.py    # Class chuyên đọc các file configs/rules/*.yaml
│   └── parser.py           # Gọi sqlglot để tạo AST
├── context/
│   └── job_context.py      # Lớp chứa Metadata của 1 file SQL
├── transformers/           # Nơi chứa các logic "biến hình"
│   ├── base.py
│   ├── notebook_sql_transformer.py   # Mục tiêu 3
│   ├── basic_pyspark_transformer.py  # Mục tiêu 1
│   └── optim_pyspark_transformer.py  # Mục tiêu 2
├── templates/              # Thư mục chứa các file Jinja (.j2)
│   ├── basic_job.py.j2
│   └── optimized_job.py.j2
└── spark_verify.py         # Điểm Entry (Hàm main)
4. Làm thế nào để Config qua YAML cho tính mở rộng?
Bạn muốn "người khác không phải tôi có thể thêm bớt các logic dưới dạng file yaml".
Giải pháp: Xây dựng cơ chế "Rule-based AST Rewriting" (Viết lại AST dựa trên luật).
Thay vì hardcode logic chuyển từ "Đọc file .dat" sang "Đọc JDBC" vào trong code Python, bạn định nghĩa nó trong YAML.
Ví dụ file: configs/rules/data_source_optimization.yaml
YAML
# Rule để biến đổi lệnh tạo bảng External thành lệnh đọc Spark DataFrame
rules:
  - rule_name: "Replace_TextFile_With_JDBC"
    condition:
      sql_type: "Create"
      file_format: "TEXTFILE"
      table_prefix: "raw_k2_"
    action: "generate_jdbc_read"
    parameters:
      jdbc_url_var: 'params["mssql_jdbc_url"]'
      db_table_var: 'k2.bank_info'
Trong lớp OptimizedPySparkTransformer, bạn viết 1 engine nhỏ để:
1.
Load toàn bộ file YAML trong thư mục configs/rules/.
2.
Gặp 1 node AST (ví dụ node Create), nó sẽ check xem node này có thỏa mãn condition trong YAML không.
3.
Nếu khớp, nó kích hoạt hàm generate_jdbc_read (được map sẵn bằng Python dict) để nhả ra code PySpark thay vì SQL.
🌟 Tóm lại:
Thiết kế này giúp bạn đạt được 3 điều:
1.
Tách biệt Mối quan tâm (Separation of Concerns): Dữ liệu (YAML), Logic phân tích cốt lõi (sqlglot), Logic biến đổi (Transformers), và Hiển thị (Jinja Templates) hoàn toàn độc lập. Sửa cái này không làm gãy cái kia.
2.
Khả năng mở rộng: Ngày mai có sếp yêu cầu thêm nhánh thứ 4 (ví dụ: tạo file chạy trên Airflow), bạn chỉ việc tạo thêm 1 file AirflowTransformer.py kế thừa từ BaseTransformer mà không cần đụng vào code cũ.
3.
Thân thiện với người dùng: Bạn bè trong team có thể sửa quy tắc đổi tên biến, hay quy tắc chọn file template chỉ bằng việc mở file cấu hình YAML lên sửa, không cần biết dòng code Python nào.