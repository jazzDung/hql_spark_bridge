Quyết định cực kỳ sáng suốt! Đây chính là tư duy của một Data Engineer thực thụ: **Dùng LLM để viết code tạo ra tool (tất định), chứ không dùng LLM để làm tool (xác suất).**

Khi bạn giao task này cho một Coding Agent (như Cursor, GitHub Copilot, hoặc một LLM khác), bạn cần cung cấp một bản đặc tả kỹ thuật (Technical Specification) thật rõ ràng về cách thư viện `sqlglot` hoạt động.

Dưới đây là **Bảng hướng dẫn chi tiết** và **Prompt chuẩn mực** để bạn copy-paste thẳng cho Coding Agent của mình.

---

### Phần 1: Prompt giao việc cho Coding Agent

Bạn hãy copy chính xác đoạn này đưa cho Agent:

> **Role:** You are an Expert Python Data Engineer specializing in AST (Abstract Syntax Tree) manipulation using the `sqlglot` library.
> **Task:** Write a Python function `extract_primary_key(sql_string: str, dialect: str = 'hive') -> dict` that analyzes an ETL SQL script and deduces the Logical Primary Key based on specific coding patterns.
> **Core Logic & Heuristics:**
> Implement the deduction logic based on the Technical Specification Table below. Prioritize "The Golden Clue" (Window Functions).
> **Output Format Expected from the Function:**
> Return a dictionary containing:
> * `logical_primary_key`: list of strings (column names).
> * `confidence`: string ('HIGH', 'MEDIUM', 'LOW').
> * `reasoning`: string (explanation of which pattern was matched).
> 
> 
> **Crucial `sqlglot` details:**
> * When extracting column names from `partition_by`, remember that columns might be wrapped in functions (e.g., `TRIM(COALESCE(col_name, ''))`). You MUST traverse down to find `exp.Column` nodes and extract the `.name` attribute to avoid getting function strings.
> * Ignore alias assignments (`exp.Alias`) when traversing for column names.
> 
> 

---

### Phần 2: Bảng Đặc tả Kỹ thuật (Technical Specification Table)

Bạn đính kèm bảng này vào sau đoạn prompt trên để Agent hiểu chính xác các Node của `sqlglot` cần tìm:

| Heuristic (Luật suy diễn) | Sqlglot Node Target | Logic / Điều kiện kích hoạt (Conditions) | Hành động trích xuất (Extraction Action) | Confidence Score |
| --- | --- | --- | --- | --- |
| **1. The Golden Clue (Khử trùng lặp)** | `exp.Window` | 1. `node.this` phải là `exp.RowNumber`.<br>

<br>2. `node.args` phải chứa `partition_by`. | Đi sâu vào `partition_by`, tìm tất cả các node `exp.Column` bên trong. Lấy giá trị `column.name`. | **HIGH** |
| **2. The Silver Clue (Trục Join)** | `exp.Join` | 1. Quét mệnh đề `on` (loại `exp.EQ` hoặc `exp.And`).<br>

<br>2. Tìm các cột được dùng làm điều kiện map giữa các bảng.<br>

<br>3. Chỉ lấy nếu tập hợp cột này lặp lại ở ≥ 2 lệnh `LEFT JOIN`. | Lấy tên các cột nằm ở vế tham chiếu (`table.column`) của mệnh đề `ON`. | **MEDIUM** |
| **3. DDL Explicit (Khai báo tường minh)** | `exp.Create` / `exp.Schema` | Tìm node `exp.PrimaryKeyColumnConstraint` bên trong định nghĩa `exp.ColumnDef`. | Lấy trực tiếp tên cột có chứa constraint khóa chính. | **HIGH** |
| **4. Physical Partition (Khóa vật lý)** | `exp.Insert` / `exp.Partition` | Nằm trong DML (Insert/Alter). Tìm cụm `PARTITION (col = 'val')`. | Lấy thuộc tính `this` (thường là `exp.Identifier` hoặc `exp.Column`) của phép gán trong partition. | *(Bổ sung vào Physical Key, không thay thế Logical Key)* |

---

### Phần 3: Góc nhìn phản biện - Các "Bẫy" cần dặn Agent (The Sparring Partner Check) 🥊

Coding Agent thường viết code chạy được ngay (happy path), nhưng hay gãy ở các case thực tế phức tạp. Khi Agent trả code về, hãy kiểm tra xem nó đã xử lý 3 góc khuất này chưa:

1. **Bẫy Hàm Lồng Nhau (Nested Functions in Partition):**
* *SQL thực tế:* `PARTITION BY trim(coalesce(account_number, '')), client_id`
* *Lỗi thường gặp của Agent:* Nó sẽ trả về cái tên quái thai: `['trim(coalesce(account_number, ''))', 'client_id']`.
* *Cách check:* Đảm bảo code của Agent có dùng vòng lặp `.find_all(exp.Column)` để bóc trần mọi lớp vỏ function, chỉ lấy cái lõi `account_number`.


2. **Bẫy CTE (Common Table Expressions):**
* Trong một file SQL lớn, sẽ có nhiều lệnh `ROW_NUMBER()` ở các CTE phụ trợ (ví dụ lấy tỷ giá mới nhất), chứ không phải lấy khóa chính của bảng.
* *Cách check:* Agent cần ưu tiên quét hàm `Window` nằm ở **mệnh đề `SELECT` ngoài cùng nhất** (thường là câu DML `INSERT INTO`) trước khi quét các CTE con bên trong `WITH`.


3. **Bẫy Không Tìm Thấy (Fallback):**
* Nếu SQL chỉ đơn thuần là `INSERT INTO target SELECT * FROM source` (không có Window, không có Join), Agent phải biết cách trả về mảng rỗng `[]` và `confidence: LOW`, thay vì cố gắng lấy bừa cột đầu tiên nó thấy.



Bằng cách đưa bản đặc tả này cho một Coding Agent (như Claude 3.5 Sonnet hoặc GPT-4o), nó sẽ gen ra cho bạn một script Python `extract_primary_key.py` cực kỳ mạnh mẽ, sạch sẽ và có thể tích hợp thẳng vào luồng CI/CD của bạn để tự động build Data Catalog.