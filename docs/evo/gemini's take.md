Chào bạn, dựa trên mô tả yêu cầu rất thực tế và chuyên sâu của bạn, mình đã biên soạn một **Master Sheet: Technical Specification & Implementation Guide** cực kỳ chi tiết. Tài liệu này đóng vai trò như bản thiết kế hệ thống (System Design) và sổ tay hướng dẫn từng bước (Step-by-step) để bạn refactor lại `hql_spark_bridge` thành một **AI-Enhanced Metadata-driven Framework** thực thụ.

---

# MASTER SHEET: HQL TO PYSPARK REFACTORING FRAMEWORK

## I. TỔNG QUAN KIẾN TRÚC HỆ THỐNG
Dự án sẽ được cấu trúc lại thành dạng Module (Plug-and-play), chia làm 3 luồng xử lý chính (Pipeline Stages):

1. **Decomposition Engine (Bóc tách)**: Đọc HiveQL SQL, chia nhỏ thành các "Lego block" (bảng tạm, bước biến đổi).
2. **Rule & Metadata Engine (Xử lý logic)**: Đối chiếu với Source Rules (YAML) để quyết định giữ/bỏ logic nào, bổ sung DDL nào, nhận diện Data Model.
3. **Generation Engine (Sinh code)**: Kết hợp Metadata cuối cùng + Jinja Templates để sinh ra PySpark DML & DDL.

---

## II. CẤU TRÚC THƯ MỤC DỰ KIẾN (Folder Structure)

Bạn nên cấu trúc lại repo/thư mục làm việc để quản lý rõ ràng giữa logic cũ và mới:

```text
docs/datalake_new/
├── source_rules/          # Chứa YAML cấu hình cho từng source (k2.yaml, mhbos.yaml...)
├── metadata/              # Chứa metadata sinh ra sau Bước 1 (com_t_k2_cif_alias.yaml...)
├── processing_steps/      # Chứa các file SQL đã bóc tách (com_temp_...sql)
├── templates/             # Jinja template cho 8 mô hình Datalake
├── output/
│   ├── ddl/               # Script DDL sau khi xử lý (com_t_*.sql)
│   └── dml/               # Script PySpark DML sinh ra (com_t_*.py)
```

---

## III. HƯỚNG DẪN TRIỂN KHAI CHI TIẾT (STEP-BY-STEP)

### BƯỚC 1: BÓC TÁCH SQL VÀ LƯU METADATA (Decomposition)

**Mục tiêu:** Parse file HiveQL cũ, bóc ra các bảng phụ và sinh file `metadata.yaml` mô tả cấu trúc biến đổi.

**Công cụ sử dụng:**
- **`sqlglot` (Python)**: Để phân tích AST (Abstract Syntax Tree), lấy ra tất cả các lệnh `CREATE TABLE ... AS SELECT` hoặc `INSERT INTO ... SELECT`.
- **AI Agent (OpenAI/Gemini API)**: Sử dụng **chỉ khi** `sqlglot` không thể parse các câu query quá dị (khác chuẩn) HOẶC để "đọc hiểu" và lấy ra `joining key`, `filter criteria`.

**Logic xử lý:**
1. Đọc script `com_r_<name>.sql` hoặc `com_t_<name>.sql`.
2. Kiểm tra quy tắc `com_r` / `com_t`:
   - Nếu đầu vào là `com_r_X`: Kiểm tra xem có script `com_t_X` tồn tại không. 
   - Có -> Hủy tiến trình hiện tại, switch sang xử lý `com_t_X`.
   - Không -> Dùng `com_r_X`, nhưng cờ `target_table_name` = `com_t_X`.
3. Duyệt AST:
   - Lưu trữ các bảng temp thành dạng file text vào `docs/datalake_new/processing_steps/<tên_job>/<layer>_<temp_table>.sql`.
4. Gọi **AI Agent** để phân tích logic:
   - Do các logic join key, nghiệp vụ phức tạp rất khó dùng Regex hoặc AST để trích xuất chuẩn, đây là lúc AI phát huy sức mạnh vượt trội.
   - *Prompt cho AI*: Cung cấp câu SQL, yêu cầu trả về định dạng JSON chứa: Primary Keys dự kiến, Join Keys giữa các bảng, Lọc (Filter).

**Ví dụ thiết kế - `metadata/com_r_k2_cif_alias.yaml` (Kết quả của Bước 1):**

```yaml
job_name: com_r_k2_cif_alias
target_table: com_t_k2_cif_alias
source_system: k2 # Tự động infer từ tên job

original_layer: com_r
steps:
  - step_id: 1
    table_name: com_temp_t_k2_cif_alias_all
    sql_file: processing_steps/r_k2_cif_alias/com_temp_t_k2_cif_alias_all.sql
    join_keys: []
    filters: "date > '2023-01-01'"
  - step_id: 2
    table_name: target
    sql_file: processing_steps/r_k2_cif_alias/target_insert.sql
    join_keys: ["cifaliasid"] # Nhờ AI trích xuất
schema_location: "docs/datalake_old/ddl/com_r_k2_cif_alias.sql"
```

---

### BƯỚC 2: XỬ LÝ METADATA THÔNG QUA RULE ENGINE

**Mục tiêu:** Áp dụng bộ luật (Source Rules) để lọc bỏ bảng thừa, mapping mô hình Datalake, bổ sung trường DDL cần thiết.

**Thiết kế `source_rules/k2.yaml` (File do con người tự input):**

```yaml
source_name: k2
default_model_type: 3  # SCD Type 2
ignore_temp_suffixes:  # Luật bỏ bảng tạm
  - "_bk"
  - "_bf"
  - "_nw"
  - "_od"
column_rules:
  non_original_fields: # Các cột logic cũ thừa, cần bỏ đi
    - "partition_date"
    - "etl_date_old"
model_injections:      # Áp dụng cho data model 3
  model_3:
    add_columns:
      - { name: "etl_timestamp", type: "timestamp" }
      - { name: "record_created_date", type: "timestamp" }
      - { name: "record_updated_date", type: "timestamp" }
      - { name: "record_status", type: "string" }
      - { name: "hash_value", type: "string" }
```

**Logic xử lý (Code Python):**
1. Load `metadata.yaml` (từ Bước 1) và `source_rules/<source>.yaml`.
2. **Lọc DML**: Duyệt danh sách `steps` trong metadata, xóa các step có bảng kết thúc bằng đuôi bị ignore (`_bk`, `_bf`,...).
3. **Xử lý DDL**:
   - Đọc file DDL gốc (dựa vào `schema_location`).
   - Xóa các cột nằm trong `non_original_fields` hoặc xóa khai báo `PARTITIONED BY` (vì datalake mới là 1 set duy nhất).
   - Thêm các cột từ `model_injections.model_3.add_columns`.
   - Sinh ra script hoàn chỉnh và ghi vào `output/ddl/com_t_k2_cif_alias.sql`.
4. Sinh ra `final_metadata.yaml` (chỉ chứa trong bộ nhớ Python) để truyền cho Bước 3.

---

### BƯỚC 3: TẠO SCRIPT PYSPARK (GENERATION)

**Mục tiêu:** Kết nối Jinja template và biến thành script PySpark cuối cùng.

**Phân luồng dựa trên mức độ phức tạp:**
- **Loại đơn giản (Simple)**: Job chỉ có 1 bước (chỉ lấy thẳng từ RAW ném vào COM), hoặc sau khi áp dụng rules (bỏ bảng `_bk`, `_bf`...) thì chỉ còn lại đúng bảng đích.
- **Loại phức tạp (Complex)**: Job vẫn còn nhiều bảng `temp` cần được xử lý tuần tự.

**Chiến lược Render Jinja:**
Dùng PySpark `spark.sql()` để bọc các khối Lego (SQL) lại. Hệ thống mới dùng PySpark thay thế Hive, do đó việc gói SQL bên trong `spark.sql()` là an toàn và dễ debug nhất.

**Ví dụ Code Python Render Template:**

```python
from jinja2 import Environment, FileSystemLoader

env = Environment(loader=FileSystemLoader('docs/datalake_new/templates'))

if len(final_steps) == 1:
    # LOẠI ĐƠN GIẢN
    template = env.get_template('datalake_model_3/pyspark_dml/simple_com_t.jinja')
    pyspark_code = template.render(
        target_table="com_t_k2_cif_alias",
        join_keys=final_metadata['join_keys'],
        main_sql_logic=read_sql_file(final_steps[0]['sql_file'])
    )
    write_file('output/dml/com_t_k2_cif_alias.py', pyspark_code)
else:
    # LOẠI PHỨC TẠP
    # 1. Tạo folder output/dml/com_t_k2_cif_alias_steps/
    # 2. Render từng file python tương ứng với các temp table step
    # 3. Tạo 1 file master_merge.py để chạy chuỗi pipeline
```

**Ví dụ Jinja Template cho Loại Đơn Giản (`simple_com_t.jinja`):**

```python
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("{{ target_table }}").enableHiveSupport().getOrCreate()
    
    # Logic SQL được bóc tách
    df_transformed = spark.sql("""
        {{ main_sql_logic | indent(8) }}
    """)
    
    # Xử lý các logic Model 3 (Hash Diff, UPSERT)
    # Đây là logic chuẩn bị sẵn của framework dựa trên {{ join_keys }}
    df_target = spark.table("{{ target_table }}")
    
    # ... logic join so sánh hash ...
    
    df_final.write.mode("overwrite").saveAsTable("{{ target_table }}")

if __name__ == "__main__":
    main()
```

---

## IV. QUY TRÌNH RA QUYẾT ĐỊNH CHO AI AGENT (AI USAGE CHECKLIST)

Để tránh lạm dụng chi phí và đảm bảo hệ thống chạy ổn định (deterministic), bạn **chỉ dùng AI** ở các tác vụ sau (thuộc Bước 1):

✅ **Nên dùng AI API (như OpenAI GPT-4o-mini hoặc Gemini Flash):**
1. **Trích xuất Join Key & Lọc:** Truyền khối query SELECT khổng lồ vào và hỏi: *"Lấy cho tôi joining key và WHERE clauses của logic bảng đích này"*. (AI trả lời JSON rất giỏi).
2. **Format SQL rác:** Rất nhiều script do developer viết tay thụt dòng lộn xộn, không tuân thủ chuẩn ANSI. Đưa qua AI dọn dẹp trước khi đưa cho `sqlglot` parse.

❌ **Không dùng AI (Sử dụng Rule-base/Code):**
1. Đọc và chỉnh sửa file DDL: Chắc chắn phải dùng Regex hoặc code bóc mảng, nếu dùng AI có thể bịa ra (hallucinate) các data type sai lệch hệ thống.
2. Sinh code PySpark cuối cùng: Luôn phải dùng Jinja Template. AI sinh code nguyên file dễ làm hỏng kiến trúc pipeline đồng nhất của Data Platform.

---

## V. TÓM TẮT HÀNH ĐỘNG (ACTION PLAN DÀNH CHO BẠN)

1. **Chuẩn bị Dữ liệu:** Đảm bảo tất cả file DDL và DML cũ đều đặt sẵn vào thư mục input.
2. **Phát triển Module 1 (Parser):** Xây dựng Python Script đọc HiveQL file, tách theo từ khóa `CREATE TEMPORARY TABLE` hoặc `INSERT INTO`, xuất ra các file phụ.
3. **Định nghĩa Rule:** Tự tay ngồi viết file `k2.yaml`, `mhbos.yaml` trước để làm mỏ neo kiểm thử.
4. **Phát triển Module 2 (Rule Engine):** Viết logic Python load YAML và loại bỏ các bước rác (`_bk`, `_bf`).
5. **Cập nhật DDL Engine:** Viết class parse DDL thuần tuý (bỏ `PARTITIONED BY`, thêm cột dựa trên Type 3).
6. **Xây dựng Template Jinja:** Đưa các mô hình Datalake mới vào file `.jinja`.
7. **Tích hợp API AI (Tùy chọn)**: Tại vị trí bóc tách Key, gọi API ném payload SQL và nhận JSON trả về. 

Kiến trúc này đảm bảo **tính Data-driven** (tất cả cấu hình nằm ngoài code, ở file YAML), tính **module** (dễ dàng bảo trì) và kiểm soát hoàn toàn được luồng code (sử dụng template render thay vì phó mặc cho AI viết code bừa).