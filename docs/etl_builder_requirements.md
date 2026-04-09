## Context: Luồng ETL
1. Extraction: 
   - Đọc từ database nguồn, sử dụng Pentaho Kettle, đầu ra file .dat
   - Bên trong script sẽ có 1 đoạn query sql để làm logic lấy data từ nguồn

1.5 API Extraction:
    - Đọc thẳng từ API, sử dụng Python
    - 

2. Datalake: HiveQL
   - Raw, Com layer: 
     - Cấu thành từ 2 script: DDl và DML
     - Biến đổi dựa theo các model đã định sẵn (overwrite, insert append, insert overwrite dựa trên partition ....)
     - Mặc định có các lệnh trimming, nullif, .... tùy vào data type 
     - Có thể có bi biến đổi đơn giản (in-line) cho các trường, nhưng hầu như là không có joining với bảng raw, com khác
     - Cách đặt tên: {schema}_{com_table_type (only apply for com}_{source_name}_{source_table_name}
     - Ví dụ: raw_mhbos_m_client, com_t_mhbos_m_client, com_m_mhbos_m_client
     - Người tạo bảng cần liệt kê các partition key: Thường là etl_dt hoặc year_month
     - Schema:
       - raw: ${raw_schema}
       - com: ${com_schema}
   - Cur layer
     - Cấu thành từ 2 script: DDl và DML
     - Logic phức tạp, đọc từ nhiều bảng raw, com, cur khác nhau, thỉnh thoảng là cur lol
     - Cũng có các model bieến đổi điịnh sẵn (SCD 1,2,3,4, Transaction, accounts, reference insert overwrite ...)
     - Có rõ ràng 2 kiểu thiết kế script cur:
       - Kiểu 1: Cùng 1 bảng cur có thể có nhiều nguồn dữ liệu, tách thành nhiều script
       - Kiểu 2: Tất cả nguồn đều nhét hết vào trong 1 script và trong đó tạo nhiều bảng temp cho mỗi nguồn, đến cuối insert toàn bộ vào bảng đích
     - Người tạo bảng cần liệt kê các partition key: Thường là etl_dt, year_month, source_key
     - Cách đặt tên: 
       - Nếu thiết kế kiểu 1: {curated_table_type}_{table_name}_{source_key}
         - curated_table_type:
           - Nếu là bảng sử dụng nội bộ Datalake thì là cur, dim, fact, log, ...
           - Nếu là các bảng với mục đích dump ra file để các application sử dụng thì đặt theo application: einvoice, reach, iremisier, ailab
         - source_key: Đây thường được sử dụng để làm 1 trong những partition của bảng, cái này sẽ có document để xác định chứ không phải lúc nào cũng = source_table_name
         - Ví dụ: dim_customer_mhbos_m_client, dim_customer
       - Nếu thiết kế kiểu 2: {curated_table_type}_{table_name}_{source_key}
     - Schema: ${cur_schema}
     - Trong logic thỉnh thoảng sẽ có source merging: 2 nguồn dữ liệu có nhiều record overlap, cần có logic để merge cả 2 (Chọn record mới hơn dựa vào trường thời gian, hoặc với từng cột, ưu tiên tùy nguồn)
     - Trong logic sẽ có 1 số logic dedup, merging, nói chung là để đảm bảo dữ liệu bảng thỏa mãn các unique key mà người thiết kế script muốn
   - Unl layer
     - Cấu thành từ 1 script: DML
     - Script đơn giản select từ bảng cur, rồi unload ra file .dat
     - Cách đặt tên
       - Nếu đọc hết từ bảng curated: unl_{curated_table_type}_{table_name}
       - Nếu đọc từ bảng curated có filter theo source_key: unl_{curated_table_type}_{table_name}_{source_key}
     - Schema: ${cur_schema}

3. AILab Datahub loading: PostgreSQL
   - Staging Table: Bảng để chứa trực tiếp file từ unload layer
     - Chỉ cần script DDL
   - Main table: Bảng đích tại database của AILab
     - Script DDL
   - Procedue: Procedure để insert dữ liệu từ file dat > bảng staging > bảng main
     - Script DML

### Đề xuất: Cho phép build tự động mọi script sử dụng yaml
1. Các file config:
  - Source credential:
    - Login vào tất cả các Database nguồn để lấy trực tiếp schema
  - Input YAML cho ext / raw / com:
    - Tên database, schema, bảng hoặc API
    - Tên model biến đổi tại từng layer raw, com
    - Input path dẫn đến đoạn query sql cho ext
    - Các config khác như primary key, partition key, timestamp key (dùng cho các model biến đổi)
  - Input YAML cho ext / raw / com:
    - Model biến đổi
    - List các bảng input (raw / com / cur) cùng với alias
    - List mapping từng field dựa vào alias bảng ở trên + tên trường, cố gắng ghi chép dưới dạng SQL nhất có thể
    - Các config khác như primary key, partition key, timestamp key (dùng cho các model biến đổi)
    - Nếu có logic merging, liệt kê nguồn merge, ưu tiên, cách thức merge (tímestamp, mặc định ưu tiên 1 nguồn, ưu tiên tùy vào trường dữ liệu)

2. Các file template
  - File xml pentaho: ext.jinja
    - Sẽ render toàn bộ file XML. File XML này có 3 mục chính
      - Parameters + Connection: Credential dẫn đến database nguồn
      - Table input: SQL Query
      - String operations: 1 số trimming, biến đổi nhỏ cho các trường string
      - Text file output: File đầu ra
  - File DDL raw / com / cur
    - Mới bắt đầu có thể chưa quá cần thiết logic merging, tập trung vào các logic in line trước
    - cur_fact_transaction_event_sbl_transactiondata: Kiểu script curated chia nhỏ theo nguồn
    - cur_dim_agent: Kiểu script curated gộp tất cả nguồn, có cả logic merging cho TOMS
  - File DML cho từng model biến đổi
  - File DDL cho bảng staging / main trên AILab
  - FIle Procedure AILab: sp_dm_account.jinja

3. Các bước để chương trình chạy
  - Tạo các file config yaml
  - Chương trình chạy hàm phụ để lấy schema nguồn, schema của API
    - Nếu là db thì lưư query sql cơ bản
    - Nếu là db thì lưu schema dạng json
  - Người dùng sửa / thêm bớt vào query / schema trên để phù hợp nhu cầu bản thân
  - Chạy hàm chính để generate tiếp ext, raw, com, cur, ... dựa vào query sql / schema đã có input người dùng
  - Nếu được thì các file config yaml, query nguồn, schema api sẽ lưu tập trung tại 1 repo / folder duy nhất để phục vụ mục đích documentation luôn


