


Sản phẩm là một Metadata-driven Framework chuyên dụng để tự động hóa việc di cư (Migration) và tái cấu trúc (Refactoring) các đường ống dữ liệu từ **HiveQL** sang **Spark SQL**. Hệ thống sử dụng cơ chế **SQL Decomposition** để bóc tách logic nghiệp vụ phức tạp thành các khối Lego có thể tái sử dụng.


### Đầu vào
- File biến đổi sql
- Ví dụ: File com_t_mhbos_m_client


### Bước 1: Bóc tách SQL + Lưu metadata của script
- Sử dụng logic để tự bóc tách các bảng con, biến đổi logic.
  - Nếu là bảng phụ (ví dụ như temp_t_mhbos_m_client_all, temp_t_mhbos_m_client_secondary_identification_type, ....) thì bóc các phần sql ra và để trong 1 thư mục metadata, lưu thành từng file như com_temp_t_mhbos_m_client.sql, com_temp_t_mhbos_m_client_primary_identification_no.sql, .... với format là {layer}_{temp_table}.sql
- Lưu các thông tin về script gốc tại file yaml
  - Ví dụ: sample_script_metadata.yaml
  - Đây chỉ là file ví dụ, chúng ta cần thiết kế file yaml này sao cho đầy đủ thông tin cần thiết để có thể biến đổi thành công script từ cũ sang mới, có thể kể đến như joining key, filter criteria cho từng bảng temp / bảng đích
  - Cần có 1 logic để xác định được mô hình transform dữ liệu nằm trong folder /docs/Data Loading Methodology, tổng cộng có 8 model, model này sẽ quyết định logic merge dữ liệu từ layer raw vào com, không liên quan đến các logic biến đổi đã nói ở trên
  - File yaml này phải lưu cả schema của bảng, nếu file được lưu tại folder ..../dml/table_name.sql thì mặc định DDL bảng lưu tại ..../dml/table_name.sql, đọc từ đó ra.


### Bước 2: Xử lý metdata
- Do việc chuyển từ Datalake mới sang cũ khiến tất cả bảng com không còn partition nữa (chỉ có 1 set data duy nhất), khiến cho nhiều logic của scrip cũ bị thừa hoặc sai
- Mỗi source (k2, mhbos, lms, toms, ...) sẽ có 1 kiểu viết script khác nhau do người viết tự nghĩ
  - Mỗi source này chúng ta sẽ có 1 file yaml người dùng tự input, chứa hướng dẫn xử lý sâu hơn, bóc tách thông tin từ các bảng phụ / script chính
  - File yaml có thể làm nhiều thứ khác, ví dụ như gắn mặc định 1 data model cho 1 source nếu chúng giống hệt nhau, ....
- Ngoài ra tùy vào model xử lý sẽ có thêm các cột mà bảng cũ không có (như record_created_date, record_updated_date, hash_value, đều phục vụ data model mới), nhiệm vụ của chúng ta là tìm script DDL bảng gốc và bổ sung thêm các trường này vào nếu cần thiết.


### Bước 3: Tạo script PySpark
- May cho chúng ta là Datalake cũ và mới đều dùng nền Hive, chỉ khác về việc script cũ là hivesql, script mới là pyspark với các lệnh spark.sql()
- Với các mô hình biến đổi của Datalake mới, chúng ta không sử dụng com_r nữa.
  - Nếu gặp 1 script com_r, phải tìm xem script đó có com_t không, nếu có com_t, chúng ta mặc định sử dụng script com_t để làm việc, bỏ qua com_r, nếu chỉ có script com_r, chúng ta dùng script đó để làm việc nhưng script generate ra phải để tên com_t
- Các mô hình biến đổi được lưu dạng template jinja, có thể lấy ví dụ ở file template/datalake_model_5b/pyspark_dml/com_t.jinja
- Từ file yaml ở trên, các metadata sql đã bóc nhỏ, cùng với template , ta muốn tạo ra 2 file pyspark
  - Loại đơn giản, tất cả logic biến đổi trong 1 script python, chỉ là phần logic merge dữ liệu từ layer raw vào com tuân thủ theo template model
  - Loại phức tạp: Tạo ra các file python, với mỗi file cho 1 processing step, và 1 file cho khoản insert / merge vào bảng cuối


## Ví dụ luồng com_r_k2_cif_alias
1. Input: docs/datalake_old/com_r_k2_cif_alias.sql
2. Bước 1: Bóc ra các bảng phụ, logic
  - Ta bóc được các file sql phụ trong folder docs/datalake_new/processing_steps/r_k2_cif_alias
  - Và tạo ra file config yaml trong file docs/datalake_new/metadata/com_r_k2_cif_alias.yaml
  - 1 số phân tích về file này: 
    - Mục columns đọc từ file ddl, có vài cột sẽ remark là non_original_field, do chúng ta đã đề luật trong docs/datalake_new/source_rule/k2.yaml, mục column_rules / non_original_fields
    - model_type là 3 vì ăn theo mặc định trong docs/datalake_new/source_rule/k2.yaml
    - key: cifaliasid, cái này nói thật tôi cũng chưa biết phải dùng rule base như này hay dùng AI Agent, nhìn mắt thường thì sẽ thấy được dễ dàng key là cifaliasid nhưng thật sự không biết code như nào.

3. Bước 2: Xử lý metdata
  - Dựa vào luật trong docs/datalake_new/source_rule/k2.yaml, chúng ta bỏ qua các bảng tạm với hậu tố như _bk, _bf, _nw, _od

4. Bước 3: Tạo script Pyspark
  - Tạo script DDL
    - Model type 3 cần có thêm các cột sau: etl_timestamp, record_created_date, record_updated_date, record_status, hash_value nên chúng ta sẽ bổ sung nó vào script DDL
    - Các cột ở bước 1 có remark là non_original_fields chúng ta sẽ bỏ đi
    - Script DDL mới generate ra ở docs/datalake_new/ddl/com_t_k2_cif_alias.sql, note là vì sang Datalake mới không còn com_r nữa nên bảng của chúng ta giờ là com_t_k2_cif_alias
  - Tạo script DML
    - Vì luật xử lý metadata yêu cầu chúng ta bỏ qua các bảng tạm với hậu tố nhất định nên chúng ta chỉ cần xử lý mỗi bảng chính, dựa vào danh sách schema, key của bảng, cùng với transformation model template, ta generate ra script docs/datalake_new/dml/com_t_k2_cif_alias.py





