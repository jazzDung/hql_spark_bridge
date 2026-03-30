
# Tổng quan project

1. Môi trường
   - Context: Không có access đến Spark do chưa có access VPN, không có Hive luôn vì đang dùng máy tính cá nhân để dev
   - 1 instance Hive trên HDFS
     - HDFS: 3.1.1.7.1.9.0-387
     - Beeline: 3.1.3000.7.1.9.0-387
     - Hive: 3.1.3000.7.1.9.0-387
   - 1 instance Spark 
     - Spark 3.5.5.400-dep-1001
   - Ưu tiên ở dạng dễ deploy, dễ dọn dẹp khi chạy xong, không chạy nền khi không cần thiết, chỉ phục vụ mục đích thử script và demo
2. Tính năng (phát triển theo các giai đoạn)
   - Phase 1: Chưa cần bàn đến spark performance
     - Có 3 loại script chính
       - DDL: Chỉ là query tạo bảng thông thường
       - DDL: Query biến đổi dữ liệu, có sử dụng đọc từ file text trên HDFS ở layer Raw, nhiều CTE, subquery ở layer trên
       - UNL: Query từ bảng sau đó export ra file .dat trên hdfs
     - Biên dịch HiveQL sang SparkSQL ở mức tối thiểu có thể chạy được, chỉ dịch các keyword / hàm mà bản thân SparkSQL không có.
     - Hỗ trợ biến trong query: Ví dụ schema có các biến như \${raw_schema}, \${com_schema}, batch date của dữ liệu là \${batch_date} và khi query engine chạy sẽ điền vào, khi biến đổi thành Spark SQL phải hỗ trợ (có thể là qua hivevar hoặc hiveconf)
     - Tách từng query thành câu lệnh spark.sqk("...") vì spark của vendor không thể chạy 1 lúc cả script to đùng nhiều query 
     - Chỉ bỏ query vào trong lệnh spark.sql, tự động lọc lấy comment và để nó ở ngoài, như ví dụ dưới
       ```sql
       -- my comment
       select * from schema.table_name;
       
       -- My another comment
       ```

       ```python
       ## my comment
       spark.sql("select * from schema.table_name;")
       
       ## My another comment
       ```
     - Sau khi biến đổi script xong phải có tính năng tự check với Spark engine để xem syntax đúng không
     - Script Spark phải tự bổ sung thêm các config / command / hàm khác cần thiết để chạy 1 cách ổn định 
       - Các lệnh cơ bản như tạo spark session, set giá trị config / biến ...
       - Các lệnh này phải có khả năng config bằng file YAML cho dễ sửa đổi
     - Thành quả sau cùng phải có khả năng đóng lại thành 1 cái docker image để triển khai trên hệ thống
       - Nếu được thì làm luôn cả github actions / azure devops pipeline để đóng gói
   - Phase 2: Cho mọi người xài
     - Tính năng chia nhỏ query: 1 số query HiveQL cũ có nhiều transformation phụ tuần tự nhưng không có sự phụ thuộc, cho phép chia nhỏ thành nhiều lệnh spark song song khi biên dịch.
     - Có khả năng biến đổi 1 folder, output ra 1 folder khác
     - Bắt đầu cho phép biến đổi rule base 1 cách đơn giản
       - Ví dụ: Nếu bảng tên bắt đầu bằng abc xyz thì hãy sử dụng CTE thay vì sub query
       - Các quy luật này nên được config sử dụng các file như yaml hoặc json (cá nhân tôi thích yaml hơn)
       - Cho phép thay đổi quy luật biên dịch code cho từng file riêng lẻ nếu cần
     - Data integrity
       - Sử dụng 1 thư viện nào đó (Great expectation cũng được này) + các query cơ bản để kiểm tra dữ liệu
       - Đảm bảo đầu vào của bảng trên cả Hive và Spark là giống nhau, sau đó kiểm tra dữ liệu đầu ra
       - Hỗ trợ config các bài kiểm tra này từ record / column level cho đến check cơ bản hơn như count(*), limit 10...
   - Phase 3: Bánh vẽ
     - UI: Yeah, làm 1 cái UI để biểu diễn 
       - Có khả năng tự pull code từ github, show tất cả các file theo đúng file hiarachy, gần như là 1 cái file system viewer
       - Khi ấn vào từng file, sẽ hiện ra file gốc bên trái, file đã biên dịch bên phải, cùng với các config / luật chuyển đổi bên dưới cùng
       - Cho phép tự động thêm / bớt các luật biên dịch
