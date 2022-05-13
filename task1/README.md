# 1. [Đề bài](Task.txt):
1. /data/ppcv
- Lấy top 5 domain có số lượng GUID nhiều nhất.
- Lấy top 5 vị trí địa lý có nhiều GUID truy cập nhất. Vị trí địa lý sử dụng trường locid >1.
- Tính tỉ lệ pageview phát sinh từ google, fb. Sử dụng trường refer để giải quyết.
2. apcdx
- Đếm số  GUID theo từng bannerid theo ngày
- Đếm số  GUID theo từng bannerid theo tháng
- Tính toán việc phân bổ bannerid theo từng domain
  => Chú ý, các dữ liệu này lấy các row có click_or_view =false

# 2. Cách làm
- Nhóm các cột bằng groupBy 
- Tổng hợp kết quả: count
- Sắp xếp kết quả: orderBy

# 3. Chạy
# 3.1. Build file Jars
Chạy lệnh:
`mvn clean package`

hoặc file .sh:

`./buildJar.sh`


## 3.2. /data/ppcv
Chạy Job 1:

`spark-submit --class ppcv.Task --master yarn --deploy-mode client --num-executors 5 --executor-memory 2g --executor-cores 2 target/task1-1.0-SNAPSHOT.jar`

hoặc chạy file .sh

`./taskPPCV.sh`

## 3.3. /data/apcdx
Chạy Job 2:

`spark-submit --class apcdx.Task --master yarn --deploy-mode client --num-executors 5 --executor-memory 2g --executor-cores 2 target/task1-1.0-SNAPSHOT.jar`

hoặc chạy file .sh

`./taskAPCDX.sh`

# 3. Kết quả
Kết quả được lưu dưới dạng file Parquet ở folder "/result"
Để xem được kết quả có thể chạy câu lệnh

`spark-submit --class ShowResult --master yarn --deploy-mode client --num-executors 5 --executor-memory 2g --executor-cores 2 target/task1-1.0-SNAPSHOT.jar`

hoặc

`./ShowResult.sh`

Kết quả mẫu được lưu ỏ file [Result.txt](Result.txt)

