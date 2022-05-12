Thông tin chi tiết về Task xem tại [đây](https://github.com/dinhphu2k1-gif/Intern-Task/blob/master/task1/Task)

# 1. /data/ppcv
Chạy câu lệnh sau để chạy Job:

`spark-submit --class ppcv.Task --master yarn --deploy-mode client --num-executors 5 --executor-memory 2g --executor-cores 2 target/task1-1.0-SNAPSHOT.jar`

hoặc chạy file .sh

`./TaskPPCV.sh`

# 2. /data/apcdx
Chạy câu lệnh sau để chạy Job:

`spark-submit --class apcdx.Task --master yarn --deploy-mode client --num-executors 5 --executor-memory 2g --executor-cores 2 target/task1-1.0-SNAPSHOT.jar`

hoặc chạy file .sh

`./TaskAPCDX.sh`

# 3. Kết quả
Kết quả được lưu dưới dạng file Parquet ở folder "/result"
Để xem được kết quả có thể chạy câu lệnh

`spark-submit --class ShowResult --master yarn --deploy-mode client --num-executors 5 --executor-memory 2g --executor-cores 2 target/task1-1.0-SNAPSHOT.jar`

hoặc

`./ShowResult.sh`

