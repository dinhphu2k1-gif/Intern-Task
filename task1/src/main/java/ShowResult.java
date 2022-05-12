import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ShowResult {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Show Result Task").master("yarn").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        /*
        1. data/ppcv
        - Lấy top 5 domain có số lượng GUID nhiều nhất.
         */
        Dataset<Row> df1 = spark.read().format("parquet").load("hdfs:/result/task1/ppcv/ex1/part-*");
        System.out.println("Top 5 domain có số lượng GUID nhiều nhất.");
        df1.show(false);

        /*
        - Lấy top 5 vị trí địa lý có nhiều GUID truy cập nhất. Vị trí địa lý sử dụng trường locid >1.
         */
        Dataset<Row> df2 = spark.read().format("parquet").load("hdfs:/result/task1/ppcv/ex2/part-*");
        System.out.println("Top 5 vị trí địa lý có nhiều GUID truy cập nhất.");
        df2.show(false);

        /*
        - Tính tỉ lệ pageview phát sinh từ google, fb.
         */

        Dataset<Row> df3 = spark.read().format("parquet").load("hdfs:/result/task1/ppcv/ex3/part-*");
        System.out.println("Tỉ lệ pageview phát sinh từ google, fb.");
        df3.show(false);


        /*
        2. /data/apcdx
        - Đếm số  GUID theo từng bannerid theo ngày
         */
        Dataset<Row> df4 = spark.read().format("parquet").load("hdfs:/result/task1/apcdx/ex1/part-*");
        System.out.println("Số  GUID theo từng bannerid theo ngày");
        df4.show(false);

        /*
        - Đếm số  GUID theo từng bannerid theo tháng
         */
        Dataset<Row> df5 = spark.read().format("parquet").load("hdfs:/result/task1/apcdx/ex2/part-*");
        System.out.println("Số  GUID theo từng bannerid theo ngày");
        df5.show(false);

        /*
        - Tính toán việc phân bổ bannerid theo từng domain
         */
        Dataset<Row> df6 = spark.read().format("parquet").load("hdfs:/result/task1/apcdx/ex3/part-*");
        System.out.println("Tính toán việc phân bổ bannerid theo từng domain.");
        df6.show(false);
    }
}
