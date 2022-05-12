package ppcv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.StringType;

import functions.MyFunctions;

public class Task {
    private SparkSession spark;
    private Dataset<Row> df;

    public void start() {
        this.spark = SparkSession.builder().appName("Task PPCV").master("yarn").getOrCreate();
        this.spark.sparkContext().setLogLevel("ERROR");

        MyFunctions functions = new MyFunctions(spark, "Task PPCV");

        this.df = functions.readParquetFile("hdfs:/data/ppcv/*");

        Dataset<Row> df1 = this.df.select(col("domain"), col("guid"));

        //Lấy top 5 domain có số lượng GUID nhiều nhất.
        Dataset<Row> res1 = functions.topBasedMaxGuid(df1, 5, col("domain"));
        res1 = res1.select(col("domain")
                    .cast(StringType), col("numGUID"));

        res1.show(false);

        functions.writeParquet(res1, "hdfs:/result/task1/ppcv/ex1");

        System.out.println("============================================");

        //Lấy top 5 vị trí địa lý có nhiều GUID truy cập nhất. Vị trí địa lý sử dụng trường locid >1.
        Dataset<Row> df2 = this.df.select(col("locid"), col("guid"))
                                .filter("locid > 1");

        Dataset<Row> res2 = functions.topBasedMaxGuid(df2, 5, col("locid"));
        res2.show(false);

        functions.writeParquet(res2, "hdfs:/result/task1/ppcv/ex2");

        System.out.println("============================================");

        // Tính tỉ lệ pageview phát sinh từ google, fb. Sử dụng trường refer để giải quyết.
        Dataset<Row> df3 = this.df.select(col("refer").cast(StringType));

        long numRecords = this.df.count();
        long numGoogle = functions.countSubstring(df3, "refer", "google.com|com.google");
        long numFacebook = functions.countSubstring(df3, "refer", "facebook.com|com.facebook");

        StructType structType = new StructType();
        structType = structType.add("source", StringType, false);
        structType = structType.add("rate", DoubleType, false);

        List<Row> ls = new ArrayList<Row>();
        ls.add(RowFactory.create("google", numGoogle * 100.0 / numRecords));
        ls.add(RowFactory.create("facebook", numFacebook * 1.0 / numRecords));

        Dataset<Row> res3 = spark.createDataFrame(ls, structType);
        res3.show(false);
        functions.writeParquet(res3, "hdfs:/result/task1/ppcv/ex3");
    }

    public static void main(String[] args) {
        Task app = new Task();
        app.start();
    }
}
