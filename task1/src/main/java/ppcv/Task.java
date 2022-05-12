package ppcv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class Task {
    private SparkSession spark;
    private Dataset<Row> df;

    public Dataset<Row> readParquetFile(String path) {
        return spark.read().format("parquet").load(path);
    }

    public void writeParquet(Dataset<Row> df, String path) {
        df.write().mode("overwrite").parquet(path);
    }

    public Dataset<Row> topBasedMaxGuid(Dataset<Row> df, String colName, int numRecords) {
        /**
         * Hàm lấy 1 DataFrame, tên cột và trả về số lượng records của cột dựa trên số lượng GUID nhiều nhất
         *
         * @param colName: tên cột cần lấy thông tin
         * @param numRecords: số lượng bản ghi cần lấy
         * @return một DataFrame với 2 cột: $colName và count(guid) (BinaryType và IntegetType)
         */
        Dataset<Row> newDf = df.groupBy(col(colName).cast(StringType).as(colName))
                .agg(count("guid").as("numGUID"))
                .orderBy(col("numGUID").desc());
        return newDf.limit(numRecords);
    }




    public void start() {
        this.spark = SparkSession.builder().appName("Task PPCV").master("yarn").getOrCreate();
        this.spark.sparkContext().setLogLevel("ERROR");

        this.df = this.readParquetFile("hdfs:/data/ppcv/*");

        Dataset<Row> df1 = df.select(col("domain"), col("guid"));

        //Lấy top 5 domain có số lượng GUID nhiều nhất.
        Dataset<Row> res1 = this.topBasedMaxGuid(df1, "domain", 5);
        res1.show(false);
        this.writeParquet(res1, "hdfs:/result/task1/ppcv/ex1");

        System.out.println("============================================");

        //Lấy top 5 vị trí địa lý có nhiều GUID truy cập nhất. Vị trí địa lý sử dụng trường locid >1.
        Dataset<Row> df2 = df1.filter("locid > 1");
        Dataset<Row> res2 = this.topBasedMaxGuid(df2, "locid", 5);
        res2.show(false);
        this.writeParquet(res2, "hdfs:/result/task1/ppcv/ex2");

        System.out.println("============================================");

        // Tính tỉ lệ pageview phát sinh từ google, fb. Sử dụng trường refer để giải quyết.

    }

    public static void main(String[] args) {
        Task app = new Task();
        app.start();

//        //Lấy top 5 vị trí địa lý có nhiều GUID truy cập nhất. Vị trí địa lý sử dụng trường locid >1.
//        Dataset<Row> df2 = df.select(col("locid"), col("guid"));
//
//        df2 = df2.filter("locid > 1");
//
//        df2 = df2.groupBy(col("locid"))
//                .agg(count("guid"))
//                .orderBy(col("count(guid)").desc());
//        df2 = df2.select(col("locid").cast(StringType)).limit(5);
//
//        System.out.println("Top 5 vị trí địa lý có nhiều GUID truy cập nhất.");
//        df2.show(false);
//
//
//        // Tính tỉ lệ pageview phát sinh từ google, fb. Sử dụng trường refer để giải quyết.
//        Dataset<Row> df3 = df.select(col("refer").cast(StringType));
//
////        df3 = df3.where("refer like '%google%' or refer like '%facebook%'");
////        System.out.println("tỉ lệ pageview phát sinh từ google, fb: " + (df3.count() * 1.0 / df.count()));
//        long cnt = df3.count();
//        long ggCount = df3.filter(col("refer").rlike("google.com|com.google")).count();
//        long fbCount = df3.filter(col("refer").rlike("facebook.com|com.facebook")).count();
//
//        StructType structType = new StructType();
//        structType = structType.add("source", StringType, false);
//        structType = structType.add("rate", DoubleType, false);
//
//        List<Row> ls = new ArrayList<Row>();
//        ls.add(RowFactory.create("google", ggCount * 1.0 / cnt));
//        ls.add(RowFactory.create("facebook", fbCount * 1.0 / cnt));
//
//        Dataset<Row> res = spark.createDataFrame(ls, structType);
//        res.show(false);
    }
}
