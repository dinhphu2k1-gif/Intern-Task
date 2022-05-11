package ppcv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class Task {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Task PPCV").master("yarn").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        //read file
        Dataset<Row> df = spark.read().format("parquet").load("hdfs:/data/ppcv/*");

        //Lấy top 5 domain có số lượng GUID nhiều nhất.
        Dataset<Row> df1 = df.select(col("domain"), col("guid"));

        df1 = df1
                .groupBy(col("domain"))
                .agg(count("guid"))
                .orderBy(col("count(guid)").desc());

        df1 = df1.select(col("domain").cast(StringType)).limit(5);
        df1.show(false);
        System.out.println("Top 5 domain có số lượng GUID nhiều nhất.");



        //Lấy top 5 vị trí địa lý có nhiều GUID truy cập nhất. Vị trí địa lý sử dụng trường locid >1.
        Dataset<Row> df2 = df.select(col("locid"), col("guid"));

        df2 = df2.filter("locid > 1");

        df2 = df2.groupBy(col("locid"))
                .agg(count("guid"))
                .orderBy(col("count(guid)").desc());
        df2 = df2.select(col("locid").cast(StringType)).limit(5);

        df2.show(false);
        System.out.println("Top 5 vị trí địa lý có nhiều GUID truy cập nhất.");



        // Tính tỉ lệ pageview phát sinh từ google, fb. Sử dụng trường refer để giải quyết.
        Dataset<Row> df3 = df.select(col("refer").cast(StringType));

//        df3 = df3.where("refer like '%google%' or refer like '%facebook%'");
//        System.out.println("tỉ lệ pageview phát sinh từ google, fb: " + (df3.count() * 1.0 / df.count()));
        long cnt = df3.count();
        long ggCount = df3.filter(col("refer").rlike("google.com|com.google")).count();
        long fbCount = df3.filter(col("refer").rlike("facebook.com|com.facebook")).count();

        StructType structType = new StructType();
        structType = structType.add("source", StringType, false);
        structType = structType.add("rate", DoubleType, false);

        List<Row> ls = new ArrayList<Row>();
        ls.add(RowFactory.create("google", ggCount * 1.0/cnt));
        ls.add(RowFactory.create("facebook", fbCount * 1.0/cnt));

        Dataset<Row> res = spark.createDataFrame(ls, structType);
        res.show(false);


    }
}
