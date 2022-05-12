package apcdx;

import functions.MyFunctions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;

public class Task {
    private SparkSession spark;
    private Dataset<Row> df;

    public void start() throws IOException {
        this.spark = SparkSession.builder().appName("Task APCDX").master("yarn").getOrCreate();
        this.spark.sparkContext().setLogLevel("ERROR");

        MyFunctions functions = new MyFunctions(this.spark, "Task APCDX");

        //Load đường dẫn
        List<String> directories = new ArrayList<>();

        Configuration conf = this.spark.sparkContext().hadoopConfiguration();
        conf.set("fs.defaultFS", "hdfs://internship-hadoop105185:8220/");
        FileStatus[] fs = FileSystem.get(conf).listStatus(new Path("hdfs://internship-hadoop105185:8220/data/apcdx/"));

        for (FileStatus f : fs) {
            if (f.isDirectory()) {
                directories.add(f.getPath().toString());
//                System.out.println(f.getPath());
            }
        }

        StructType schema = createStructType(new StructField[]{
                createStructField("date", StringType, true),
                createStructField("bannerId", IntegerType, true),
                createStructField("guid", LongType, true),
                createStructField("domain", BinaryType, true)
        });

        this.df = spark.createDataFrame(new ArrayList<>(), schema);
        // create a DataFrame from all files with date time
        for (String directory : directories) {
            Dataset<Row> df = spark.read()
                    .format("parquet")
                    .load(directory + "/*");

            df = df.filter("click_or_view == 'false'");
            df = df.select(col("bannerId"), col("guid"), col("domain"));
            df = df.withColumn("date", lit(directory.substring(directory.length() - 10)).cast(StringType));

//            df.show(false);
            this.df = this.df.unionByName(df);
            System.out.println("Finish file: " + directory);
        }


        this.df.sample(.01).show(false);
        this.df.printSchema();

        System.out.println("=============================");

        Dataset<Row> df1 = this.df.drop("domain");

        //Đếm số GUID theo từng bannerid theo ngày
        Dataset<Row> res1 = functions.topBasedMaxGuid(df1, -1, col("date"), col("bannerId"));
        res1.sample(.01).show(false);

        functions.writeParquet(res1, "hdfs:/result/task1/apcdx/ex1");

        System.out.println("=============================");

        //Đếm số GUID theo từng bannerid theo tháng
        Dataset<Row> df2 = res1.withColumn("month", lit(res1.col("date").substr(0, 7)))
                                .withColumnRenamed("numGUID", "guid")
                                .drop("date");

        Dataset<Row> res2 = functions.topBasedMaxGuid(df2, -1, col("month"), col("bannerId"));
        res2.sample(.01).show(false);

        functions.writeParquet(res2, "hdfs:/result/task1/apcdx/ex2");

        System.out.println("=============================");

        //Tính toán việc phân bổ bannerid theo từng domain
        Dataset<Row> df3 = this.df.drop("guid")
                .drop("date");

        Dataset<Row> res3 = functions.topBasedMaxBanner(df3, -1, col("domain"));
        res3.sample(.01).show(false);

        functions.writeParquet(res3, "hdfs:/result/task1/apcdx/ex3");
    }
    public static void main(String[] args) throws IOException {
        Task app = new Task();
        app.start();
    }
}
