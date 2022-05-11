package apcdx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;

public class Task {
    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession.builder().appName("Task APCDX").master("yarn").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        //Load đường dẫn
        List<String> directories = new ArrayList<>();

        Configuration conf = spark.sparkContext().hadoopConfiguration();
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

        Dataset<Row> df1 = spark.createDataFrame(new ArrayList<>(), schema);
        // create a DataFrame from all files with date time
        for (String directory : directories) {
            Dataset<Row> df = spark.read()
                    .format("parquet")
                    .load(directory + "/*");

            df = df.filter("click_or_view == 'false'");

            df = df.select(col("bannerId"), col("guid"), col("domain"));
//            df = df.groupBy(col("bannerId")).agg(count("guid")).orderBy(col("count(guid)").desc());
            df = df.withColumn("date", lit(directory.substring(directory.length() - 10)).cast(StringType));

//            df.show(false);
            df1 = df1.unionByName(df);
            System.out.println("Finish file: " + directory);
        }

//        df1.cache();

        df1.sample(.01).show(false);
        df1.printSchema();

        System.out.println("=============================");

        System.out.println("Đếm số GUID theo từng bannerid theo ngày");
        Dataset<Row> df2 = df1.drop("domain");

        df2 = df2.groupBy(col("date"), col("bannerId"))
                .agg(count("guid"))
                .orderBy(col("count(guid)").desc()).cache();

        df2.sample(.01).show(false);

        System.out.println("=============================");

        Dataset<Row> df3 = df2.withColumn("month", lit(df2.col("date").substr(0, 7)));

        df3 = df3.drop("date");

        df3 = df3.groupBy(col("month"), col("bannerId"))
                .agg(count("count(guid)").as("count(guid)"))
                .orderBy(col("count(guid)").desc());

        System.out.println("Đếm số  GUID theo từng bannerid theo tháng");
        df3.sample(.01).show(false);

        System.out.println("=============================");

        Dataset<Row> df4 = df1.drop("guid")
                .drop("date");

        df4 = df4.groupBy("domain")
                .agg(count("bannerId"));

        System.out.println("Tính toán việc phân bổ bannerid theo từng domain ");
        df4.sample(.01).show(false);

    }
}
