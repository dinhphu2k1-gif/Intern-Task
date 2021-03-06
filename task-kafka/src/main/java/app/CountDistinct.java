/**
 *
 */

package app;

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
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.swoop.alchemy.spark.expressions.hll.functions.hll_init_agg;
import static com.swoop.alchemy.spark.expressions.hll.functions.hll_merge;
import static com.swoop.alchemy.spark.expressions.hll.functions.hll_cardinality;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;

public class CountDistinct {
    /**
     * SparkSession.
     */
    private SparkSession spark;

    /**
     * Nơi đọc dữ liệu.
     */
    private final String sourcePath = "/data/task-kafka";

    /**
     * Lấy toàn bộ folder tại đường dẫn được chỉ định.
     *
     * @param directory : đường dẫn
     * @return danh sách các đường dẫn
     */
    public List<String> getListDirs(String directory) {

        List<String> paths = new ArrayList<>();

        Configuration conf = this.spark.sparkContext().hadoopConfiguration();
        conf.set("fs.defaultFS", "hdfs://internship-hadoop105185:8220/");
        FileStatus[] fs;
        try {
            fs = FileSystem.get(conf).listStatus(new Path(directory));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (FileStatus f : fs) {
            if (f.isDirectory()) {
                paths.add(f.getPath().toString());
//                System.out.println(f.getPath());
            }
        }
        return paths;
    }

    /**
     * Lấy các foler có thời gian tạo nằm trong khoảng 1 thời gian cho trước.
     *
     * @param startTime : thời gian bắt đầu
     * @param endTime :   thời gian kết thúc
     * @return danh sách các đường dẫn thỏa mãn
     */
    public List<String> getListFolder(String startTime, String endTime) {
        List<String> listDirs = this.getListDirs(this.sourcePath);

        List<String> ls = new ArrayList<>();
        for (String dir : listDirs) {
            String folder = dir.substring(dir.length() - 10);
            if (folder.compareTo(startTime) >= 0
                & folder.compareTo(endTime) < 0) {
                ls.add(dir);
            }
        }

        return ls;
    }

    /**
     * Đếm số lượng user theo từng banner trong một Khoảng thời gian cụ thể từ HDFS.
     * @param startTime : thời gian bắt đầu
     * @param endTime : thời gian kết thúc
     */
    public void countDistinctFromHDFS(String startTime, String endTime) {
        List<String> list = this.getListFolder(startTime, endTime);

        Dataset<Row> newDF;

        StructType schema = createStructType(new StructField[]{
                createStructField("bannerId", IntegerType, true),
                createStructField("guid", LongType, true)
        });

        newDF = this.spark.createDataFrame(new ArrayList<>(), schema);

        for (String dir : list) {
            Dataset<Row> df = this.spark.read().format("parquet").load(dir);
            newDF = newDF.union(df);
//            System.out.println("Finish file: " + dir);
        }

        System.out.println("Count not using HyperLogLog");
        long t0 = System.nanoTime();
        newDF.groupBy("bannerId")
                .agg(count_distinct(col("guid")).as("count"))
                .orderBy(desc("count"))
                .show();

        long t1 = System.nanoTime();
        System.out.println("Execution time: " + TimeUnit.NANOSECONDS.toSeconds(t1 - t0) + "s\n");

        System.out.println("Count using Hyperloglog");
        Dataset<Row> resDF = newDF.groupBy(col("bannerId"))
                .agg(hll_init_agg("guid").as("guid_hll"))
                .groupBy(col("bannerId"))
                .agg(hll_merge("guid_hll").as("guid_hll"));

        resDF.select(col("bannerId"), hll_cardinality("guid_hll").as("count"))
                .orderBy(desc("count"))
                .show(false);

        long t2 = System.nanoTime();
        System.out.println("Execution time: " + TimeUnit.NANOSECONDS.toSeconds(t2 - t1) + "s\n");
    }

    /**
     *  Đếm số lượng user theo từng banner trong một Khoảng thời gian cụ thể từ Mysql.
     * @param startTime
     * @param endTime
     */
    public void countDistinctFromMysql(String startTime, String endTime){
        Dataset<Row> df = spark.read()
                .format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", "jdbc:mysql://10.3.105.61:3506/intern2022")
                .option("dbtable", "logs")
                .option("user", "phuld")
                .option("password", "12012001")
                .load();

        long t0 = System.nanoTime();
        df.filter(col("Day").geq(startTime)).filter(col("Day").lt(endTime))
                .groupBy(col("Day"), col("bannerId"))
                .agg(hll_merge("guid_hll").as("guid_hll"))
                .select(col("bannerId"), hll_cardinality("guid_hll").as("count"))
                .orderBy(desc("count"))
                .show(false);

        long t1 = System.nanoTime();
        System.out.println("Execution time: " + TimeUnit.NANOSECONDS.toSeconds(t1 - t0) + "s\n");
    }

    /**
     * Bắt đầu chương trình
     */
    public void run(String startTime, String endTIme) {
        this.spark = SparkSession.builder()
                .appName("Count distinct bannerId")
                .master("yarn")
                .getOrCreate();
        this.spark.sparkContext().setLogLevel("ERROR");

        System.out.println("From Mysql");
        countDistinctFromMysql(startTime, endTIme);

        System.out.println("From HDFS");
        countDistinctFromHDFS(startTime, endTIme);

    }

    /**
     *
     * @param args
     */
    public static void main(String[] args) {
        CountDistinct app = new CountDistinct();

        String startTime = args[0];
        String endTime = args[1];
        app.run(startTime, endTime);
    }
}
