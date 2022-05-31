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
import java.util.ArrayList;
import java.util.List;

import static com.swoop.alchemy.spark.expressions.hll.functions.hll_init_agg;
import static com.swoop.alchemy.spark.expressions.hll.functions.hll_merge;
import static com.swoop.alchemy.spark.expressions.hll.functions.hll_cardinality;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

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
        String startDate = startTime.substring(0, 10);
        String endDate = endTime.substring(0, 10);

        List<String> listDirs = this.getListDirs(this.sourcePath);

        List<String> ls = new ArrayList<>();
        for (String dir : listDirs) {
            String folder = dir.substring(dir.length() - 10);
            if (folder.compareTo(startDate) >= 0
                & folder.compareTo(endDate) <= 0) {
                ls.add(dir);
            }
        }

        return ls;
    }

    /**
     * Đếm số lượng user theo từng banner trong một Khoảng thời gian cụ thể.
     * @param startTime : thời gian bắt đầu
     * @param endTime : thời gian kết thúc
     */
    public void countDistinct(String startTime, String endTime) {
        List<String> list = this.getListFolder(startTime, endTime);

        Dataset<Row> newDF;

        StructType schema = createStructType(new StructField[]{
                createStructField("time", TimestampType, true),
                createStructField("bannerId", IntegerType, true),
                createStructField("guid", LongType, true)
        });

        newDF = this.spark.createDataFrame(new ArrayList<>(), schema);

        for (String dir : list) {
            Dataset<Row> df = this.spark.read().format("parquet").load(dir);

            newDF = newDF.unionByName(df);
            System.out.println("Finish file: " + dir);
        }
//        newDF.show(false);
        newDF = newDF.filter(col("time").geq(startTime)).filter(col("time").leq(endTime));
//        newDF.show(false);


        Dataset<Row> resDF = newDF.groupBy("bannerId")
                .agg(hll_init_agg("guid").as("guid_hll"))
                .groupBy("bannerId")
                .agg(hll_merge("guid_hll").as("guid_hll"));

        resDF.select(col("bannerId"), hll_cardinality("guid_hll").as("count"))
                .orderBy(desc("count"))
                .show(false);
    }

    /**
     *
     */
    public void run() {
        this.spark = SparkSession.builder()
                .appName("Count distinct bannerId")
                .master("yarn")
                .getOrCreate();

        this.countDistinct("2022-05-30 06:00:00", "2022-05-31 06:00:00");
    }

    /**
     *
     * @param args
     */
    public static void main(String[] args) {
        CountDistinct app = new CountDistinct();
        app.run();
    }
}
