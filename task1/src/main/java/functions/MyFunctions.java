package functions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

public class MyFunctions {
    private SparkSession spark;

    public MyFunctions() {
    }

    public MyFunctions(SparkSession spark) {
        this.spark = spark;
    }

    public Dataset<Row> readParquetFile(String path) {
        return this.spark.read().format("parquet").load(path);
    }

    public void writeParquet(Dataset<Row> df, String path) {
        df.write().mode("overwrite").parquet(path);
    }

    public Dataset<Row> topBasedMaxGuid(Dataset<Row> df, int numRecords, Column... colNames) {
        /**
         * Hàm lấy 1 DataFrame, tên cột và trả về số lượng records của cột dựa trên số lượng GUID nhiều nhất
         *
         * @param df: bảng dữ liệu
         * @param numRecords: số lượng bản ghi cần lấy, nếu -1 thì trả về toàn bộ bảng
         * @param colName: tên cột cần lấy thông tin
         * @return một DataFrame
         */
        Dataset<Row> newDf = df.groupBy(colNames)
                .agg(count("guid").as("numGUID"))
                .orderBy(col("numGUID").desc());

        if (numRecords == -1) {
            return newDf;
        }

        return newDf.limit(numRecords);
    }

    public Dataset<Row> topBasedMaxBanner(Dataset<Row> df, int numRecords, Column... colNames) {
        /**
         * Hàm lấy 1 DataFrame, tên cột và trả về số lượng records của cột dựa trên số lượng BannerId nhiều nhất
         *
         * @param df: bảng dữ liệu
         * @param numRecords: số lượng bản ghi cần lấy, nếu -1 thì trả về toàn bộ bảng
         * @param colName: tên cột cần lấy thông tin
         * @return một DataFrame
         */
        Dataset<Row> newDf = df.groupBy(colNames)
                .agg(count("bannerId").as("numBannerId"))
                .orderBy(col("numBannerId").desc());

        if (numRecords == -1) {
            return newDf;
        }

        return newDf.limit(numRecords);
    }

    public long countSubstring(Dataset<Row> df, String colName, String subString) {
        /**
         *Hàm lấy 1 DataFrame, tên cột, chuỗi con và trả về số lượng bản ghi chứa chuỗi con đó
         *
         * @param df: bảng dữ liệu
         * @param colName: tên cột cần lọc
         * @param subString: chuỗi con cần lọc
         * @return số lượng bản ghi chứa chuỗi con
         */
        return df.filter(col(colName).rlike(subString)).count();
    }

    public List<String> getListDirs(String directory){
        /**
         * Hàm để lấy tất cả folder trong 1 đường dẫn
         */
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
}
