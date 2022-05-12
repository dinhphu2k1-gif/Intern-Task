package functions;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

public class MyFunctions {
    private SparkSession spark;
    private String appName;

    public MyFunctions() {
    }

    public MyFunctions(SparkSession spark, String appName) {
        this.spark = spark;
        this.appName = appName;
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
         * @param colName: tên cột cần lấy thông tin
         * @param numRecords: số lượng bản ghi cần lấy
         * @return một DataFrame với 2 cột: $colName và count(guid) (BinaryType và IntegetType)
         */
        Dataset<Row> newDf = df.groupBy(colNames)
                .agg(count("guid").as("numGUID"))
                .orderBy(col("numGUID").desc());
        return newDf.limit(numRecords);
    }

    public long countSubstring(Dataset<Row> df, String colName, String subString){
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
}
