package functions;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
         * @param numRecords: số lượng bản ghi cần lấy, nếu -1 thì trả về toàn bộ bảng
         * @param colName: tên cột cần lấy thông tin
         * @return một DataFrame
         */
        Dataset<Row> newDf = df.groupBy(colNames)
                .agg(count("guid").as("numGUID"))
                .orderBy(col("numGUID").desc());

        if (numRecords == -1){
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

        if (numRecords == -1){
            return newDf;
        }

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