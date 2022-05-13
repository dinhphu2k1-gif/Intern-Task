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
    private MyFunctions functions;
    private Dataset<Row> df;
    private List<String> directories;
    private final String destination = "hdfs:/result/task1/apcdx/";

    public Dataset<Row> initialDf(){
        Dataset<Row> newDf;

        StructType schema = createStructType(new StructField[]{
                createStructField("date", StringType, true),
                createStructField("bannerId", IntegerType, true),
                createStructField("guid", LongType, true),
                createStructField("domain", BinaryType, true)
        });

        newDf = this.spark.createDataFrame(new ArrayList<>(), schema);
        // create a DataFrame from all files with date time
        for (String directory : this.directories) {
            Dataset<Row> df = this.functions.readParquetFile(directory + "/*");

            df = df.filter("click_or_view == 'false'");
            df = df.select(col("bannerId"), col("guid"), col("domain"));
            df = df.withColumn("date", lit(directory.substring(directory.length() - 10)).cast(StringType));

            newDf = newDf.unionByName(df);
            System.out.println("Finish file: " + directory);
        }

        newDf.printSchema();

        return newDf;
    }

    public void start() throws IOException {
        this.spark = SparkSession.builder().appName("Task APCDX").master("yarn").getOrCreate();

        this.functions = new MyFunctions(this.spark);

        //Load đường dẫn
        this.directories = functions.getListDirs("hdfs://internship-hadoop105185:8220/data/apcdx/");

        this.df = this.initialDf();

        Dataset<Row> df1 = this.df.drop("domain");

        /*
            Đếm số GUID theo từng bannerid theo ngày
         */
        Dataset<Row> res1 = functions.topBasedMaxGuid(df1, -1, col("date"), col("bannerId"));

        functions.writeParquet(res1,  destination + "ex1");

        /*
            Đếm số GUID theo từng bannerid theo tháng
         */
        Dataset<Row> df2 = res1.withColumn("month", lit(res1.col("date").substr(0, 7)))
                                .withColumnRenamed("numGUID", "guid")
                                .drop("date");

        Dataset<Row> res2 = functions.topBasedMaxGuid(df2, -1, col("month"), col("bannerId"));

        functions.writeParquet(res2, destination + "ex2");

        /*
            Tính toán việc phân bổ bannerid theo từng domain
         */
        Dataset<Row> df3 = this.df.drop("guid")
                .drop("date");

        Dataset<Row> res3 = functions.topBasedMaxBanner(df3, -1, col("domain"));

        functions.writeParquet(res3, destination + "ex3");
    }
    public static void main(String[] args) throws IOException {
        Task app = new Task();
        app.start();
    }
}
