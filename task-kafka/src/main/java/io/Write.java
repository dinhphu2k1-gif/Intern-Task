package io;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import static org.apache.spark.sql.functions.col;
import static com.swoop.alchemy.spark.expressions.hll.functions.hll_init_agg;
import static com.swoop.alchemy.spark.expressions.hll.functions.hll_merge;

import java.util.concurrent.TimeoutException;

public class Write {
    /**
     * Nơi lưu trữ dữ liệu đọc từ Kafka.
     */
    private final String destinationPath = "/data/task-kafka";

    /**
     *  Lưu giữ các điểm kiểm tra phục vụ cho việc phục hồi dữ liệu.
     */
    private final String checkpoint = "/tmp/sparkcheckpoint1";

    /**
     * SparkSession.
     */
    private SparkSession spark;

    /**
     * Ghi dữ liệu đọc được từ Kafka vào HDFS.
     * Cứ sau 1 giờ sẽ cập nhật dữ liệu từ Kafka 1 lần
     */
    public void writeToHDFS() {
        Read read = new Read(spark);
        Dataset<Row> df = read.readKafka();

        try {
            df.coalesce(1).writeStream()
                    .trigger(Trigger.ProcessingTime("1 hour"))
                    .partitionBy("day")
                    .format("parquet")
                    .option("path", destinationPath)
                    .option("checkpointLocation", checkpoint)
                    .outputMode("append")
                    .start()
                    .awaitTermination();
        } catch (TimeoutException | StreamingQueryException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     */
    public void writeToMysql(){
        Read read = new Read(spark);
        Dataset<Row> df = read.readKafka();

        try {
            df.coalesce(1).writeStream()
                    .trigger(Trigger.ProcessingTime("1 hour"))
                    .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batchDF, batchId) ->
                                    batchDF.groupBy(col("day"), col("bannerId"))
                                            .agg(hll_init_agg("guid")
                                                    .as("guid_hll"))
                                            .groupBy(col("day"), col("bannerId"))
                                            .agg(hll_merge("guid_hll")
                                                    .as("guid_hll"))
                                            .write()
                                            .option("truncate", "true")
                                            .format("jdbc")
                                            .option("driver", "com.mysql.cj.jdbc.Driver")
                                            .option("url", "jdbc:mysql://10.3.105.61:3506/intern2022")
                                            .option("dbtable", "logs")
                                            .option("user", "phuld")
                                            .option("password", "12012001")
                                            .mode("append")
                                            .save()
                    )
//                    .outputMode("append")
                    .start()
                    .awaitTermination();
        } catch (TimeoutException | StreamingQueryException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Bắt đầu chạy chương trình
     * @param function
     */
    public void run(String function) {
        this.spark = SparkSession
                .builder()
                .appName("Read write data")
                .master("yarn")
                .getOrCreate();

        if (function == "mysql") {
            writeToMysql();
        }
        else if (function == "hdfs") {
            writeToHDFS();
        }
    }

    /**
     * Main Application.
     * @param args
     */
    public static void main(String[] args) {
        Write write = new Write();

        String function = args[0];
        write.run(function);
    }
}
