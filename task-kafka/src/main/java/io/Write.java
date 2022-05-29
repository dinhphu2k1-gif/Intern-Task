package io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;

public class Write {
    private final String destinationPath = "/data/task-kafka";
    private final String checkpoint = "/tmp/sparkcheckpoint1/";
    private SparkSession spark;

    /**
     * Ghi dữ liệu đọc được từ Kafka vào HDFS
     * Cứ sau 30p sẽ cập nhật dữ liệu từ Kafka 1 lần
     */
    public void writeToHDFS() {
        Read read = new Read(spark);
        Dataset<Row> df = read.readKafka();

        try {
            df.coalesce(1).writeStream()
                    .trigger(Trigger.ProcessingTime("30 minutes"))
                    .partitionBy("date")
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
     * Bắt đầu chạy chương trình
     */
    public void run() {
        this.spark = SparkSession.builder().appName("Read write data").master("yarn").getOrCreate();
        writeToHDFS();
    }

    public static void main(String[] args) {
        Write write = new Write();
        write.run();
    }
}
