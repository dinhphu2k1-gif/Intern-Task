package io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class Write {
    private final String destinationPath = "/result/task-kafka";
    private final String checkpoint = "/tmp/sparkcheckpoint1/";
    private SparkSession spark;

    public void writeToHDFS(){
        Read read = new Read(spark);
        Dataset<Row> df = read.readKafka();

        try {
            df.writeStream()
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

    public void run(){
        this.spark = SparkSession.builder().appName("Read write data").master("yarn").getOrCreate();

        writeToHDFS();
    }

    public static void main(String[] args) {
        Write write = new Write();
        write.run();
    }
}
