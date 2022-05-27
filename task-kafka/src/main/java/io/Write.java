package io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.types.DataTypes.*;

public class Write {
    private final String destinationPath = "/result/task-kafka";
    private final String checkpoint = "/tmp/sparkcheckpoint1/";
    private SparkSession spark;

    public void writeToHDFS(){
        Read read = new Read(spark);
        Dataset<Row> df = read.readKafka();

        try {
            df.coalesce(1).writeStream()
                    .trigger(Trigger.ProcessingTime("1 hours"))
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
