package io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.to_date;

import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

public class Read {
    /**
     * Thông tin về các node và cổng từ Kafka cần lấy dữ liệu.
     */
    private final String kafkaServers = "10.3.68.20:9092,"
            + "10.3.68.21:9092,"
            + "10.3.68.23:9092,"
            + "10.3.68.26:9092,"
            + "10.3.68.28:9092,"
            + "10.3.68.32:9092,"
            + "10.3.68.47:9092,"
            + "10.3.68.48:9092,"
            + "10.3.68.50:9092,"
            + "10.3.68.52:9092";

    /**
     * Topic.
     */
    private final String topic = "rt-queue_1";

    /**
     * SparkSession.
     */
    private SparkSession spark;

    /**
     * Contructor.
     * @param spark
     */
    public Read(SparkSession spark) {
        this.spark = spark;
    }

    /**
     * Read data from kafka.
     * @return : một Dataframe sau khi tách và chuyển đổi kiểu dữ liệu
     */
    public Dataset<Row> readKafka() {
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaServers)
                .option("subscribe", topic)
//                .option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST(value AS STRING) AS value");

        df = df.select(split(col("value"), "\t").as("split(value)"));

        Dataset<Row> midDf = df.select(
                col("split(value)")
                        .getItem(0)
                        .cast(LongType)
                        .cast(TimestampType)
                        .as("time"),
                col("split(value)")
                        .getItem(4)
                        .cast(IntegerType)
                        .as("bannerId"),
                col("split(value)")
                        .getItem(6)
                        .cast(LongType)
                        .as("guid"));

        midDf = midDf.withColumn("date", to_date(col("time"), "yyyy-MM-dd"));
        return midDf;
    }
}
