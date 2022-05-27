package io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.apache.spark.sql.types.DataTypes.LongType;

import static com.swoop.alchemy.spark.expressions.hll.functions.*;

public class Read {
    private final static String kafka_servers = "10.3.68.20:9092," +
            "10.3.68.21:9092," +
            "10.3.68.23:9092," +
            "10.3.68.26:9092," +
            "10.3.68.28:9092," +
            "10.3.68.32:9092," +
            "10.3.68.47:9092," +
            "10.3.68.48:9092," +
            "10.3.68.50:9092," +
            "10.3.68.52:9092";
    private final static String topic = "rt-queue_1";
    private SparkSession spark;

    public Read(SparkSession spark) {
        this.spark = spark;
    }

    public Dataset<Row> readKafka(){
        /*
            Read data from kafka and init HLL sketch (a binary column) for each row
         */
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafka_servers)
                .option("subscribe", topic)
//                .option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST(value AS STRING) AS value")
                ;

        df = df.select(split(col("value"), "\t").as("split(value)"));

        Dataset<Row> midDf = df.select(col("split(value)").getItem(0).cast(LongType).cast(TimestampType).as("time"),
                col("split(value)").getItem(4).cast(IntegerType).as("bannerId"),
                col("split(value)").getItem(6).cast(LongType).as("guid"))
                ;

        midDf = midDf
//                .withColumn("guid_hll", hll_init("guid"));
                .groupBy("time", "bannerId").agg(hll_init_agg("guid").as("id_hll"))
                .groupBy("time", "bannerId").agg(hll_merge("id_hll").as("id_hll"))
                .withColumn("date", to_date(col("time"), "yyyy-MM-dd"))
                ;

        return midDf;
    }
}
