package com.spark.streaming.kafka.consumer.structured.streaming;

import jakarta.annotation.PostConstruct;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;
//import spark.implicits.*;

@Service
public class SparkStructuredStreamingService {

    @Value(value = "fiserv-coforge-event-produced-spark-stream")
    private String topicReceivingInput;

    @Value(value = "${spring.spark.structured.streaming.with.window.output}")
    private String topicNameWithWindowsFunction;

    @Value(value = "${spring.spark.structured.streaming.without.window.output}")
    private String topicNameWithoutWindowFunction;

    @PostConstruct
    public void initialize() {
        System.out.println(System.currentTimeMillis() + " IInitialized Spark App");
    }

    public void startSparkStructuredStreaming() {

        org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
        SparkConf properties = new SparkConf();
        properties.set("spark.sql.session.timeZone", "UTC");
        properties.set("spark.default.parallelism", String.valueOf(8));
        properties.set("spark.sql.shuffle.partitions", String.valueOf(8));
        String master = System.getProperty("spark.master", "local[" + Runtime.getRuntime().availableProcessors() + "]");
        try (SparkSession spark = SparkSession.builder().config(properties).master("local[*]").appName("coforge-spark-structred-streaming").getOrCreate()) {
            spark.sparkContext().setLogLevel("INFO");
            Dataset<Row> df = spark.read().json("/opt/rightship/codebase/Right-Scope-Zone-Snowflake-Spark-Streaming-Consumer/SparkJson/ExampleJSONA.json");
            Dataset<Row> dfProcessed = df.withColumn("event_time", lit(System.currentTimeMillis()));
            dfProcessed.selectExpr("CAST(value AS STRING)").write().format("kafka").option("kafka.bootstrap.servers", "localhost:9092")
                    .option("topic", topicNameWithWindowsFunction).save();

            StructType jSchema = new StructType().add("id", IntegerType)
                    .add("name", StringType).add("fee", IntegerType)
                    .add("event_time", LongType);
            df.select(from_json(col("value"), jSchema).
                            as("col1")).select(col("col1.*"));
        }
    }

    public void codesHere() {
        import net.snowflake.spark.snowflake.SnowflakeConnectorUtilsimport
    org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, StructType}
        import org.apache.spark.sql.streaming.Triggerimport spark.implicits._import
        org.apache.spark.sql.functions._val inputStream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "input_stream")
                .option("startingOffsets", "earliest").
       load().selectExpr("CAST(value AS STRING)")val
                jSchema = new StructType().add("id", IntegerType).
                add("name", StringType).add("fee", IntegerType).
                add("event_time", LongType)val stream = inputStream.
                select(from_json(col("value"), jSchema).
                        as("col1")).select(col("col1.*"))
        val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"SnowflakeConnectorUtils.
                enablePushdownSession(spark)
        val sfOptions = new scala.collection.mutable.HashMap[String, String]()sfOptions += ("sfURL" ->
        "https://**********.privatelink.snowflakecomputing.com/",
                "sfUser" -> "**********","sfPassword" -> "*********","sfDatabase" -> "UAT","sfSchema" -> "PUBLIC","sfWarehouse" -> "UAT_XSMALL_WH")stream.writeStream.
                trigger(Trigger.ProcessingTime("30 seconds")).
    foreachBatch((ds, dt) => {ds.toDF().show(false)ds.toDF().write.
                format(SNOWFLAKE_SOURCE_NAME).
                options(sfOptions).
                option("dbtable", "members_stage").
                mode(SaveMode.Append).save()}).
        start().awaitTermination(

    }

}
