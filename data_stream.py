import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# TODO (done) Create a schema for incoming resources
# Using this information about the JSON object, the student should create a proper schema for Apache Spark
# Structured Streaming. This schema will check data types, and will be used with Spark Catalyst Optimizers.
schema = StructType([
    StructField("crime_id", StringType(), False),
    StructField("original_crime_type_name", StringType(), False),
    StructField("report_date", DateType(), False),
    StructField("call_date", DateType(), False),
    StructField("offense_date", DateType(), False),
    StructField("call_time", StringType(), False),
    StructField("call_date_time", TimestampType(), False),  # timestamp of event time
    StructField("disposition", StringType(), False),
    StructField("address", StringType(), False),
    StructField("city", StringType(), False),
    StructField("state", StringType(), False),
    StructField("agency_id", StringType(), False),
    StructField("address_type", StringType(), False),
    StructField("common_location", StringType(), False)
])


def run_spark_job(spark):

    log4j_logger = spark.sparkContext._jvm.org.apache.log4j
    logger = log4j_logger.LogManager.getLogger(__name__)

    # TODO (done) Create Spark Configuration
    print("load kafka stream")
    df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 300)
        .option("minPartitions", 6)
        .option("subscribe", "org.sfc.crimes")
        .load()
    )

    # TODO (done) extract the correct column from the kafka input resources
    print("cast kafka payload 'value' field to string")
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    print("create service table")
    service_table = (
        kafka_df
        .select(psf.from_json(psf.col('value'), schema)  # deserialize from JSON string with schema
                .alias("VALUE"))
        .select("VALUE.*")  # only keep deserialized JSON columns
    )

    print("create distinct table")
    # TODO (done) select original_crime_type_name and disposition
    distinct_table = (
        service_table
        .selectExpr("original_crime_type_name", "disposition", "call_date_time AS original_crime_time")
        .withWatermark("original_crime_time", "10 minutes")
    )

    # TODO (done) count the number of original crime type
    print("create aggregation stream for count of crime types per time window")
    agg_df = (
        distinct_table
        .groupBy(  # group by time window and original_crime_type_name
            psf.window("original_crime_time", "1 day")
            , "original_crime_type_name")
        .count()
        .sort("window", "count", ascending=False)
    )

    # TODO (done) Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO (done) write output stream
    print("run aggregation stream query")
    query = (
        agg_df.writeStream
        .format("console")
        .outputMode("complete")
        .option("truncate", "false")
        .option("numRows", 30)
        .queryName("Crime_Aggregations")
        .start()
     )

    # TODO (done) attach a ProgressReporter
    query.awaitTermination()

    # TODO (done) get the right radio code json path
    print("read radio code data to dataframe")
    radio_code_df = spark.read.option("multiline", "True").json("radio_code.json")

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code
    # TODO (done) rename disposition_code column to disposition
    print("rename radio_code_df column")
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    print("run join query for crimes and descriptions")
    # TODO (done) join on disposition column
    join_query = (
        distinct_table
        .join(radio_code_df, "disposition", "left")
        .writeStream
        .format("console")
        .outputMode("append")
        .queryName("Crimes_with_description_of_disposition")
        .start()
    )
    join_query.awaitTermination()

    # print("run queries")
    # spark.streams.awaitAnyTermination()  # await termination for all active streaming queries in spark session


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO (done) Create Spark in Standalone mode
    spark = (
        SparkSession
        .builder
        .config('spark.ui.port', 3000)
        .master("local[6]")
        .appName("SanFranciscoCrimeStatistics")
        .getOrCreate()
    )

    logger.info("Spark started")

    # spark.sparkContext.setLogLevel('WARN')

    run_spark_job(spark)

    spark.stop()
