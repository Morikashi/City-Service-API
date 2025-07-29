from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time


def create_spark_session():
    """Create Spark session with comprehensive S3A configuration"""
    return SparkSession.builder \
        .appName("SMS_Revenue_Analytics") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.6") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
 \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3") \
        .config("spark.hadoop.fs.s3a.retry.interval", "1000") \
 \
        .config("spark.hadoop.fs.s3a.connection.maximum", "15") \
        .config("spark.hadoop.fs.s3a.threads.max", "10") \
        .config("spark.hadoop.fs.s3a.threads.core", "5") \
        .config("spark.hadoop.fs.s3a.max.total.tasks", "5") \
        .config("spark.hadoop.fs.s3a.multipart.size", "67108864") \
        .config("spark.hadoop.fs.s3a.multipart.threshold", "134217728") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.block.size", "33554432") \
 \
        .config("spark.hadoop.fs.s3a.committer.name", "file") \
        .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "append") \
        .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/tmp/staging") \
        .config("spark.hadoop.fs.s3a.committer.staging.unique-filenames", "true") \
 \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.connection.ttl", "60000") \
        .config("spark.hadoop.fs.s3a.connection.keepalive", "60000") \
 \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()


def main():
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # Reduce log verbosity

    print("=== SMS Revenue Analytics Started ===")

    # Define schemas with corrected data type instantiation
    ref_schema = StructType([
        StructField("PayType", IntegerType(), True),
        StructField("value", StringType(), True)
    ])

    sms_schema = StructType([
        StructField("ROAMSTATE_519", IntegerType(), True),
        StructField("CUST_LOCAL_START_DATE_15", StringType(), True),
        StructField("CDR_ID_1", StringType(), True),
        StructField("CDR_SUB_ID_2", IntegerType(), True),
        StructField("CDR_TYPE_3", StringType(), True),
        StructField("SPLIT_CDR_REASON_4", StringType(), True),
        StructField("RECORD_DATE", StringType(), True),
        StructField("PAYTYPE_515", IntegerType(), True),
        StructField("DEBIT_AMOUNT_42", LongType(), True),
        StructField("SERVICEFLOW_498", IntegerType(), True),
        StructField("EVENTSOURCE_CATE_17", StringType(), True),
        StructField("USAGE_SERVICE_TYPE_19", IntegerType(), True),
        StructField("SPECIALNUMBERINDICATOR_534", IntegerType(), True),
        StructField("BE_ID_30", IntegerType(), True),
        StructField("CALLEDPARTYIMSI_495", StringType(), True),
        StructField("CALLINGPARTYIMSI_494", StringType(), True)
    ])

    try:
        # Test MinIO connection first
        print("Testing MinIO connection...")
        try:
            # Try to list bucket contents
            hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
            hadoop_conf.set("fs.s3a.endpoint", "http://localhost:9000")
            hadoop_conf.set("fs.s3a.access.key", "minioadmin")
            hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
            hadoop_conf.set("fs.s3a.path.style.access", "true")
            hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

            # Set all timeout values explicitly at runtime
            hadoop_conf.set("fs.s3a.connection.timeout", "60000")
            hadoop_conf.set("fs.s3a.socket.timeout", "60000")
            hadoop_conf.set("fs.s3a.connection.establish.timeout", "60000")
            hadoop_conf.set("fs.s3a.connection.request.timeout", "60000")
            hadoop_conf.set("fs.s3a.connection.ttl", "60000")
            hadoop_conf.set("fs.s3a.connection.keepalive", "60000")

            print("MinIO configuration applied successfully.")

        except Exception as config_error:
            print(f"MinIO configuration warning: {config_error}")

        # Load reference data
        print("Loading reference data...")
        ref_df = spark.read \
            .option("header", "true") \
            .option("delimiter", ",") \
            .schema(ref_schema) \
            .csv("s3a://data/ref-data/Ref.csv")

        print("Reference data loaded successfully:")
        ref_df.show()

        # Load and process streaming SMS data
        print("Setting up streaming data source...")
        sms_stream = spark.readStream \
            .option("header", "true") \
            .option("delimiter", "|") \
            .schema(sms_schema) \
            .csv("s3a://data/input-data/")

        # Convert timestamps and amounts
        sms_processed = sms_stream \
            .withColumn("timestamp", to_timestamp(col("RECORD_DATE"), "M/d/yyyy h:mm:ss a")) \
            .withColumn("revenue_tomans", col("DEBIT_AMOUNT_42") / 10000.0) \
            .filter(col("timestamp").isNotNull() & col("DEBIT_AMOUNT_42").isNotNull())

        # Join with reference data
        sms_enriched = sms_processed.join(broadcast(ref_df),
                                          sms_processed.PAYTYPE_515 == ref_df.PayType,
                                          "left") \
            .withColumnRenamed("value", "paytype_name") \
            .drop("PayType")

        print("Starting streaming queries...")

        # Report 1: Daily Revenue
        daily_revenue = sms_enriched \
            .withWatermark("timestamp", "30 minutes") \
            .groupBy(date_format(col("timestamp"), "yyyy-MM-dd").alias("date")) \
            .agg(
            sum("revenue_tomans").alias("daily_revenue_tomans"),
            count("*").alias("total_transactions")
        )

        daily_query = daily_revenue.writeStream \
            .outputMode("complete") \
            .format("csv") \
            .option("header", "true") \
            .option("path", "s3a://data/reports/daily_revenue_tomans") \
            .option("checkpointLocation", "s3a://data/checkpoints/daily_revenue") \
            .trigger(processingTime="1 minute") \
            .start()

        # Report 2: 15-minute Revenue by PayType
        revenue_15min = sms_enriched \
            .withWatermark("timestamp", "30 minutes") \
            .groupBy(
            window(col("timestamp"), "15 minutes").alias("time_window"),
            col("paytype_name")
        ) \
            .agg(sum("revenue_tomans").alias("revenue_tomans")) \
            .select(
            col("time_window.start").alias("window_start"),
            col("time_window.end").alias("window_end"),
            col("paytype_name"),
            col("revenue_tomans")
        )

        revenue_15min_query = revenue_15min.writeStream \
            .outputMode("append") \
            .format("csv") \
            .option("header", "true") \
            .option("path", "s3a://data/reports/15min_revenue_by_paytype") \
            .option("checkpointLocation", "s3a://data/checkpoints/15min_revenue") \
            .trigger(processingTime="1 minute") \
            .start()

        # Report 3: Max/Min Revenue by PayType
        minmax_revenue = sms_enriched \
            .withWatermark("timestamp", "30 minutes") \
            .groupBy(
            window(col("timestamp"), "15 minutes").alias("time_window"),
            col("paytype_name")
        ) \
            .agg(
            max("revenue_tomans").alias("max_revenue_tomans"),
            min("revenue_tomans").alias("min_revenue_tomans"),
            avg("revenue_tomans").alias("avg_revenue_tomans")
        ) \
            .select(
            col("time_window.start").alias("window_start"),
            col("time_window.end").alias("window_end"),
            col("paytype_name"),
            col("max_revenue_tomans"),
            col("min_revenue_tomans"),
            col("avg_revenue_tomans")
        )

        minmax_query = minmax_revenue.writeStream \
            .outputMode("append") \
            .format("csv") \
            .option("header", "true") \
            .option("path", "s3a://data/reports/15min_minmax_by_paytype") \
            .option("checkpointLocation", "s3a://data/checkpoints/minmax_revenue") \
            .trigger(processingTime="1 minute") \
            .start()

        # Report 4: Revenue and Count by PayType
        revenue_count = sms_enriched \
            .withWatermark("timestamp", "30 minutes") \
            .groupBy(
            window(col("timestamp"), "15 minutes").alias("time_window"),
            col("paytype_name")
        ) \
            .agg(
            sum("revenue_tomans").alias("revenue_tomans"),
            count("*").alias("record_count")
        ) \
            .select(
            col("time_window.start").alias("window_start"),
            col("time_window.end").alias("window_end"),
            col("paytype_name"),
            col("revenue_tomans"),
            col("record_count")
        )

        revenue_count_query = revenue_count.writeStream \
            .outputMode("append") \
            .format("csv") \
            .option("header", "true") \
            .option("path", "s3a://data/reports/15min_revenue_count_by_paytype") \
            .option("checkpointLocation", "s3a://data/checkpoints/revenue_count") \
            .trigger(processingTime="1 minute") \
            .start()

        print("All streaming queries started successfully!")
        print("Reports will be generated in s3a://data/reports/")
        print("Press Ctrl+C to stop the application...")

        # Keep the application running
        daily_query.awaitTermination()

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise e
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
