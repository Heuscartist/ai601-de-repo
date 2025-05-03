from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, count, when, sum, lag, avg, current_timestamp, desc, col, window, from_json
from pyspark.sql import functions as F
from pyspark.sql.functions import sum as _sum


# SUBSCRIBING TO traffic_data TOPIC (modified code as lab5)
spark = SparkSession.builder \
    .appName("TrafficAnalysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic_data") \
    .option("startingOffsets", "latest") \
    .load()

schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("average_speed", DoubleType(), True),
    StructField("congestion_level", StringType(), True)
])

json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data"))
events_df = parsed_df.select("data.*")


# FILTERING THE EVENTS
events_df = events_df.filter(col("sensor_id").isNotNull() & col("timestamp").isNotNull())
events_df = events_df.filter((col("vehicle_count") >= 0) & (col("average_speed") > 0))
events_df = events_df.dropDuplicates(["sensor_id", "timestamp"])


# COUNTING CONGESTION LEVEL AND FINDING HIGH RATIO (modfied from lab5)
windowed_df = events_df \
    .groupBy(
        window(current_timestamp(), "1 minutes"),
        col("sensor_id")
    ) \
    .agg(
        count(when(col("congestion_level") == "LOW", 1)).alias("low_count"),
        count(when(col("congestion_level") == "MEDIUM", 1)).alias("medium_count"),
        count(when(col("congestion_level") == "HIGH", 1)).alias("high_count")
    )

windowed_df = windowed_df.withColumn(
    "high_ratio",
    col("high_count") / (col("high_count") + col("medium_count") + col("low_count"))
)

def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print("No data in this batch.")
        return

    w = Window.partitionBy("sensor_id", "window").orderBy(desc("high_count"))

    ranked_df = batch_df.withColumn("rn", row_number().over(w)) \
                        .filter(col("rn") <= 3)

    print(f"=== Batch: {batch_id} ===")
    ranked_df.show(truncate=False)

query = windowed_df \
    .writeStream \
    .outputMode("update") \
    .trigger(processingTime='5 seconds') \
    .foreachBatch(process_batch) \
    .start()


# SUMMING NUMBER OF CARS PER SENSOR IN 5 MIN WINDOW
volume_df = events_df \
    .groupBy(
        window(current_timestamp(), "5 minutes"),
        col("sensor_id")
    ) \
    .agg(
        sum("vehicle_count").alias("total_vehicles")
    )

def process_volume_batch(batch_df, batch_id):
    if not batch_df.rdd.isEmpty():
        print(f"=== Batch {batch_id}: Real-Time Traffic Volume per Sensor (5 min window) ===")
        batch_df.show()

volume_query = volume_df \
    .writeStream \
    .outputMode("update") \
    .trigger(processingTime='5 seconds') \
    .foreachBatch(process_volume_batch) \
    .start()
    

# CALCULATING AVERAGE SPEED PER SENSOR IN 10 MIN WINDOW
average_speed_df = events_df \
    .groupBy(
        window(current_timestamp(), "10 minutes"),
        col("sensor_id")
    ) \
    .agg(
        avg("average_speed").alias("avg_speed")
    )

def process_avg_speed_batch(batch_df, batch_id):
    if not batch_df.rdd.isEmpty():
        print(f"=== Batch {batch_id}: Average Speed per Sensor (10 min window) ===")
        batch_df.show()

avg_speed_query = average_speed_df \
    .writeStream \
    .outputMode("update") \
    .trigger(processingTime='5 seconds') \
    .foreachBatch(process_avg_speed_batch) \
    .start()
    

# FINDING TOP 3 SENSORS WITH MOST CARS IN LAST 30 MINS
vehicle_count_30min_df = events_df \
    .groupBy(
        window(current_timestamp(), "30 minutes"), 
        col("sensor_id")
    ) \
    .agg(
        _sum("vehicle_count").alias("total_vehicle_count")
    )


def process_busiest_sensors_batch(batch_df, batch_id):
    if not batch_df.rdd.isEmpty():
        w = Window.orderBy(desc("total_vehicle_count"))

        ranked_df = batch_df.withColumn("rank", row_number().over(w)) \
                             .filter(col("rank") <= 3)

        print(f"=== Batch {batch_id}: Top 3 Busiest Sensors in the Last 30 Minutes ===")
        ranked_df.show(truncate=False)

busiest_sensors_query = vehicle_count_30min_df \
    .writeStream \
    .outputMode("update") \
    .trigger(processingTime='5 seconds') \
    .foreachBatch(process_busiest_sensors_batch) \
    .start()
    
# CHECKING FOR SPEED ANOMOLIES (WILL ONLY SHOW DF IF ANOMOLY DETECTED)
average_speed_df = events_df \
    .groupBy(
        window(current_timestamp(), "2 minutes"),
        col("sensor_id")
    ) \
    .agg(
        avg("average_speed").alias("avg_speed")
    )

def process_speed_drop_batch(batch_df, batch_id):
    if not batch_df.rdd.isEmpty():
        w = Window.partitionBy("sensor_id").orderBy("window")
        
        speed_with_lag_df = batch_df.withColumn("prev_avg_speed", F.lag("avg_speed").over(w)) \
                                    .withColumn("speed_drop", (F.col("avg_speed") / F.col("prev_avg_speed")) < 0.5)
        
        anomaly_df = speed_with_lag_df.filter(F.col("speed_drop") == True)
        
        if anomaly_df.count() > 0:
            print(f"=== Batch {batch_id}: Detected Sudden Speed Drops (More than 50%) ===")
            anomaly_df.show(truncate=False)

speed_drop_query = average_speed_df \
    .writeStream \
    .outputMode("update") \
    .trigger(processingTime='5 seconds') \
    .foreachBatch(process_speed_drop_batch) \
    .start()


# CHECKING FOR CONGESTION HOTSPOT ANOMOLIES (WILL ONLY SHOW DF IF CONSECUTIVE 3 HIGH CONGESTIONS)
congestion_windowed_df = events_df \
    .filter(col("congestion_level") == "HIGH") \
    .groupBy(
        window(current_timestamp(), "1 minute"),  
        col("sensor_id")
    ) \
    .agg(
        count("*").alias("high_count")
    )

def process_congestion_hotspot_batch(batch_df, batch_id):
    if not batch_df.rdd.isEmpty():
        w = Window.partitionBy("sensor_id").orderBy("window")
        congestion_with_lag_df = batch_df.withColumn("prev_high_count", F.lag("high_count", 1).over(w)) \
                                         .withColumn("prev_prev_high_count", F.lag("high_count", 2).over(w)) \
                                         .withColumn("consecutive_high", 
                                                     (F.col("high_count") > 0) &
                                                     (F.col("prev_high_count") > 0) &
                                                     (F.col("prev_prev_high_count") > 0))


        hotspot_df = congestion_with_lag_df.filter(F.col("consecutive_high") == True)

        if hotspot_df.count() > 0:
            print(f"=== Batch {batch_id}: Congestion Hotspots Detected ===")
            hotspot_df.show(truncate=False)

congestion_hotspot_query = congestion_windowed_df \
    .writeStream \
    .outputMode("update") \
    .trigger(processingTime='5 seconds') \
    .foreachBatch(process_congestion_hotspot_batch) \
    .start()

query.awaitTermination()
volume_query.awaitTermination()
avg_speed_query.awaitTermination()
busiest_sensors_query.awaitTermination()
speed_drop_query.awaitTermination()
congestion_hotspot_query.awaitTermination()
