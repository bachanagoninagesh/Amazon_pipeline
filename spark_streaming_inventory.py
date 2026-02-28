from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, BooleanType, StringType, TimestampType

# -----------------------------
# Create Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("LogisticsInventoryStreaming") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Define Schema (Data Contract)
# -----------------------------
inventory_schema = StructType([
    StructField("warehouse_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("stock_level", IntegerType()),
    StructField("reorder_flag", BooleanType()),
    StructField("event_time", StringType())
])

# -----------------------------
# Read From Kafka
# -----------------------------
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "inventory_events") \
    .option("startingOffsets", "latest") \
    .load()

# Convert Kafka value (binary) → string
df_json = df_raw.selectExpr("CAST(value AS STRING)")

# Parse JSON
df_parsed = df_json.select(
    from_json(col("value"), inventory_schema).alias("data")
).select("data.*")

# -----------------------------
# Write To PostgreSQL
# -----------------------------
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/logistics_dw") \
        .option("dbtable", "fact_inventory") \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

query = df_parsed.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()