from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, from_unixtime, struct, to_json
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
from colorama import Fore, Style, init
import os

# Ініціалізація кольорового логування
init(autoreset=True)

# Задаємо ім'я топіка
my_name = "ivan"
topic_name_in = f"{my_name}_sensors_data"
alerts_topic_name = f"{my_name}_alerts"

# Пакети для роботи з Kafka
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# Створення SparkSession
print(f"{Fore.CYAN}Starting Spark session...")
spark = (SparkSession.builder
         .appName("IoT_Sensors_Aggregation")
         .master("local[*]")
         .getOrCreate())
print(f"{Fore.GREEN}Spark session started successfully.")

# Схема JSON для даних із Kafka
iot_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("timestamp", DoubleType(), True)  # Початково DOUBLE
])

# Схема CSV-файлу з умовами для алертів
alerts_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("humidity_min", DoubleType(), True),
    StructField("humidity_max", DoubleType(), True),
    StructField("temperature_min", DoubleType(), True),
    StructField("temperature_max", DoubleType(), True),
    StructField("code", StringType(), True),
    StructField("message", StringType(), True)
])

# Читання умов для алертів із CSV
alerts_conditions_path = "alerts_conditions.csv"
print(f"{Fore.CYAN}Loading alert conditions from {alerts_conditions_path}...")
alerts_df = spark.read.csv(alerts_conditions_path, schema=alerts_schema, header=True)
print(f"{Fore.GREEN}Alert conditions loaded successfully.")

# Читання потоку даних із Kafka
print(f"{Fore.CYAN}Connecting to Kafka topic: {topic_name_in}...")
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("subscribe", topic_name_in) \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option(
        "kafka.sasl.jaas.config",
        'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";'
    ) \
    .load()
print(f"{Fore.GREEN}Connected to Kafka topic: {topic_name_in}")

# Десеріалізація даних і приведення до схеми
print(f"{Fore.CYAN}Parsing and transforming data from Kafka...")
iot_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), iot_schema).alias("data")) \
    .select(
        col("data.id"),
        col("data.temperature"),
        col("data.humidity"),
        from_unixtime(col("data.timestamp").cast("long")).cast("timestamp").alias("timestamp")  # Перетворення в TIMESTAMP
    )
print(f"{Fore.GREEN}Data parsed and transformed successfully.")

# Агрегація: Sliding window (1 хвилина) з інтервалом 30 секунд
print(f"{Fore.CYAN}Starting data aggregation pipeline...")
agg_df = iot_df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(window(col("timestamp"), "1 minute", "30 seconds")) \
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity")
    )
print(f"{Fore.GREEN}Aggregation pipeline created successfully.")

# Перевірка умов для алертів
print(f"{Fore.CYAN}Applying alert conditions...")
alerts = agg_df.crossJoin(alerts_df) \
    .filter(
        (col("avg_temperature") > col("temperature_min")) &
        (col("avg_temperature") < col("temperature_max")) |
        (col("avg_humidity") > col("humidity_min")) &
        (col("avg_humidity") < col("humidity_max"))
    ) \
    .select(
        "window",
        "avg_temperature",
        "avg_humidity",
        "code",
        "message"
    )
print(f"{Fore.GREEN}Alert conditions applied successfully.")

# Виведення результатів алертів у консоль
print(f"{Fore.CYAN}Starting to stream alerts to console...")
alerts_query = alerts.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Підготовка даних для Kafka
alerts_to_kafka = alerts.select(
    to_json(
        struct(
            col("window"),
            col("avg_temperature"),
            col("avg_humidity"),
            col("code"),
            col("message")
        )
    ).alias("value")
)

# Запис алертів у Kafka
print(f"{Fore.CYAN}Streaming alerts to Kafka topic: {alerts_topic_name}...")
alerts_kafka_query = alerts_to_kafka.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("topic", alerts_topic_name) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option(
        "kafka.sasl.jaas.config",
        'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";'
    ) \
    .option("checkpointLocation", "/tmp/kafka_alerts_checkpoint") \
    .start()

# Завершення стримінгу
alerts_query.awaitTermination()
alerts_kafka_query.awaitTermination()

print(f"{Fore.GREEN}Data successfully written to topic: {alerts_topic_name}")
