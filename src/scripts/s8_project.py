import os
from datetime import datetime

from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct, current_timestamp, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, BooleanType

# Определяем константы для топиков и таблицы фидбеков
TOPIC_NAME_OUT = 'student.topic.cohort31.malina692.out'
TOPIC_NAME_IN = 'student.topic.cohort31.malina692.in'

# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()

    # записываем df в PostgreSQL с полем feedback
    df.withColumn("feedback", lit("Action processed successfully")) \
      .write \
      .format("jdbc") \
      .option("url", "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de") \
      .option("dbtable", 'public.subscribers_feedback') \
      .option("user", "student") \
      .option("password", "de-student") \
      .mode("append")

    # создаём df для отправки в Kafka. Сериализация в json.
    kafka_df = df.drop("feedback")
    kafka_df = kafka_df.withColumn("value", to_json(struct(*kafka_df.columns)))

    # параметры для подключения к Kafka
    kafka_bootstrap_servers = "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091"
    kafka_security_protocol = 'SASL_SSL'
    kafka_sasl_mechanism = 'SCRAM-SHA-512'
    kafka_sasl_jaas_config = 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";'


    # отправляем сообщения в результирующий топик Kafka без поля feedback
    kafka_df.select("value") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("topic", TOPIC_NAME_OUT) \
            .option('kafka.security.protocol', kafka_security_protocol) \
            .option('kafka.sasl.jaas.config', kafka_sasl_jaas_config) \
            .option('kafka.sasl.mechanism', kafka_sasl_mechanism) \
            .option("checkpointLocation", "kafka_checkpoint_location") \
            .mode("append") \
            .save()

    # очищаем память от df
    df.unpersist()

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

# читаем из топика Kafka сообщения с акциями от ресторанов
restaurant_read_stream_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
    .option('kafka.security.protocol', 'SASL_SSL') \
    .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username="login" password="password";') \
    .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
    .option('subscribe', TOPIC_NAME_IN) \
    .load()

# определяем схему входного сообщения для json
incomming_message_schema = StructType([
    StructField("restaurant_id", StringType(), True),
    StructField("action_id", StringType(), True),
    StructField("action_name", StringType(), True),
    StructField("start_time", LongType(), True),
    StructField("end_time", LongType(), True),
    StructField("discount", IntegerType(), True),
    StructField("description", StringType(), True)
])

# определяем текущее время в UTC в миллисекундах, затем округляем до секунд
current_timestamp_utc = f.round(f.unix_timestamp(f.current_timestamp())).cast("int")

# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
parsed_restaurant_df = restaurant_read_stream_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", incomming_message_schema).alias("data")) \
    .select("data.*")

filtered_restaurant_df = parsed_restaurant_df.filter(
    (col("start_time") <= current_timestamp_utc) & (col("end_time") >= current_timestamp_utc)
)

# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = spark.read \
    .format('jdbc') \
    .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
    .option('driver', 'org.postgresql.Driver') \
    .option('dbtable', 'subscribers_restaurants') \
    .option('user', 'student') \
    .option('password', 'de-student') \
    .load()

# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
result_df = filtered_restaurant_df.join(subscribers_restaurant_df,
                                          filtered_restaurant_df.restaurant_id == subscribers_restaurant_df.restaurant_id) \
    .withColumn("created_at", current_timestamp())

# запускаем стриминг
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()