import os
import sys
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import DoubleType
from pyspark.sql import DataFrame


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def haversine(lat1, lon1, lat2, lon2):

    r = 6371  # радиус Земли в км

    lat1 = F.radians(lat1)
    lon1 = F.radians(lon1)
    lat2 = F.radians(lat2)
    lon2 = F.radians(lon2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = F.pow(F.sin(dlat / 2), 2) + F.cos(lat1) * F.cos(lat2) * F.pow(F.sin(dlon / 2), 2)
    c = 2 * F.asin(F.sqrt(a))
    return r * c

def input_event_paths(date, days_count, events_base_path):

    dt = datetime.strptime(date, '%Y-%m-%d')
    return [f"{events_base_path}/date={(dt - timedelta(days=x)).strftime('%Y-%m-%d')}" for x in range(days_count)]

def get_events_cities_df(events_base_path, date, days_count, cities_base_path, spark):

    try:
        geo_cities = spark.read.csv(cities_base_path, sep=';', header=True, inferSchema=True) \
            .withColumn("city_lat", F.regexp_replace(F.col("lat"), ',', '.').cast("double")) \
            .withColumn("city_lng", F.regexp_replace(F.col("lng"), ',', '.').cast("double")) \
            .withColumnRenamed("id", "zone_id")

        geo_events_paths = input_event_paths(date, int(days_count), events_base_path)
        events = spark.read.parquet(*geo_events_paths) \
            .withColumn("event_lat", F.col("lat")) \
            .withColumn("event_lon", F.col("lon")) \
            .withColumn("user_id", F.col("event.message_from")) \
            .withColumn("TIME_UTC", F.col("event.datetime"))

        events_city = events.crossJoin(geo_cities) \
            .withColumn("distance", haversine(F.col("event_lat"), F.col("event_lon"), F.col("city_lat"), F.col("city_lng")))

        window = Window.partitionBy("user_id", "TIME_UTC", "event_lat", "event_lon").orderBy("distance")
        events_city = events_city.withColumn("row_number", F.row_number().over(window)) \
            .filter(F.col("row_number") == 1) \
            .drop("row_number", "distance", "lat", "lon")

        return events_city

    except Exception as e:
        logging.error(f"Error in get_events_cities_df: {e}")
        raise

def create_users_view(events_city):

    try:
        user_act_city = events_city.withColumn("msg_date", F.to_date(F.col("TIME_UTC")))

        window = Window.partitionBy("user_id").orderBy(F.desc("msg_date"))
        user_act_city = user_act_city.withColumn("row_number", F.row_number().over(window)) \
            .filter(F.col("row_number") == 1) \
            .drop("row_number")

        home_city_window = Window.partitionBy("user_id", "city").orderBy(F.desc("TIME_UTC"))
        user_home_city = events_city.withColumn("date_group_rank",
            F.row_number().over(home_city_window)) \
            .filter(F.col("date_group_rank") <= 27)

        home_city = user_home_city.groupBy("user_id","city").count().orderBy(F.desc("count")).dropDuplicates(["user_id"]).select("user_id","city")

        user_visited_cities = events_city.groupBy("user_id") \
            .agg(F.collect_set("city").alias("visited_cities"))

        user_activity_view = user_act_city.join(home_city, "user_id", "left").join(user_visited_cities, "user_id", "left")

        return user_activity_view

    except Exception as e:
        logging.error(f"Error in create_users_view: {e}")
        raise

def generic_week_month_transformations(df, date_field_name, entity, event_type):

    df = df.withColumn("week", F.date_format(F.col(date_field_name), "yyyy-WW"))\
        .withColumn("month", F.date_format(F.col(date_field_name), "yyyy-MM"))

    week_entity = f"week_{entity}"
    month_entity = f"month_{entity}"
    agg_df = df.groupBy("week", "month", "zone_id") \
        .agg(
            F.sum(F.when(F.col("event_type") == event_type, 1).otherwise(0)).alias(week_entity),
            F.sum(F.when(F.col("event_type") == event_type, 1).otherwise(0)).alias(month_entity)
    )
    return agg_df

def create_recommendation_view(user_activity_view, events_city):

    try:
        user_pairs = events_city.alias("a").join(events_city.alias("b"),
                                                  (F.col("a.zone_id") == F.col("b.zone_id")) & (F.col("a.user_id") != F.col("b.user_id")),
                                                  "inner")
        user_pairs = user_pairs.filter(F.col("a.user_id") < F.col("b.user_id"))

        user_pairs = user_pairs.withColumn("distance", haversine(F.col("a.event_lat"), F.col("a.event_lon"), F.col("b.event_lat"), F.col("b.event_lon")))

        recommendations = user_pairs.filter((F.col("distance") <= 1)
                                             & (F.col("a.event_type") == "subscription")
                                             & (F.col("b.event_type") == "subscription"))

        recommendations = recommendations.select(
            F.col("a.user_id").alias("user_left"),
            F.col("b.user_id").alias("user_right"),
            F.current_timestamp().alias("processed_dttm"),
            F.col("a.zone_id").alias("zone_id"),
            F.col("a.TIME_UTC").alias("local_time")
        ).distinct()

        return recommendations

    except Exception as e:
        logging.error(f"Error in create_recommendation_view: {e}")
        raise

def main(date, depth, events_base_path, cities_base_path, recommendations_path, geo_layer_path):
    
    spark = SparkSession.builder \
        .appName("geo_analysis") \
        .getOrCreate()

    try:

        events_city = get_events_cities_df(events_base_path, date, depth, cities_base_path, spark)


        user_activity_view = create_users_view(events_city)


        recommendations_view = create_recommendation_view(user_activity_view, events_city)


        messages = generic_week_month_transformations(events_city, "TIME_UTC", "message", "message")
        subscriptions = generic_week_month_transformations(events_city, "TIME_UTC", "subscription", "subscription")
        registrations = generic_week_month_transformations(events_city, "TIME_UTC", "registration", "message")
        geo_layer = messages \
            .join(subscriptions, ["week", "month", "zone_id"], 'left') \
            .join(registrations, ["week", "month", "zone_id"], 'left')


        recommendations_view.write.mode("overwrite").parquet(recommendations_path)
        geo_layer.write.mode("overwrite").parquet(geo_layer_path)
        logging.info("Success!!")

    except Exception as e:
        logging.error(f"Job finished with error {e}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    date = sys.argv[1] if len(sys.argv) > 1 else "2022-05-30"
    depth = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    events_base_path = sys.argv[3] if len(sys.argv) > 3 else "/user/master/data/geo/events"
    cities_base_path = sys.argv[4] if len(sys.argv) > 4 else "/user/malina692/data/geo/geo.csv"
    recommendations_path = sys.argv[5] if len(sys.argv) > 5 else "/user/malina692/geo/recommendations"
    geo_layer_path = sys.argv[6] if len(sys.argv) > 6 else "/user/malina692/geo/geo_layer"


    main(date, depth, events_base_path, cities_base_path, recommendations_path, geo_layer_path)