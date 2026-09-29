# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import functions as F

@dp.materialized_view(
    name="session_21_firststep.default.gold_yearly_df",
    table_properties={"Tier": "silver"},
    cluster_by=["city_name","year"]
)
def gold_yearly_df():
    gold_yearly_df = spark.table("session_21_firststep.default.silver_check_sdp")
    gold_yearly_df = (
    gold_yearly_df.withColumn("year", F.year(F.col("datetime").cast("date")))
    .groupBy("city_name", "year")
    .agg(
        F.count("datetime").alias("days_recorded"),
        F.round(F.avg("temperature_2m_mean"), 2).alias("avg_temp_mean"),
        F.round(F.avg("temperature_2m_max"), 2).alias("avg_temp_max"),
        F.round(F.avg("temperature_2m_min"), 2).alias("avg_temp_min"),
        F.max("temperature_2m_max").alias("highest_temp"),
        F.min("temperature_2m_min").alias("lowest_temp"),
        F.round(F.sum("precipitation_sum"), 2).alias("total_precipitation_mm"),
        F.round(F.sum("rain_sum"), 2).alias("total_rain_mm"),
        F.sum(F.when(F.col("rain_sum") > 0, 1).otherwise(0)).alias("rainy_days"),
        F.round(F.avg("wind_speed_10m_max"), 2).alias("avg_wind_speed_max"),
        F.max("wind_gusts_10m_max").alias("max_wind_gust"),
    )
    .orderBy("year")
)
    return gold_yearly_df


@dp.materialized_view(
    name="session_21_firststep.default.gold_monthly_df",
    table_properties={"Tier": "silver"},
    cluster_by=["city_name","year","month"]
)
def gold_monthly_df():
    gold_monthly_df = spark.table("session_21_firststep.default.silver_check_sdp")
    window_temp = Window.partitionBy("city_name", "year").orderBy(F.col("avg_temp_mean").desc())
    window_precip = Window.partitionBy("city_name", "year").orderBy(F.col("total_precipitation_mm").desc())
    gold_monthly_df = (
    gold_monthly_df.withColumn("year", F.year(F.col("datetime").cast("date")))
    .withColumn("month", F.month(F.col("datetime").cast("date")))
    .groupBy("city_name", "year", "month")
    .agg(
        F.count("datetime").alias("days_recorded"),
        F.round(F.avg("temperature_2m_mean"), 2).alias("avg_temp_mean"),
        F.round(F.avg("temperature_2m_max"), 2).alias("avg_temp_max"),
        F.round(F.avg("temperature_2m_min"), 2).alias("avg_temp_min"),
        F.max("temperature_2m_max").alias("highest_temp"),
        F.min("temperature_2m_min").alias("lowest_temp"),
        F.round(F.sum("precipitation_sum"), 2).alias("total_precipitation_mm"),
        F.round(F.sum("rain_sum"), 2).alias("total_rain_mm"),
        F.sum(F.when(F.col("rain_sum") > 0, 1).otherwise(0)).alias("rainy_days"),
        F.round(F.avg("wind_speed_10m_max"), 2).alias("avg_wind_speed_max"),
        F.max("wind_gusts_10m_max").alias("max_wind_gust"),
    )
    .withColumn("temp_rank", F.dense_rank().over(window_temp))
    .withColumn("precip_rank", F.dense_rank().over(window_precip))
    .orderBy("city_name", "year", "month")
)
    return gold_monthly_df