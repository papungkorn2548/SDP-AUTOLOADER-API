# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql import functions as F

@dp.append_flow(
    target="session_21_firststep.default.weather_api",
    name="staging_to_weather_api"
)
def append_staging_to_weather_api():
    bronze_df = spark.readStream.table("session_21_firststep.default.api_ingest")
    change_name_df = bronze_df.withColumnRenamed("_city", "city_name")
    change_name_df = change_name_df.filter(col("city_name").isin("London","Bangkok","Tokyo"))
    daily_schema = "struct<time:array<string>, weather_code:array<string>, temperature_2m_max:array<string>, temperature_2m_min:array<string>,temperature_2m_mean:array<string>,apparent_temperature_max:array<string>,apparent_temperature_min:array<string>>"
    change_name_df = change_name_df.withColumn("daily", from_json(col("daily"), daily_schema))
    df_daily = (
    change_name_df
    .select(
        F.col("city_name"),
        F.explode(
            F.arrays_zip(
                "daily.time",
                "daily.weather_code",
                "daily.temperature_2m_max",
                "daily.temperature_2m_min",
                "daily.temperature_2m_mean",
                "daily.apparent_temperature_max",
                "daily.apparent_temperature_min"
            )
        ).alias("daily"),
        F.col("_file_name"),
    ).withColumn("_load_dttm",current_timestamp())
    .select(
        F.col("city_name"),
        F.col("daily.time").alias("datetime"),
        F.col("daily.weather_code").alias("weather_code"),
        F.col("daily.temperature_2m_max").alias("temperature_2m_max"),
        F.col("daily.temperature_2m_min").alias("temperature_2m_min"),
        F.col("daily.temperature_2m_mean").alias("temperature_2m_mean"),
        F.col("daily.apparent_temperature_max").alias("apparent_temperature_max"),
        F.col("daily.apparent_temperature_min").alias("apparent_temperature_min"),
        F.col("_file_name"),
        F.col("_load_dttm")
        )
    )

    return df_daily