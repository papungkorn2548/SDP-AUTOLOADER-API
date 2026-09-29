# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql.functions import *

#@dp.table(name = "session_21_firststep.default.customer")
dp.create_streaming_table("session_21_firststep.default.weather_api")


@dp.append_flow(target = "session_21_firststep.default.weather_api")
def Bronze_setup():
    return(
         spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("pathGlobFilter", "*.csv")
        .load("/Volumes/session_21_firststep/default/fistproject2")
        .withColumn("_file_name",col("_metadata.file_name"))
        .withColumn("_load_dttm",current_timestamp())
    )