from pyspark import pipelines as dp
from pyspark.sql.functions import *
dp.create_streaming_table("session_21_firststep.default.api_ingest")

@dp.append_flow(target = "session_21_firststep.default.api_ingest", name = "api_ingest_json")
def Bronze_api_ingest():
    return(
         spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("pathGlobFilter", "*.json")
        .load("/Volumes/workspace/default/weather_landing/weather_data/")
        .withColumn("_file_name",col("_metadata.file_name"))
        .withColumn("_load_dttm",current_timestamp())
    )

