# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *

# COMMAND ----------

spark.sql("create catalog if not exists session_21_firststep")
spark.sql("create volume if not exists session_21_firststep.default.fistproject2")

# COMMAND ----------

def write_csv_to_volume(file_name:str) -> None:
    current_user = spark.sql("SELECT current_user()").collect()[0][0]
    source_path = f'/Workspace/Users/{current_user}/.bundle/SDP-AUTOLOADER/dev/files/data_set/{file_name}.csv'
    volume_location = f'/Volumes/session_21_firststep/default/fistproject2/{file_name}.csv'
    try:
        dbutils.fs.rm(volume_location)
    except:
        pass
    try:
        dbutils.fs.cp(source_path, volume_location)
    except Exception:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        content = w.workspace.download(f'/Users/{current_user}/.bundle/SDP-AUTOLOADER/dev/files/data_set/{file_name}.csv')
        dbutils.fs.put(volume_location, content.read().decode('utf-8'), overwrite=True)
    print(f'{file_name} written to {volume_location}')

# COMMAND ----------

write_csv_to_volume("daily_data")

# COMMAND ----------

#add dataset