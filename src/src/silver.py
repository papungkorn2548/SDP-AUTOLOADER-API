from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window

@dp.materialized_view(
    name="session_21_firststep.default.silver_check_sdp",
    table_properties={"Tier": "silver"},
    cluster_by=["city_name","datetime"]
)
@dp.expect_all_or_drop({
    "not_null_city_name": "city_name IS NOT NULL",
    "not_null_datetime": "datetime IS NOT NULL",
    "valid_city_name": "city_name IN ('London', 'Bangkok', 'Tokyo')",
    "not_null_weather_code": "weather_code IS NOT NULL",
    "valid_temperature_max": "CAST(temperature_2m_max AS DOUBLE) BETWEEN -100 AND 70",
    "valid_temperature_min": "CAST(temperature_2m_min AS DOUBLE) BETWEEN -100 AND 70",
    "max_ge_min": "CAST(temperature_2m_max AS DOUBLE) >= CAST(temperature_2m_min AS DOUBLE)",
    "non_negative_precipitation": "CAST(precipitation_sum AS DOUBLE) >= 0",
    "non_negative_rain": "CAST(rain_sum AS DOUBLE) >= 0",
    "not_null_wind_direction": "wind_direction_10m_dominant IS NOT NULL",
    "not_null_precipitation_hours": "precipitation_hours IS NOT NULL",
    "not_null_et0": "et0_fao_evapotranspiration IS NOT NULL"
})
def silver_check_sdp():
    silver_df = spark.table("session_21_firststep.default.silver")
    return silver_df

@dp.temporary_view()
def silver_check_null():
    bronze_u_df = spark.table("session_21_firststep.default.weather_api")
    bronze_u_df = bronze_u_df.filter(col("city_name").isin("London","Bangkok","Tokyo"))
    bronze_u_df = bronze_u_df.withColumn(
        "_is_invalid_city_name",
        coalesce(~col("city_name").rlike("^[A-Za-z]+$"), lit(False))
    ).withColumn(
        "_is_invalid_date_time",
        coalesce(~col("datetime").rlike("^\\d{4}-\\d{2}-\\d{2}$"), lit(False))
    ).withColumn(
        "_is_null_city_name",
        coalesce(col("city_name").isNull(), lit(False))
    ).withColumn(
        "_is_null_date_time",
        coalesce(col("datetime").isNull(), lit(False)))

    data_cols = [c for c in bronze_u_df.columns if c not in ("_file_name", "_load_dttm", "_rescued_data")]
    bronze_u_df = bronze_u_df.withColumn("_sk", sha2(concat_ws("||", *[col(c) for c in data_cols]), 256))
    bronze_u_df = bronze_u_df.withColumn(
        "reason",
        array_compact(
            array(
                when(col("_is_invalid_city_name"), lit("invalid_city_name")),
                when(col("_is_invalid_date_time"), lit("invalid_date_time")),
                when(col("_is_null_city_name"), lit("null_city_name")),
                when(col("_is_null_date_time"), lit("null_date_time"))
            )
        )
    )
    return bronze_u_df.filter(size(col("reason")) == 0).drop(col("_is_invalid_city_name"),col("_is_invalid_date_time"),col("reason"),col("_is_null_date_time"),col("_is_null_city_name"))


@dp.materialized_view(
    name="session_21_firststep.default.silver",
    table_properties={"Tier": "silver"},
    cluster_by=["city_name","datetime"]
)
def SilverLayer():
    bronze_u_df = spark.table("silver_check_null")
    silver_row = spark.table("session_21_firststep.default.silver_row_dupe_quarantine")
    silver_key = spark.table("session_21_firststep.default.silver_key_dupe_quarantine")

    all_dupe = silver_row.unionByName(silver_key)

    return bronze_u_df.alias("target").join(all_dupe.alias("source"),[col("target._sk") == col("source._sk")],how = "left_anti").select("target.*")


@dp.materialized_view(
    name="session_21_firststep.default.silver_row_dupe_quarantine",
    table_properties={"Tier": "silver"}
)
def silver_row_dupe_quarantine():
    bronze_u_df = spark.table("silver_check_null")
    data_cols = [c for c in bronze_u_df.columns if c not in ("_file_name", "_load_dttm", "_rescued_data", "_sk", "reason")]

    window_row_dupe = Window.partitionBy("_sk").orderBy("_sk")

    row_dupe_df = (
        bronze_u_df.withColumn("rn", row_number().over(window_row_dupe))
        .filter(col("rn") > 1)
        .withColumn("reason", lit("row_dupe"))
        .groupBy(*data_cols, "_sk")
        .agg(collect_list("reason").alias("reason"))
    )
    return row_dupe_df


@dp.materialized_view(
    name="session_21_firststep.default.silver_key_dupe_quarantine",
    table_properties={"Tier": "silver"}
)
def silver_key_dupe_quarantine():
    bronze_u_df = spark.table("silver_check_null")
    data_cols = [c for c in bronze_u_df.columns if c not in ("_file_name", "_load_dttm", "_rescued_data", "_sk", "reason")]

    window_row_dupe = Window.partitionBy("_sk").orderBy("_sk")
    window_key_dupe = Window.partitionBy("city_name", "datetime").orderBy("_sk")

    row_dedup = bronze_u_df.withColumn("rn", row_number().over(window_row_dupe)).filter(col("rn") == 1).drop("rn")

    key_dupe_df = (
        row_dedup.withColumn("rn", row_number().over(window_key_dupe))
        .filter(col("rn") > 1)
        .withColumn("reason", lit("key_dupe"))
        .groupBy(*data_cols, "_sk")
        .agg(collect_list("reason").alias("reason"))
    )
    return key_dupe_df
    