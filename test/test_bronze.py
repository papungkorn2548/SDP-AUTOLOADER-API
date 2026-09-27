import os
import uuid
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@pytest.fixture(scope="session")
def spark():
    """Create a fresh Spark Connect session with a unique session_id."""
    base_url = os.environ["SPARK_REMOTE"].split(";session_id=")[0]
    url = f"{base_url};session_id={uuid.uuid4()}"
    return SparkSession.builder.remote(url).getOrCreate()


# ── staging_weather.py ──────────────────────────────────────────────
# Replicates the transformation from staging_weather() in staging_weather.py:
#   1. Rename _city -> city_name
#   2. Filter to London / Bangkok / Tokyo
#   3. Parse daily JSON string -> struct
#   4. Explode arrays into one row per day

def _staging_weather_transform(df):
    """Copy of the logic in staging_weather() for testing."""
    change_name_df = df.withColumnRenamed("_city", "city_name")
    change_name_df = change_name_df.filter(col("city_name").isin("London", "Bangkok", "Tokyo"))
    daily_schema = (
        "struct<time:array<string>, weather_code:array<string>, "
        "temperature_2m_max:array<string>, temperature_2m_min:array<string>,"
        "temperature_2m_mean:array<string>, "
        "apparent_temperature_max:array<string>, "
        "apparent_temperature_min:array<string>>"
    )
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
                    "daily.apparent_temperature_min",
                )
            ).alias("daily"),
            F.col("_file_name"),
        )
        .withColumn("_load_dttm", current_timestamp())
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
            F.col("_load_dttm"),
        )
    )
    return df_daily


def test_staging_weather_filters_unwanted_cities(spark):
    """Only London, Bangkok, Tokyo should survive the filter."""
    daily_json = (
        '{"time":["2024-01-01"],"weather_code":["0"],'
        '"temperature_2m_max":["10"],"temperature_2m_min":["5"],'
        '"temperature_2m_mean":["7.5"],'
        '"apparent_temperature_max":["8"],'
        '"apparent_temperature_min":["3"]}'
    )
    raw = spark.createDataFrame(
        [
            ("London", daily_json, "f1.json"),
            ("Paris", daily_json, "f2.json"),
            ("Bangkok", daily_json, "f3.json"),
            ("Tokyo", daily_json, "f4.json"),
            ("Berlin", daily_json, "f5.json"),
        ],
        ["_city", "daily", "_file_name"],
    )
    result = _staging_weather_transform(raw)
    cities = [r.city_name for r in result.collect()]
    assert cities == ["London", "Bangkok", "Tokyo"]


def test_staging_weather_explodes_daily_arrays(spark):
    """Each day in the daily JSON should become its own row."""
    daily_json = (
        '{"time":["2024-01-01","2024-01-02"],'
        '"weather_code":["0","1"],'
        '"temperature_2m_max":["10","12"],'
        '"temperature_2m_min":["5","6"],'
        '"temperature_2m_mean":["7.5","9"],'
        '"apparent_temperature_max":["8","10"],'
        '"apparent_temperature_min":["3","4"]}'
    )
    raw = spark.createDataFrame(
        [("London", daily_json, "weather.json")],
        ["_city", "daily", "_file_name"],
    )
    result = _staging_weather_transform(raw)
    assert result.count() == 2
    rows = result.orderBy("datetime").collect()
    assert rows[0]["datetime"] == "2024-01-01"
    assert rows[0]["weather_code"] == "0"
    assert rows[0]["temperature_2m_max"] == "10"
    assert rows[1]["datetime"] == "2024-01-02"
    assert rows[1]["weather_code"] == "1"


def test_staging_weather_output_columns(spark):
    """Verify the exact column set produced by the transformation."""
    raw = spark.createDataFrame(
        [("Tokyo", "{}", "f.json")],
        ["_city", "daily", "_file_name"],
    )
    result = _staging_weather_transform(raw)
    expected_cols = {
        "city_name", "datetime", "weather_code",
        "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
        "apparent_temperature_max", "apparent_temperature_min",
        "_file_name", "_load_dttm",
    }
    assert set(result.columns) == expected_cols


# ── silver.py ──────────────────────────────────────────────────────
# Replicates the validation logic from silver_check_null() in silver.py:
#   1. Filter to London / Bangkok / Tokyo
#   2. Flag invalid city names and datetime formats
#   3. Compute surrogate key (_sk) from data columns
#   4. Drop rows that fail validation

def _silver_check_null_transform(df):
    """Copy of the logic in silver_check_null() for testing."""
    bronze_u_df = df.filter(col("city_name").isin("London", "Bangkok", "Tokyo"))
    bronze_u_df = bronze_u_df.withColumn(
        "_is_invalid_city_name",
        coalesce(~col("city_name").rlike("^[A-Za-z]+$"), lit(False)),
    ).withColumn(
        "_is_invalid_date_time",
        coalesce(~col("datetime").rlike("^\\d{4}-\\d{2}-\\d{2}$"), lit(False)),
    ).withColumn(
        "_is_null_city_name",
        coalesce(col("city_name").isNull(), lit(False)),
    ).withColumn(
        "_is_null_date_time",
        coalesce(col("datetime").isNull(), lit(False)),
    )

    data_cols = [c for c in bronze_u_df.columns if c not in ("_file_name", "_load_dttm", "_rescued_data")]
    bronze_u_df = bronze_u_df.withColumn("_sk", sha2(concat_ws("||", *[col(c) for c in data_cols]), 256))
    bronze_u_df = bronze_u_df.withColumn(
        "reason",
        array_compact(
            array(
                when(col("_is_invalid_city_name"), lit("invalid_city_name")),
                when(col("_is_invalid_date_time"), lit("invalid_date_time")),
                when(col("_is_null_city_name"), lit("null_city_name")),
                when(col("_is_null_date_time"), lit("null_date_time")),
            )
        ),
    )
    return bronze_u_df.filter(size(col("reason")) == 0).drop(
        "_is_invalid_city_name", "_is_invalid_date_time", "reason",
        "_is_null_date_time", "_is_null_city_name",
    )


_SILVER_COLS = [
    "city_name", "datetime", "weather_code", "temperature_2m_max",
    "temperature_2m_min", "temperature_2m_mean",
    "apparent_temperature_max", "apparent_temperature_min",
    "_file_name", "_load_dttm",
]


def test_silver_drops_invalid_datetime(spark):
    """Rows with malformed datetime should be dropped."""
    raw = spark.createDataFrame(
        [
            ("London", "2024-01-01", "0", "10", "5", "7.5", "8", "3", "f.json", "2024-01-01T00:00:00"),
            ("London", "01/01/2024", "0", "10", "5", "7.5", "8", "3", "f.json", "2024-01-01T00:00:00"),
        ],
        _SILVER_COLS,
    )
    result = _silver_check_null_transform(raw)
    assert result.count() == 1
    assert result.collect()[0]["datetime"] == "2024-01-01"


def test_silver_keeps_valid_rows(spark):
    """All valid rows should pass through unchanged."""
    raw = spark.createDataFrame(
        [
            ("Bangkok", "2024-03-15", "1", "35", "25", "30", "36", "24", "f.json", "2024-03-15T00:00:00"),
            ("Tokyo", "2024-03-16", "2", "20", "10", "15", "21", "9", "f.json", "2024-03-16T00:00:00"),
        ],
        _SILVER_COLS,
    )
    result = _silver_check_null_transform(raw)
    assert result.count() == 2
    assert {r.city_name for r in result.collect()} == {"Bangkok", "Tokyo"}


def test_silver_computes_surrogate_key(spark):
    """_sk column should be a non-null SHA-256 hash."""
    raw = spark.createDataFrame(
        [
            ("London", "2024-01-01", "0", "10", "5", "7.5", "8", "3", "f.json", "2024-01-01T00:00:00"),
        ],
        _SILVER_COLS,
    )
    result = _silver_check_null_transform(raw)
    assert "_sk" in result.columns
    sk_val = result.collect()[0]["_sk"]
    assert sk_val is not None
    assert len(sk_val) == 64  # SHA-256 hex length


# ── Gold.py ────────────────────────────────────────────────────────
# Replicates the yearly aggregation from gold_yearly_df() in Gold.py:
#   1. Extract year from datetime
#   2. Group by city_name + year
#   3. Aggregate temperature, precipitation, rain, wind metrics

def _gold_yearly_transform(df):
    """Copy of the logic in gold_yearly_df() for testing."""
    return (
        df.withColumn("year", F.year(F.col("datetime").cast("date")))
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


_GOLD_COLS = [
    "city_name", "datetime", "temperature_2m_max", "temperature_2m_min",
    "temperature_2m_mean", "apparent_temperature_max", "apparent_temperature_min",
    "precipitation_sum", "rain_sum", "wind_speed_10m_max", "wind_gusts_10m_max",
]


def test_gold_yearly_aggregation(spark):
    """Yearly averages and totals should be computed correctly."""
    silver_data = [
        ("London", "2024-01-01", 10.0, 5.0, 7.5, 8.0, 3.0, 1.0, 0.5, 20.0, 30.0),
        ("London", "2024-01-02", 12.0, 6.0, 9.0, 10.0, 4.0, 0.0, 0.0, 22.0, 32.0),
        ("London", "2024-02-01", 8.0, 3.0, 5.5, 6.0, 1.0, 2.0, 1.0, 18.0, 28.0),
    ]
    silver_df = spark.createDataFrame(silver_data, _GOLD_COLS)
    result = _gold_yearly_transform(silver_df).collect()

    assert len(result) == 1
    row = result[0]
    assert row["city_name"] == "London"
    assert row["year"] == 2024
    assert row["days_recorded"] == 3
    assert row["highest_temp"] == 12.0
    assert row["lowest_temp"] == 3.0
    assert row["total_precipitation_mm"] == 3.0
    assert row["rainy_days"] == 2  # Jan 1 (rain 0.5) and Feb 1 (rain 1.0) had rain


def test_gold_yearly_multiple_cities(spark):
    """Each city should get its own aggregated row."""
    silver_data = [
        ("London", "2024-06-01", 20.0, 10.0, 15.0, 18.0, 8.0, 0.0, 0.0, 15.0, 25.0),
        ("Tokyo", "2024-06-01", 25.0, 15.0, 20.0, 23.0, 13.0, 5.0, 2.0, 18.0, 30.0),
    ]
    silver_df = spark.createDataFrame(silver_data, _GOLD_COLS)
    result = _gold_yearly_transform(silver_df)
    assert result.count() == 2
    cities = {r.city_name for r in result.collect()}
    assert cities == {"London", "Tokyo"}


def test_gold_yearly_output_columns(spark):
    """Verify the exact column set produced by the aggregation."""
    silver_df = spark.createDataFrame(
        [("Bangkok", "2024-01-01", 30.0, 20.0, 25.0, 32.0, 18.0, 1.0, 0.5, 10.0, 15.0)],
        _GOLD_COLS,
    )
    result = _gold_yearly_transform(silver_df)
    expected_cols = {
        "city_name", "year", "days_recorded",
        "avg_temp_mean", "avg_temp_max", "avg_temp_min",
        "highest_temp", "lowest_temp",
        "total_precipitation_mm", "total_rain_mm", "rainy_days",
        "avg_wind_speed_max", "max_wind_gust",
    }
    assert set(result.columns) == expected_cols