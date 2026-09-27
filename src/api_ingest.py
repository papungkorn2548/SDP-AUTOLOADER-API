# Databricks notebook source
# DBTITLE 1,API Ingest - Weather Data
# test pull request
import os
import requests
import json
import time
import logging

from datetime import datetime, timezone, date, timedelta


# =========================================================
# Databricks Parameters
# =========================================================

dbutils.widgets.text("start_date", "2025-04-05")

dbutils.widgets.text("days_per_run", "5")


START_DATE = date.fromisoformat(dbutils.widgets.get("start_date"))

DAYS_PER_RUN = int(dbutils.widgets.get("days_per_run"))


if DAYS_PER_RUN <= 0:
    raise ValueError("days_per_run ต้องมากกว่า 0")


# =========================================================
# Logging
# =========================================================

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger("weather_bootstrap")


# =========================================================
# Config
# =========================================================

# Historical Weather API
BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

LANDING_PATH = "/Volumes/workspace/default/weather_landing/weather_data"

# ดึงถึงเมื่อวาน
END_DATE = date.today() - timedelta(days=1)

# Checkpoint
CHECKPOINT_FILE = f"{LANDING_PATH}/_checkpoint.json"


# =========================================================
# Cities
# =========================================================

CITIES = [
    {"lat": 13.7563, "lon": 100.5018, "name": "Bangkok", "timezone": "Asia/Bangkok"},
    {"lat": 35.6762, "lon": 139.6503, "name": "Tokyo", "timezone": "Asia/Tokyo"},
    {"lat": 51.5072, "lon": -0.1276, "name": "London", "timezone": "Europe/London"},
]


# =========================================================
# Daily Variables
# =========================================================

DAILY_VARIABLES = [
    "weather_code",
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "apparent_temperature_max",
    "apparent_temperature_min",
    "apparent_temperature_mean",
    "sunrise",
    "sunset",
    "daylight_duration",
    "sunshine_duration",
    "precipitation_sum",
    "rain_sum",
    "snowfall_sum",
    "precipitation_hours",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "wind_direction_10m_dominant",
    "shortwave_radiation_sum",
    "et0_fao_evapotranspiration",
]


# =========================================================
# Build API Parameters
# =========================================================


def build_params(city: dict, target_date: date):
    date_str = target_date.isoformat()

    return {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "daily": ",".join(DAILY_VARIABLES),
        # 1 request = 1 วัน
        "start_date": date_str,
        "end_date": date_str,
        "timezone": city["timezone"],
    }


# =========================================================
# Validate API Response
# =========================================================


def validate_weather_data(data: dict) -> bool:
    required_fields = ["latitude", "longitude", "daily"]

    for field in required_fields:
        if field not in data:
            logger.warning(f"Response ขาด field: {field}")

            return False

    daily = data["daily"]

    for field in DAILY_VARIABLES:
        if field not in daily:
            logger.warning(f"Daily response ขาด field: {field}")

            return False

    return True


# =========================================================
# Fetch One City / One Day
# =========================================================


def fetch_weather(city: dict, target_date: date, max_retries: int = 3):
    for attempt in range(1, max_retries + 1):
        try:
            params = build_params(city=city, target_date=target_date)

            response = requests.get(BASE_URL, params=params, timeout=30)

            response.raise_for_status()

            data = response.json()

            # ---------------------------------------------
            # Validate
            # ---------------------------------------------

            if not validate_weather_data(data):
                logger.error(f"[{city['name']}] [{target_date}] Validation failed")

                return None

            # ---------------------------------------------
            # Metadata
            # ---------------------------------------------

            data["_city"] = city["name"]

            data["_target_date"] = target_date.isoformat()

            data["_source"] = "open-meteo"

            data["_fetched_at"] = datetime.now(timezone.utc).isoformat()

            logger.info(f"[{city['name']}] [{target_date}] Fetched successfully")

            return data

        except requests.exceptions.RequestException as e:
            logger.warning(
                f"[{city['name']}] "
                f"[{target_date}] "
                f"Attempt "
                f"{attempt}/{max_retries} "
                f"failed: {e}"
            )

            if attempt == max_retries:
                logger.error(f"[{city['name']}] [{target_date}] Max retries reached")

                return None

            # ---------------------------------------------
            # Exponential Backoff
            # ---------------------------------------------

            time.sleep(2**attempt)

    return None


# =========================================================
# Fetch One Day
# =========================================================


def fetch_daily_weather(cities: list, target_date: date):
    records = []

    logger.info(f"Starting date: {target_date}")

    for city in cities:
        record = fetch_weather(city=city, target_date=target_date)

        # -------------------------------------------------
        # ถ้าเมืองใดเมืองหนึ่ง fail
        # ทั้งวันถือว่า fail
        # -------------------------------------------------

        if record is None:
            logger.error(f"[{target_date}] Failed for {city['name']}")

            return None

        records.append(record)

        # -------------------------------------------------
        # ลดความถี่การยิง API
        # -------------------------------------------------

        time.sleep(5)

    logger.info(f"Finished date: {target_date} | success={len(records)}/{len(cities)}")

    return records


# =========================================================
# Write Daily Landing File
# =========================================================


def land_to_file(records: list, target_date: date):
    if not records:
        logger.warning(f"[{target_date}] ไม่มีข้อมูลสำหรับเขียน")

        return False

    os.makedirs(LANDING_PATH, exist_ok=True)

    file_path = f"{LANDING_PATH}/weather_{target_date.isoformat()}.json"

    try:
        # -------------------------------------------------
        # เขียนทับไฟล์เดิม
        # -------------------------------------------------

        with open(file_path, "w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

        logger.info(f"[{target_date}] เขียน {len(records)} records ลง {file_path}")

        return True

    except Exception as e:
        logger.error(f"[{target_date}] เขียนไฟล์ไม่สำเร็จ: {e}")

        return False


# =========================================================
# Load Checkpoint
# =========================================================


def load_checkpoint():
    # -----------------------------------------------------
    # ครั้งแรก
    # -----------------------------------------------------

    if not os.path.exists(CHECKPOINT_FILE):
        logger.info(f"ไม่พบ checkpoint เริ่มจาก START_DATE = {START_DATE}")

        return START_DATE

    # -----------------------------------------------------
    # มี checkpoint
    # -----------------------------------------------------

    try:
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            checkpoint = json.load(f)

        last_completed_date = date.fromisoformat(checkpoint["last_completed_date"])

        next_date = last_completed_date + timedelta(days=1)

        logger.info(f"พบ checkpoint: last_completed_date={last_completed_date}")

        logger.info(f"Resume จาก: {next_date}")

        return next_date

    except Exception as e:
        logger.warning(f"อ่าน checkpoint ไม่สำเร็จ: {e}")

        logger.info(f"Fallback กลับไป START_DATE = {START_DATE}")

        return START_DATE


# =========================================================
# Save Checkpoint
# =========================================================


def save_checkpoint(completed_date: date):
    checkpoint = {
        "last_completed_date": completed_date.isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    temp_file = f"{CHECKPOINT_FILE}.tmp"

    try:
        # -------------------------------------------------
        # เขียน temporary checkpoint
        # -------------------------------------------------

        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(checkpoint, f, indent=2)

        # -------------------------------------------------
        # Replace checkpoint เดิม
        # -------------------------------------------------

        os.replace(temp_file, CHECKPOINT_FILE)

        logger.info(f"Checkpoint updated: {completed_date}")

    except Exception as e:
        logger.error(f"ไม่สามารถ update checkpoint: {e}")

        raise


# =========================================================
# Run Bootstrap
# =========================================================


def run_bootstrap():
    # -----------------------------------------------------
    # Create landing directory
    # -----------------------------------------------------

    os.makedirs(LANDING_PATH, exist_ok=True)

    # -----------------------------------------------------
    # Load checkpoint
    # -----------------------------------------------------

    current_date = load_checkpoint()

    logger.info("========================================")

    logger.info(f"Bootstrap resume from: {current_date}")

    logger.info(f"Historical end date: {END_DATE}")

    logger.info(f"Days per run: {DAYS_PER_RUN}")

    # -----------------------------------------------------
    # ตรวจสอบว่าทำครบแล้วหรือยัง
    # -----------------------------------------------------

    if current_date > END_DATE:
        logger.info("Bootstrap completed แล้ว ไม่มีวันที่ต้อง process")

        return

    # -----------------------------------------------------
    # คำนวณวันสุดท้ายของ Run นี้
    # -----------------------------------------------------

    run_end_date = min(current_date + timedelta(days=DAYS_PER_RUN - 1), END_DATE)

    logger.info(f"This run: {current_date} → {run_end_date}")

    # -----------------------------------------------------
    # Process ทีละวัน
    # -----------------------------------------------------

    while current_date <= run_end_date:
        logger.info("========================================")

        logger.info(f"Processing date: {current_date}")

        # =================================================
        # Fetch 3 Cities
        # =================================================

        weather_data = fetch_daily_weather(cities=CITIES, target_date=current_date)

        # -------------------------------------------------
        # ถ้าวันนี้ fail
        # -------------------------------------------------

        if weather_data is None:
            logger.error(f"[{current_date}] Daily ingestion failed")

            logger.error("Run หยุดทันที")

            logger.error(f"Run ครั้งหน้า จะเริ่มจาก {current_date}")

            return

        # =================================================
        # Write Landing
        # =================================================

        write_success = land_to_file(records=weather_data, target_date=current_date)

        # -------------------------------------------------
        # ถ้าเขียนไฟล์ fail
        # -------------------------------------------------

        if not write_success:
            logger.error(f"[{current_date}] Landing failed")

            logger.error("Checkpoint จะไม่ถูก update")

            return

        # =================================================
        # Update Checkpoint
        # =================================================

        save_checkpoint(completed_date=current_date)

        logger.info(f"[{current_date}] Completed successfully")

        # -------------------------------------------------
        # Next Date
        # -------------------------------------------------

        current_date += timedelta(days=1)

    # =====================================================
    # Run Completed
    # =====================================================

    logger.info("========================================")

    logger.info(f"Run completed: {DAYS_PER_RUN} days maximum")


# =========================================================
# Execute
# =========================================================

run_bootstrap()