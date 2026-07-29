"""
Garmin Workout Dashboard - Auto-Sync Version
Automatically syncs data from Garmin Connect on a schedule.
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from garminconnect import Garmin
import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
import json
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================
GARMIN_EMAIL = os.getenv("GARMIN_EMAIL", "")
GARMIN_PASSWORD = os.getenv("GARMIN_PASSWORD", "")
SYNC_INTERVAL_HOURS = int(os.getenv("SYNC_INTERVAL_HOURS", "6"))
ACTIVITY_LOOKBACK_DAYS = int(os.getenv("ACTIVITY_LOOKBACK_DAYS", "30"))

DB_PATH = Path(__file__).parent / "workouts.db"
TOKEN_PATH = Path(__file__).parent / ".garmin_session"

# ============================================================================
# GARMIN SYNC LOGIC
# ============================================================================

class GarminSync:
    """Handles authentication and data sync with Garmin Connect."""
    
    def __init__(self):
        self.client = None
        self.last_sync = None
    
    def login(self) -> bool:
        if not GARMIN_EMAIL or not GARMIN_PASSWORD:
            logger.warning("Garmin credentials not configured")
            return False

        try:
            if TOKEN_PATH.exists():
                try:
                    logger.info("Attempting to use saved session...")
                    self.client = Garmin()
                    self.client.login(str(TOKEN_PATH))
                    logger.info("Logged in using saved session")
                    return True
                except Exception as e:
                    logger.info(f"Saved session failed ({e}), attempting fresh login...")
                    try:
                        TOKEN_PATH.unlink(missing_ok=True)
                    except:
                        pass

            logger.info("Performing fresh login with credentials...")
            self.client = Garmin(GARMIN_EMAIL, GARMIN_PASSWORD)
            self.client.login()

            try:
                self.client.garth.dump(TOKEN_PATH)
            except Exception:
                pass

            logger.info("Fresh login successful")
            return True

        except Exception as e:
            logger.error(f"Garmin login failed: {e}")
            return False
    
    def fetch_activities(self, days_back: int = 30) -> list[dict]:
        if not self.client:
            if not self.login():
                return []

        try:
            cutoff_date = datetime.now() - timedelta(days=days_back)
            all_activities = []
            batch_size = 100
            start = 0

            while start < 500:
                batch = self.client.get_activities(start, batch_size)
                if not batch:
                    break

                oldest_in_batch = datetime.fromisoformat(
                    batch[-1]["startTimeLocal"].replace("Z", "+00:00")
                )

                for activity in batch:
                    activity_date = datetime.fromisoformat(
                        activity["startTimeLocal"].replace("Z", "+00:00")
                    )
                    if activity_date >= cutoff_date:
                        all_activities.append(self._normalize_activity(activity))

                if oldest_in_batch < cutoff_date:
                    break

                start += batch_size

            logger.info(f"Fetched {len(all_activities)} activities from last {days_back} days")
            return all_activities

        except Exception as e:
            logger.error(f"Failed to fetch activities: {e}")
            if self.login():
                return self.fetch_activities(days_back)
            return []

    def fetch_resting_heart_rate(self, days_back: int = 14) -> list[dict]:
        if not self.client:
            if not self.login():
                return []

        rhr_data = []
        today = datetime.now()
        
        for i in range(days_back):
            date_obj = today - timedelta(days=i)
            date_str = date_obj.strftime("%Y-%m-%d")
            try:
                stats = self.client.get_stats(date_str)
                rhr = stats.get("restingHeartRate")
                if rhr:
                    rhr_data.append({"date": date_str, "resting_hr": rhr})
            except Exception as e:
                logger.debug(f"Could not fetch RHR for {date_str}: {e}")
                
        return rhr_data

    def fetch_training_readiness_comprehensive(self) -> dict:
        if not self.client:
            if not self.login():
                return {"score": 70, "status": "Moderat", "description": "Afventer Garmin-forbindelse"}

        today_str = datetime.now().strftime("%Y-%m-%d")
        sleep_score, hrv_status, avg_stress, body_battery = None, None, None, None
        
        try:
            sleep_data = self.client.get_sleep_data(today_str)
            if sleep_data and "dailySleepDTO" in sleep_data:
                sleep_score = sleep_data["dailySleepDTO"].get("sleepScores", {}).get("overall", {}).get("value")
        except Exception:
            pass

        try:
            hrv_data = self.client.get_hrv_data(today_str)
            if hrv_data and "hrvSummary" in hrv_data:
                hrv_status = hrv_data["hrvSummary"].get("status")
        except Exception:
            pass

        try:
            stats = self.client.get_stats(today_str)
            avg_stress = stats.get("averageStressLevel")
            body_battery = stats.get("bodyBatteryMostRecentValue")
        except Exception:
            pass

        score_components = []
        if sleep_score is not None: score_components.append(sleep_score)
        if body_battery is not None: score_components.append(body_battery)
        
        if score_components:
            final_score = int(sum(score_components) / len(score_components))
            if hrv_status == 'UNBALANCED': final_score = max(10, final_score - 15)
            if avg_stress and avg_stress > 40: final_score = max(10, final_score - 10)
            final_score = max(10, min(98, final_score))
        else:
            final_score = 75

        if final_score >= 75:
            status, desc = "Høj", f"Optimal restitution. Søvnscore: {sleep_score or 'N/A'}, BB: {body_battery or 'N/A'}"
        elif final_score >= 50:
            status, desc = "Moderat", f"God balance. HRV: {hrv_status or 'Normal'}, Stress: {avg_stress or 'N/A'}"
        else:
            status, desc = "Lav", "Kroppen er belastet. Prioriter hvile."

        return {"score": final_score, "status": status, "description": desc}

    def _normalize_activity(self, raw: dict) -> dict:
        def safe_get(key, default=None):
            val = raw.get(key)
            return val if val is not None else default
        
        def seconds_to_minutes(s):
            return s / 60 if s else None
        
        def mps_to_metric_pace(speed):
            if not speed or speed == 0: return None
            return (1000 / speed) / 60
        
        return {
            "activity_type": safe_get("activityType", {}).get("typeKey", "unknown"),
            "date": safe_get("startTimeLocal"),
            "title": safe_get("activityName", ""),
            "distance": safe_get("distance"),
            "calories": safe_get("calories"),
            "duration_minutes": seconds_to_minutes(safe_get("duration")),
            "avg_hr": safe_get("averageHR"),
            "max_hr": safe_get("maxHR"),
            "avg_pace_minutes": mps_to_metric_pace(safe_get("averageSpeed")),
            "best_pace_minutes": mps_to_metric_pace(safe_get("maxSpeed")),
            "device": safe_get("deviceModelName", "Garmin enhed"),
            "avg_cadence": safe_get("averageRunningCadenceInStepsPerMinute")
        }


garmin_sync = GarminSync()


def save_activities_to_db(activities: list[dict]) -> int:
    if not activities: return 0
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    inserted = 0
    for a in activities:
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO activities (
                    activity_type, date, title, distance, calories, duration_minutes,
                    avg_hr, max_hr, avg_pace_minutes, best_pace_minutes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                a["activity_type"], a["date"], a["title"], a["distance"],
                a["calories"], a["duration_minutes"], a["avg_hr"], a["max_hr"],
                a["avg_pace_minutes"], a["best_pace_minutes"],
            ))
            inserted += 1
        except Exception as e:
            logger.error(f"Error inserting activity: {e}")
    conn.commit()
    conn.close()
    return inserted


def save_resting_heart_rate_to_db(rhr_list: list[dict]):
    if not rhr_list: return
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    for item in rhr_list:
        cursor.execute("""
            INSERT OR REPLACE INTO resting_heart_rate (date, resting_hr)
            VALUES (?, ?)
        """, (item["date"], item["resting_hr"]))
    conn.commit()
    conn.close()


async def scheduled_sync():
    logger.info("Starting scheduled sync...")
    activities = garmin_sync.fetch_activities(ACTIVITY_LOOKBACK_DAYS)
    save_activities_to_db(activities)
    rhr_data = garmin_sync.fetch_resting_heart_rate(14)
    save_resting_heart_rate_to_db(rhr_data)
    garmin_sync.last_sync = datetime.now()
    logger.info("Scheduled sync complete")


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS activities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            activity_type TEXT,
            date TEXT,
            title TEXT,
            distance REAL,
            calories INTEGER,
            duration_minutes REAL,
            avg_hr INTEGER,
            max_hr INTEGER,
            aerobic_te REAL,
            avg_cadence INTEGER,
            max_cadence INTEGER,
            avg_pace_minutes REAL,
            best_pace_minutes REAL,
            total_ascent INTEGER,
            total_descent INTEGER,
            steps INTEGER,
            total_reps INTEGER,
            total_sets INTEGER,
            UNIQUE(activity_type, date, title)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS resting_heart_rate (
            date TEXT PRIMARY KEY,
            resting_hr INTEGER
        )
    """)
    conn.commit()
    conn.close()


def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if GARMIN_EMAIL and GARMIN_PASSWORD:
        logger.info("Credentials found, performing initial sync...")
        await scheduled_sync()
        scheduler.add_job(scheduled_sync, 'interval', hours=SYNC_INTERVAL_HOURS, id='garmin_sync', replace_existing=True)
        scheduler.start()
    yield
    if scheduler.running: scheduler.shutdown()

app = FastAPI(title="Garmin Dashboard", lifespan=lifespan)

@app.get("/api/sync/status")
async def sync_status():
    return {
        "auto_sync_enabled": bool(GARMIN_EMAIL and GARMIN_PASSWORD),
        "sync_interval_hours": SYNC_INTERVAL_HOURS,
        "last_sync": garmin_sync.last_sync.isoformat() if garmin_sync.last_sync else None,
    }

@app.post("/api/sync/now")
async def sync_now():
    if not GARMIN_EMAIL or not GARMIN_PASSWORD:
        raise HTTPException(400, "Garmin credentials not configured")
    activities = garmin_sync.fetch_activities(ACTIVITY_LOOKBACK_DAYS)
    save_activities_to_db(activities)
    rhr_data = garmin_sync.fetch_resting_heart_rate(14)
    save_resting_heart_rate_to_db(rhr_data)
    garmin_sync.last_sync = datetime.now()
    return {"message": "Sync fuldført", "last_sync": garmin_sync.last_sync.isoformat()}

@app.get("/api/health/resting-hr")
async def get_resting_hr():
    conn = get_db_connection()
    rows = conn.execute("SELECT date, resting_hr FROM resting_heart_rate ORDER BY date ASC LIMIT 14").fetchall()
    conn.close()
    return [dict(row) for row in rows]

@app.get("/api/running/pace-30days")
async def get_running_pace_30days():
    conn = get_db_connection()
    cutoff = (datetime.now() - timedelta(days=30)).isoformat()
    rows = conn.execute("""
        SELECT date, title, distance, avg_pace_minutes as avg_pace_min_km, avg_hr, max_hr
        FROM activities
        WHERE activity_type IN ('running', 'treadmill_running', 'track_running')
          AND date >= ?
          AND avg_pace_minutes IS NOT NULL
        ORDER BY date ASC
    """, (cutoff,)).fetchall()
    conn.close()
    return [dict(row) for row in rows]

@app.get("/api/training/readiness")
async def get_training_readiness():
    return garmin_sync.fetch_training_readiness_comprehensive()

@app.get("/api/activities/all")
async def get_all_activities():
    """Endpoint der henter alle aktiviteter til kalenderen."""
    conn = get_db_connection()
    rows = conn.execute("SELECT * FROM activities ORDER BY date DESC").fetchall()
    conn.close()
    return [dict(row) for row in rows]

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    html_path = Path(__file__).parent / "dashboard_new.html"
    return html_path.read_text(encoding="utf-8")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", poprt=8000)