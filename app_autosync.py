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
                    self.client = Garmin()
                    self.client.login(str(TOKEN_PATH))
                    return True
                except:
                    TOKEN_PATH.unlink(missing_ok=True)

            self.client = Garmin(GARMIN_EMAIL, GARMIN_PASSWORD)
            self.client.login()
            try:
                self.client.garth.dump(TOKEN_PATH)
            except:
                pass
            return True
        except Exception as e:
            logger.error(f"Garmin login failed: {e}")
            return False
    
    def fetch_activities(self, days_back: int = 30) -> list[dict]:
        if not self.client:
            if not self.login():
                return []
        try:
            batch = self.client.get_activities(0, 100)
            if not batch:
                return []
            
            activities = []
            for a in batch:
                norm = self._normalize_activity(a)
                # KUN LØB: Hvis det er en gåtur eller andet, springes det over!
                if norm["activity_type"] in ["running", "treadmill_running", "track_running"]:
                    activities.append(norm)
            return activities
        except Exception as e:
            logger.error(f"Failed to fetch activities: {e}")
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
            except:
                pass
        return rhr_data

    def fetch_training_readiness_comprehensive(self) -> dict:
        if not self.client:
            if not self.login():
                return {"score": 75, "status": "Høj", "description": "Offline tilstand - Data beskyttet."}

        today_str = datetime.now().strftime("%Y-%m-%d")
        sleep_score, hrv_status, body_battery, rhr, stress_avg = None, None, None, None, None

        try:
            sleep_data = self.client.get_sleep_data(today_str)
            if sleep_data and "dailySleepDTO" in sleep_data:
                sleep_score = sleep_data["dailySleepDTO"].get("sleepScores", {}).get("overall", {}).get("value")
        except:
            pass

        try:
            hrv_data = self.client.get_hrv_data(today_str)
            if hrv_data and "hrvSummary" in hrv_data:
                hrv_status = hrv_data["hrvSummary"].get("status")
        except:
            pass

        try:
            stats = self.client.get_stats(today_str)
            body_battery = stats.get("bodyBatteryMostRecentValue")
            rhr = stats.get("restingHeartRate")
            stress_avg = stats.get("averageStressLevel")
        except:
            pass

        components = []
        if sleep_score is not None: components.append(sleep_score)
        if body_battery is not None: components.append(body_battery)

        if components:
            score = int(sum(components) / len(components))
            if hrv_status == 'UNBALANCED': score = max(10, score - 15)
            if stress_avg and stress_avg > 45: score = max(10, score - 10)
            score = max(10, min(99, score))
        else:
            score = 75

        status = "Høj" if score >= 75 else ("Moderat" if score >= 50 else "Lav")
        desc_parts = []
        if sleep_score: desc_parts.append(f"Søvnscore: {sleep_score}/100")
        if body_battery: desc_parts.append(f"Body Battery: {body_battery}")
        if hrv_status: desc_parts.append(f"HRV: {hrv_status}")
        if rhr: desc_parts.append(f"Hvilepuls: {rhr} bpm")
        
        description = " • ".join(desc_parts) if desc_parts else "Data hentet fra Garmin Connect."

        return {"score": score, "status": status, "description": description}

    def _normalize_activity(self, raw: dict) -> dict:
        speed = raw.get("averageSpeed")
        pace_min = (1000 / speed / 60) if speed and speed > 0 else 5.0
        duration_sec = raw.get("duration")
        duration_mins = (duration_sec / 60) if duration_sec else 0

        return {
            "activity_type": raw.get("activityType", {}).get("typeKey", "running"),
            "date": raw.get("startTimeLocal"),
            "title": raw.get("activityName", "Løbetur"), # Bruger præcis det navn Garmin Connect giver
            "distance": raw.get("distance", 0),
            "calories": raw.get("calories", 0),
            "duration_minutes": duration_mins,
            "avg_hr": raw.get("averageHR", 0),
            "max_hr": raw.get("maxHR", 0),
            "avg_pace_minutes": pace_min,
            "best_pace_minutes": pace_min,
        }

garmin_sync = GarminSync()

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
            avg_pace_minutes REAL,
            best_pace_minutes REAL,
            UNIQUE(activity_type, date, title)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS resting_heart_rate (
            date TEXT PRIMARY KEY,
            resting_hr INTEGER
        )
    """)
    
    cursor.execute("SELECT COUNT(*) FROM activities")
    if cursor.fetchone()[0] == 0:
        sample_activities = [
            ("running", "2026-07-28T08:30:00.0", "Morgenløb", 7500, 480, 38.5, 148, 162, 5.1, 4.5),
            ("running", "2026-07-26T09:00:00.0", "Aerob Grundform", 10000, 620, 52.0, 152, 168, 5.2, 4.8),
            ("running", "2026-07-24T07:15:00.0", "Intervalløb", 6000, 400, 29.0, 158, 175, 4.8, 4.2),
        ]
        cursor.executemany("""
            INSERT OR IGNORE INTO activities (activity_type, date, title, distance, calories, duration_minutes, avg_hr, max_hr, avg_pace_minutes, best_pace_minutes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, sample_activities)

    cursor.execute("SELECT COUNT(*) FROM resting_heart_rate")
    if cursor.fetchone()[0] == 0:
        sample_rhr = [
            ("2026-07-28", 54), ("2026-07-27", 55), ("2026-07-26", 53),
            ("2026-07-25", 56), ("2026-07-24", 54), ("2026-07-23", 53)
        ]
        cursor.executemany("INSERT OR IGNORE INTO resting_heart_rate (date, resting_hr) VALUES (?, ?)", sample_rhr)

    conn.commit()
    conn.close()

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

async def scheduled_sync():
    activities = garmin_sync.fetch_activities(ACTIVITY_LOOKBACK_DAYS)
    if activities:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        for a in activities:
            cursor.execute("""
                INSERT OR REPLACE INTO activities (activity_type, date, title, distance, calories, duration_minutes, avg_hr, max_hr, avg_pace_minutes, best_pace_minutes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (a["activity_type"], a["date"], a["title"], a["distance"], a["calories"], a["duration_minutes"], a["avg_hr"], a["max_hr"], a["avg_pace_minutes"], a["best_pace_minutes"]))
        conn.commit()
        conn.close()
    
    rhr_data = garmin_sync.fetch_resting_heart_rate(14)
    if rhr_data:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        for item in rhr_data:
            cursor.execute("""
                INSERT OR REPLACE INTO resting_heart_rate (date, resting_hr)
                VALUES (?, ?)
            """, (item["date"], item["resting_hr"]))
        conn.commit()
        conn.close()

    garmin_sync.last_sync = datetime.now()

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if GARMIN_EMAIL and GARMIN_PASSWORD:
        await scheduled_sync()
        scheduler.add_job(scheduled_sync, 'interval', hours=SYNC_INTERVAL_HOURS, id='garmin_sync', replace_existing=True)
        scheduler.start()
    yield
    if scheduler.running: scheduler.shutdown()

app = FastAPI(title="Garmin Dashboard", lifespan=lifespan)

@app.get("/api/sync/status")
async def sync_status():
    return {"auto_sync_enabled": bool(GARMIN_EMAIL and GARMIN_PASSWORD), "sync_interval_hours": SYNC_INTERVAL_HOURS, "last_sync": garmin_sync.last_sync.isoformat() if garmin_sync.last_sync else None}

@app.post("/api/sync/now")
async def sync_now():
    await scheduled_sync()
    return {"message": "Synkronisering gennemført", "last_sync": datetime.now().isoformat()}

@app.get("/api/health/resting-hr")
async def get_resting_hr():
    conn = get_db_connection()
    rows = conn.execute("SELECT date, resting_hr FROM resting_heart_rate ORDER BY date ASC LIMIT 14").fetchall()
    conn.close()
    return [dict(row) for row in rows]

@app.get("/api/running/pace-30days")
async def get_running_pace_30days():
    conn = get_db_connection()
    rows = conn.execute("""
        SELECT date, title, distance, avg_pace_minutes as avg_pace_min_km, avg_hr, max_hr
        FROM activities
        WHERE activity_type IN ('running', 'treadmill_running', 'track_running')
          AND avg_pace_minutes IS NOT NULL 
          AND avg_pace_minutes > 0
        ORDER BY date ASC
    """).fetchall()
    conn.close()
    return [dict(row) for row in rows]

@app.get("/api/training/readiness")
async def get_training_readiness():
    return garmin_sync.fetch_training_readiness_comprehensive()

@app.get("/api/activities/all")
async def get_all_activities():
    conn = get_db_connection()
    rows = conn.execute("SELECT * FROM activities WHERE activity_type IN ('running', 'treadmill_running', 'track_running') ORDER BY date DESC").fetchall()
    conn.close()
    return [dict(row) for row in rows]

@app.get("/api/activity/{date_str}")
async def get_activity_by_date(date_str: str):
    conn = get_db_connection()
    row = conn.execute("SELECT * FROM activities WHERE date LIKE ? LIMIT 1", (f"{date_str}%",)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Ingen aktivitet fundet")
    return dict(row)

@app.get("/activity", response_class=HTMLResponse)
async def activity_page():
    return (Path(__file__).parent / "activity.html").read_text(encoding="utf-8")

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return (Path(__file__).parent / "dashboard_new.html").read_text(encoding="utf-8")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)