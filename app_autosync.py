"""
Garmin Workout Dashboard - Pure Garmin Sync Version
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
ACTIVITY_LOOKBACK_DAYS = int(os.getenv("ACTIVITY_LOOKBACK_DAYS", "90"))

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
    
    def fetch_activities(self, days_back: int = 90) -> list[dict]:
        if not self.client:
            if not self.login():
                return []
        try:
            batch = self.client.get_activities(0, 100)
            if not batch:
                return []
            
            batch = sorted(batch, key=lambda x: x.get("startTimeLocal", ""))
            
            activities = []
            maf_counter = 1
            maf_start_date = datetime.strptime("2026-07-13", "%Y-%m-%d")

            for a in batch:
                norm = self._normalize_activity(a)
                if norm["activity_type"] in ["running", "treadmill_running", "track_running"]:
                    try:
                        act_date = datetime.strptime(norm["date"].split("T")[0], "%Y-%m-%d")
                        if act_date >= maf_start_date:
                            norm["title"] = f"MAF {maf_counter}"
                            maf_counter += 1
                    except:
                        pass
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

    def fetch_hrv_history(self, days_back: int = 14) -> list[dict]:
        if not self.client:
            if not self.login():
                return []
        hrv_data = []
        today = datetime.now()
        for i in range(days_back):
            date_obj = today - timedelta(days=i)
            date_str = date_obj.strftime("%Y-%m-%d")
            try:
                data = self.client.get_hrv_data(date_str)
                if data and "hrvSummary" in data:
                    summary = data["hrvSummary"]
                    val = summary.get("weeklyAverage") or summary.get("lastNightAvg")
                    
                    baseline = summary.get("baseline", {})
                    low = baseline.get("balancedLow")
                    high = baseline.get("balancedUpper")

                    if val:
                        hrv_data.append({
                            "date": date_str, 
                            "hrv": val,
                            "baseline_low": low,
                            "baseline_high": high
                        })
            except:
                pass
        return sorted(hrv_data, key=lambda x: x["date"])

    def fetch_training_readiness_comprehensive(self) -> dict:
        if not self.client:
            if not self.login():
                return {"score": 75, "status": "Høj", "description": "Afventer forbindelse til Garmin Connect."}

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

        recent_penalty = 0
        last_activity_text = "Ingen nyere træning fundet"
        latest_act = None

        try:
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            run_row = cursor.execute("""
                SELECT date, title, distance, duration_minutes, 'running' as type
                FROM activities 
                WHERE activity_type IN ('running', 'treadmill_running', 'track_running')
                ORDER BY date DESC LIMIT 1
            """).fetchone()
            conn.close()

            if run_row:
                latest_act = dict(run_row)
        except Exception as e:
            logger.error(f"Fejl ved hentning af seneste aktivitet: {e}")

        if latest_act:
            try:
                act_date_str = latest_act["date"].split("T")[0]
                act_date = datetime.strptime(act_date_str, "%Y-%m-%d")
                today_date = datetime.strptime("2026-07-30", "%Y-%m-%d")
                days_ago = (today_date - act_date).days
                
                dist_km = (latest_act["distance"] or 0) / 1000
                if days_ago == 0:
                    recent_penalty = int(dist_km * 1.5)
                    last_activity_text = f"Sidste løb: I dag ({dist_km:.1f} km)"
                elif days_ago == 1:
                    recent_penalty = int(dist_km * 0.8)
                    last_activity_text = f"Sidste løb: I går ({dist_km:.1f} km)"
                else:
                    recent_penalty = max(0, int(dist_km * (0.5 / days_ago)))
                    last_activity_text = f"Sidste løb: For {days_ago} dage siden ({dist_km:.1f} km)"
            except Exception as e:
                logger.error(f"Fejl ved udregning af straf: {e}")

        score_parts = []
        if sleep_score is not None: score_parts.append(sleep_score * 0.5)
        if body_battery is not None: score_parts.append(body_battery * 0.5)

        score = int(sum(score_parts) / (len(score_parts) * 0.5) if len(score_parts) > 0 else 75)
        score -= recent_penalty

        if hrv_status == 'UNBALANCED': score -= 12
        elif hrv_status == 'BALANCED': score += 5
        if rhr and rhr > 60: score -= 8

        stress_text = "Stress: Ikke tilgængelig"
        if stress_avg is not None:
            stress_text = f"Stress: {stress_avg}"
            if stress_avg > 40: score -= 15
            elif stress_avg > 25: score -= 8
            else: score += 5

        score = max(1, min(100, score))
        status = "Høj" if score >= 70 else ("Moderat" if score >= 45 else "Lav")
        
        desc_parts = []
        if body_battery is not None: desc_parts.append(f"BB: {body_battery}")
        if sleep_score is not None: desc_parts.append(f"Søvn: {sleep_score}/100")
        desc_parts.append(last_activity_text)
        if hrv_status: desc_parts.append(f"HRV: {hrv_status}")
        desc_parts.append(stress_text)

        return {"score": score, "status": status, "description": " • ".join(desc_parts)}

    def _normalize_activity(self, raw: dict) -> dict:
        speed = raw.get("averageSpeed")
        pace_min = (1000 / speed / 60) if speed and speed > 0 else 5.0
        duration_sec = raw.get("duration")
        duration_mins = (duration_sec / 60) if duration_sec else 0

        act_type = "running"
        try:
            act_type = raw.get("activityType", {}).get("typeKey", "running")
        except:
            pass

        return {
            "activity_id": str(raw.get("activityId", "")),
            "activity_type": act_type,
            "date": raw.get("startTimeLocal"),
            "title": raw.get("activityName", "Løbetur"),
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
            activity_id TEXT PRIMARY KEY,
            activity_type TEXT,
            date TEXT,
            title TEXT,
            distance REAL,
            calories INTEGER,
            duration_minutes REAL,
            avg_hr INTEGER,
            max_hr INTEGER,
            avg_pace_minutes REAL,
            best_pace_minutes REAL
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

async def scheduled_sync():
    activities = garmin_sync.fetch_activities(ACTIVITY_LOOKBACK_DAYS)
    if activities:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        for a in activities:
            cursor.execute("""
                INSERT OR REPLACE INTO activities (activity_id, activity_type, date, title, distance, calories, duration_minutes, avg_hr, max_hr, avg_pace_minutes, best_pace_minutes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (a["activity_id"], a["activity_type"], a["date"], a["title"], a["distance"], a["calories"], a["duration_minutes"], a["avg_hr"], a["max_hr"], a["avg_pace_minutes"], a["best_pace_minutes"]))
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

@app.get("/api/health/hrv-14days")
async def get_hrv_14days():
    return garmin_sync.fetch_hrv_history(14)

@app.get("/api/maf/progress")
async def get_maf_progress():
    conn = get_db_connection()
    rows = conn.execute("""
        SELECT date, title, distance, avg_pace_minutes as avg_pace, avg_hr
        FROM activities
        WHERE activity_type IN ('running', 'treadmill_running', 'track_running')
          AND date >= '2026-07-13'
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
    rows = conn.execute("""
        SELECT * FROM activities 
        WHERE activity_type IN ('running', 'treadmill_running', 'track_running')
        ORDER BY date DESC
    """).fetchall()
    conn.close()
    return [dict(row) for row in rows]

@app.get("/api/activity/{date_str}")
async def get_activity_by_date(date_str: str):
    conn = get_db_connection()
    row = conn.execute("""
        SELECT * FROM activities 
        WHERE activity_type IN ('running', 'treadmill_running', 'track_running') 
          AND date LIKE ? 
        LIMIT 1
    """, (f"{date_str}%",)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Ingen løbetur fundet på denne dato")
    
    activity_dict = dict(row)
    act_id = activity_dict.get("activity_id")

    splits = []
    if garmin_sync.client or garmin_sync.login():
        try:
            laps_data = garmin_sync.client.get_activity_splits(act_id)
            if laps_data and "lapDTOs" in laps_data:
                for idx, lap in enumerate(laps_data["lapDTOs"]):
                    lap_dist = lap.get("distance", 0) / 1000
                    lap_duration = lap.get("duration", 0) / 60
                    lap_speed = lap.get("averageSpeed", 0)
                    lap_pace = (1000 / lap_speed / 60) if lap_speed and lap_speed > 0 else 0
                    lap_hr = lap.get("averageHR", 0)
                    
                    if lap_dist > 0:
                        splits.append({
                            "km": idx + 1,
                            "distance": lap_dist,
                            "duration_minutes": lap_duration,
                            "pace": lap_pace,
                            "hr": int(lap_hr) if lap_hr else 0
                        })
        except Exception as e:
            logger.error(f"Kunne ikke hente splits fra Garmin: {e}")

    activity_dict["splits"] = splits
    return activity_dict

@app.get("/activity", response_class=HTMLResponse)
async def activity_page():
    return (Path(__file__).parent / "activity.html").read_text(encoding="utf-8")

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return (Path(__file__).parent / "dashboard_new.html").read_text(encoding="utf-8")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)