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
# CONFIGURATION - Set these via environment variables or .env file
# ============================================================================
GARMIN_EMAIL = os.getenv("GARMIN_EMAIL", "")
GARMIN_PASSWORD = os.getenv("GARMIN_PASSWORD", "")
SYNC_INTERVAL_HOURS = int(os.getenv("SYNC_INTERVAL_HOURS", "6"))  # How often to sync
ACTIVITY_LOOKBACK_DAYS = int(os.getenv("ACTIVITY_LOOKBACK_DAYS", "120"))  # How far back to fetch

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
        """Authenticate with Garmin Connect, reusing session if possible."""
        if not GARMIN_EMAIL or not GARMIN_PASSWORD:
            logger.warning("Garmin credentials not configured")
            return False

        try:
            # Try to reuse saved session token first
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

            # Fresh login with credentials
            logger.info("Performing fresh login with credentials...")
            self.client = Garmin(GARMIN_EMAIL, GARMIN_PASSWORD)
            self.client.login()

            # Save session safely (skipped on read-only environments like Render)
            try:
                self.client.garth.dump(TOKEN_PATH)
            except Exception:
                pass

            logger.info("Fresh login successful")
            return True

        except Exception as e:
            logger.error(f"Garmin login failed: {e}")
            logger.exception("Full traceback:")
            return False
    
    def fetch_activities(self, days_back: int = 30) -> list[dict]:
        """Fetch activities from the last N days."""
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
    
    def _normalize_activity(self, raw: dict) -> dict:
        """Convert Garmin API response to our database schema."""
        
        def safe_get(key, default=None):
            val = raw.get(key)
            return val if val is not None else default
        
        def meters_to_miles(m):
            return m * 0.000621371 if m else None
        
        def seconds_to_minutes(s):
            return s / 60 if s else None
        
        def mps_to_pace(speed):
            """Convert m/s to min/mile pace."""
            if not speed or speed == 0:
                return None
            return 26.8224 / speed
        
        return {
            "activity_type": safe_get("activityType", {}).get("typeKey", "unknown"),
            "date": safe_get("startTimeLocal"),
            "title": safe_get("activityName", ""),
            "distance": meters_to_miles(safe_get("distance")),
            "calories": safe_get("calories"),
            "duration_minutes": seconds_to_minutes(safe_get("duration")),
            "avg_hr": safe_get("averageHR"),
            "max_hr": safe_get("maxHR"),
            "aerobic_te": safe_get("aerobicTrainingEffect"),
            "avg_cadence": safe_get("averageRunningCadenceInStepsPerMinute"),
            "max_cadence": safe_get("maxRunningCadenceInStepsPerMinute"),
            "avg_pace_minutes": mps_to_pace(safe_get("averageSpeed")),
            "best_pace_minutes": mps_to_pace(safe_get("maxSpeed")),
            "total_ascent": safe_get("elevationGain"),
            "total_descent": safe_get("elevationLoss"),
            "steps": safe_get("steps"),
            "total_reps": safe_get("totalReps"),
            "total_sets": safe_get("totalSets"),
        }


garmin_sync = GarminSync()


def save_activities_to_db(activities: list[dict]) -> int:
    """Save activities to SQLite database."""
    if not activities:
        return 0
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    inserted = 0
    for a in activities:
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO activities (
                    activity_type, date, title, distance, calories, duration_minutes,
                    avg_hr, max_hr, aerobic_te, avg_cadence, max_cadence,
                    avg_pace_minutes, best_pace_minutes, total_ascent, total_descent,
                    steps, total_reps, total_sets
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                a["activity_type"], a["date"], a["title"], a["distance"],
                a["calories"], a["duration_minutes"], a["avg_hr"], a["max_hr"],
                a["aerobic_te"], a["avg_cadence"], a["max_cadence"],
                a["avg_pace_minutes"], a["best_pace_minutes"],
                a["total_ascent"], a["total_descent"],
                a["steps"], a["total_reps"], a["total_sets"],
            ))
            inserted += 1
        except Exception as e:
            logger.error(f"Error inserting activity: {e}")
    
    conn.commit()
    conn.close()
    return inserted


async def scheduled_sync():
    """Background job that runs on schedule."""
    logger.info("Starting scheduled sync...")
    activities = garmin_sync.fetch_activities(ACTIVITY_LOOKBACK_DAYS)
    count = save_activities_to_db(activities)
    garmin_sync.last_sync = datetime.now()
    logger.info(f"Scheduled sync complete: {count} activities updated")


# ============================================================================
# DATABASE SETUP
# ============================================================================

def init_db():
    """Initialize SQLite database."""
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
    conn.commit()
    conn.close()


def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ============================================================================
# FASTAPI APP WITH SCHEDULER
# ============================================================================

scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown logic."""
    init_db()
    
    if GARMIN_EMAIL and GARMIN_PASSWORD:
        logger.info("Credentials found, performing initial sync...")
        await scheduled_sync()
        
        scheduler.add_job(
            scheduled_sync,
            'interval',
            hours=SYNC_INTERVAL_HOURS,
            id='garmin_sync',
            replace_existing=True
        )
        scheduler.start()
        logger.info(f"Scheduler started: syncing every {SYNC_INTERVAL_HOURS} hours")
    else:
        logger.warning("No Garmin credentials - auto-sync disabled")
    
    yield
    
    if scheduler.running:
        scheduler.shutdown()


app = FastAPI(title="Garmin Workout Dashboard", lifespan=lifespan)


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.get("/api/sync/status")
async def sync_status():
    """Get current sync status."""
    return {
        "auto_sync_enabled": bool(GARMIN_EMAIL and GARMIN_PASSWORD),
        "sync_interval_hours": SYNC_INTERVAL_HOURS,
        "last_sync": garmin_sync.last_sync.isoformat() if garmin_sync.last_sync else None,
        "next_sync": scheduler.get_job('garmin_sync').next_run_time.isoformat() 
                     if scheduler.get_job('garmin_sync') else None
    }


@app.post("/api/sync/now")
async def sync_now():
    """Trigger an immediate sync."""
    if not GARMIN_EMAIL or not GARMIN_PASSWORD:
        raise HTTPException(400, "Garmin credentials not configured")
    
    activities = garmin_sync.fetch_activities(ACTIVITY_LOOKBACK_DAYS)
    count = save_activities_to_db(activities)
    garmin_sync.last_sync = datetime.now()
    
    return {"message": f"Synced {count} activities", "last_sync": garmin_sync.last_sync.isoformat()}


@app.get("/api/activities")
async def get_activities(activity_type: str = None, limit: int = 100):
    conn = get_db_connection()
    if activity_type:
        query = "SELECT * FROM activities WHERE activity_type LIKE ? ORDER BY date DESC LIMIT ?"
        rows = conn.execute(query, (f"%{activity_type}%", limit)).fetchall()
    else:
        rows = conn.execute("SELECT * FROM activities ORDER BY date DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return [dict(row) for row in rows]


@app.get("/api/running/pace-trend")
async def get_pace_trend():
    conn = get_db_connection()
    rows = conn.execute("""
        SELECT date, avg_pace_minutes, best_pace_minutes, distance, title
        FROM activities 
        WHERE activity_type IN ('running', 'treadmill_running', 'track_running')
          AND avg_pace_minutes IS NOT NULL
          AND distance > 0.5
        ORDER BY date ASC
    """).fetchall()
    conn.close()
    return [dict(row) for row in rows]


@app.get("/api/running/weekly-mileage")
async def get_weekly_mileage(start_date: str = None, end_date: str = None):
    conn = get_db_connection()
    query = """
        SELECT
            strftime('%Y-%W', date) as week,
            SUM(CASE WHEN activity_type = 'treadmill_running' THEN distance ELSE 0 END) as treadmill_distance,
            SUM(CASE WHEN activity_type IN ('running', 'track_running') THEN distance ELSE 0 END) as outdoor_distance,
            SUM(distance) as total_distance,
            COUNT(*) as num_runs,
            AVG(avg_pace_minutes) as avg_pace,
            AVG(avg_hr) as avg_hr,
            SUM(calories) as total_calories
        FROM activities
        WHERE activity_type IN ('running', 'treadmill_running', 'track_running')
          AND distance > 0
    """
    params = []
    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " GROUP BY week ORDER BY week ASC"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(row) for row in rows]


@app.get("/api/running/day-hour-distribution")
async def get_run_day_hour_distribution(start_date: str = None, end_date: str = None):
    """Get distribution of runs by day of week and hour of day."""
    conn = get_db_connection()
    query = """
        SELECT
            CASE CAST(strftime('%w', date) AS INTEGER)
                WHEN 0 THEN 'Sunday'
                WHEN 1 THEN 'Monday'
                WHEN 2 THEN 'Tuesday'
                WHEN 3 THEN 'Wednesday'
                WHEN 4 THEN 'Thursday'
                WHEN 5 THEN 'Friday'
                WHEN 6 THEN 'Saturday'
            END as day_of_week,
            strftime('%w', date) as day_num,
            strftime('%H', date) as hour,
            COUNT(*) as run_count,
            AVG(distance) as avg_distance,
            AVG(avg_pace_minutes) as avg_pace
        FROM activities
        WHERE activity_type IN ('running', 'treadmill_running', 'track_running')
          AND distance > 0
    """
    params = []
    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " GROUP BY day_num, hour ORDER BY day_num, hour"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(row) for row in rows]


@app.get("/api/strength/summary")
async def get_strength_summary(start_date: str = None, end_date: str = None):
    conn = get_db_connection()
    query = """
        SELECT date, title, duration_minutes, calories, avg_hr, max_hr, total_reps, total_sets
        FROM activities
        WHERE activity_type = 'strength_training'
    """
    params = []
    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date ASC"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(row) for row in rows]


@app.get("/api/strength/weekly")
async def get_strength_weekly(start_date: str = None, end_date: str = None):
    conn = get_db_connection()
    query = """
        SELECT
            strftime('%Y-%W', date) as week,
            COUNT(*) as num_sessions,
            SUM(duration_minutes) as total_duration,
            SUM(total_reps) as total_reps,
            SUM(total_sets) as total_sets,
            SUM(calories) as total_calories,
            AVG(avg_hr) as avg_hr
        FROM activities
        WHERE activity_type = 'strength_training'
    """
    params = []
    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " GROUP BY week ORDER BY week ASC"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(row) for row in rows]


@app.get("/api/metrics")
async def get_metrics(start_date: str = None, end_date: str = None):
    """Get aggregated metrics with optional date filtering."""
    conn = get_db_connection()

    date_filter = ""
    params = []
    if start_date:
        date_filter += " AND date >= ?"
        params.append(start_date)
    if end_date:
        date_filter += " AND date <= ?"
        params.append(end_date)

    run_query = f"""
        SELECT
            COUNT(*) as count,
            SUM(duration_minutes) as duration,
            SUM(distance) as distance,
            AVG(avg_hr) as avg_hr
        FROM activities
        WHERE activity_type IN ('running', 'treadmill_running', 'track_running')
        {date_filter}
    """
    run_row = conn.execute(run_query, params).fetchone()

    strength_query = f"""
        SELECT
            COUNT(*) as count,
            SUM(duration_minutes) as duration,
            AVG(avg_hr) as avg_hr
        FROM activities
        WHERE activity_type = 'strength_training'
        {date_filter}
    """
    strength_row = conn.execute(strength_query, params).fetchone()

    other_query = f"""
        SELECT
            COUNT(*) as count,
            SUM(duration_minutes) as duration,
            SUM(distance) as distance
        FROM activities
        WHERE activity_type NOT IN ('running', 'treadmill_running', 'track_running', 'strength_training')
        {date_filter}
    """
    other_row = conn.execute(other_query, params).fetchone()

    conn.close()

    return {
        "run_count": run_row["count"] or 0,
        "run_duration": run_row["duration"] or 0,
        "run_distance": run_row["distance"] or 0,
        "run_avg_hr": run_row["avg_hr"],
        "strength_count": strength_row["count"] or 0,
        "strength_duration": strength_row["duration"] or 0,
        "strength_avg_hr": strength_row["avg_hr"],
        "other_count": other_row["count"] or 0,
        "other_duration": other_row["duration"] or 0,
        "other_distance": other_row["distance"] or 0,
    }


@app.get("/api/other-activities/weekly")
async def get_other_activities_weekly(start_date: str = None, end_date: str = None):
    """Get weekly breakdown of other activities (not running or strength)."""
    conn = get_db_connection()

    query = """
        SELECT
            strftime('%Y-%W', date) as week,
            activity_type,
            SUM(duration_minutes) as total_duration,
            SUM(distance) as total_distance,
            COUNT(*) as count
        FROM activities
        WHERE activity_type NOT IN ('running', 'treadmill_running', 'track_running', 'strength_training')
    """
    params = []
    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " GROUP BY week, activity_type ORDER BY week ASC, activity_type"
    rows = conn.execute(query, params).fetchall()
    conn.close()

    weekly_data = {}
    for row in rows:
        week = row["week"]
        if week not in weekly_data:
            weekly_data[week] = []
        weekly_data[week].append({
            "activity_type": row["activity_type"],
            "total_duration": row["total_duration"],
            "total_distance": row["total_distance"],
            "count": row["count"]
        })

    return [{"week": week, "activities": activities} for week, activities in weekly_data.items()]


@app.get("/api/summary")
async def get_summary():
    conn = get_db_connection()
    summary = {}

    row = conn.execute("""
        SELECT COUNT(*) as total_activities, SUM(calories) as total_calories,
               SUM(duration_minutes) as total_duration
        FROM activities
    """).fetchone()
    summary["totals"] = dict(row)

    rows = conn.execute("""
        SELECT activity_type, COUNT(*) as count, SUM(distance) as total_distance,
               SUM(calories) as total_calories, SUM(duration_minutes) as total_duration
        FROM activities GROUP BY activity_type
    """).fetchall()
    summary["by_type"] = [dict(row) for row in rows]

    row = conn.execute("""
        SELECT SUM(distance) as total_miles, AVG(avg_pace_minutes) as avg_pace,
               MIN(best_pace_minutes) as best_pace, AVG(avg_hr) as avg_hr
        FROM activities
        WHERE activity_type IN ('running', 'treadmill_running', 'track_running')
          AND distance > 0.5
    """).fetchone()
    summary["running"] = dict(row)

    row = conn.execute("""
        SELECT SUM(total_reps) as total_reps, SUM(total_sets) as total_sets,
               AVG(duration_minutes) as avg_duration, SUM(calories) as total_calories
        FROM activities WHERE activity_type = 'strength_training'
    """).fetchone()
    summary["strength"] = dict(row)

    conn.close()
    return summary


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve the main dashboard with sync status."""
    html_path = Path(__file__).parent / "dashboard_new.html"
    return html_path.read_text(encoding="utf-8")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)