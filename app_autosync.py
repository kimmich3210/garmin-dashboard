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

            # Save session for future use
            self.client.garth.dump(TOKEN_PATH)
            logger.info("Fresh login successful, session saved")
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
            # Garmin API returns activities in reverse chronological order
            # Fetch in batches to get full lookback period
            cutoff_date = datetime.now() - timedelta(days=days_back)

            all_activities = []
            batch_size = 100
            start = 0

            # Keep fetching until we've gone past the cutoff date or hit 500 activities max
            while start < 500:
                batch = self.client.get_activities(start, batch_size)

                if not batch:
                    break

                # Check if we've gone past the cutoff date
                oldest_in_batch = datetime.fromisoformat(
                    batch[-1]["startTimeLocal"].replace("Z", "+00:00")
                )

                for activity in batch:
                    activity_date = datetime.fromisoformat(
                        activity["startTimeLocal"].replace("Z", "+00:00")
                    )
                    if activity_date >= cutoff_date:
                        all_activities.append(self._normalize_activity(activity))

                # If the oldest activity in this batch is before cutoff, we're done
                if oldest_in_batch < cutoff_date:
                    break

                start += batch_size

            logger.info(f"Fetched {len(all_activities)} activities from last {days_back} days")
            return all_activities

        except Exception as e:
            logger.error(f"Failed to fetch activities: {e}")
            # Try re-login once
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
            # m/s -> min/mile
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
    # Startup
    init_db()
    
    # Initial sync on startup
    if GARMIN_EMAIL and GARMIN_PASSWORD:
        logger.info("Credentials found, performing initial sync...")
        await scheduled_sync()
        
        # Schedule recurring syncs
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
    
    # Shutdown
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

    # Build base query with date filters
    date_filter = ""
    params = []
    if start_date:
        date_filter += " AND date >= ?"
        params.append(start_date)
    if end_date:
        date_filter += " AND date <= ?"
        params.append(end_date)

    # Running metrics
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

    # Strength training metrics
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

    # Other activities metrics
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

    # Group by week
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
    return html_path.read_text()


# ============================================================================
# HTML TEMPLATE (same as before, with sync status added)
# ============================================================================

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Garmin Workout Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0f0f0f; color: #e0e0e0; min-height: 100vh; padding: 20px;
        }
        .header { text-align: center; margin-bottom: 30px; padding: 20px; }
        .header h1 { font-size: 2.5rem; color: #00d4ff; margin-bottom: 10px; }
        .header p { color: #888; }

        .controls {
            display: flex; justify-content: center; align-items: center; gap: 20px; flex-wrap: wrap;
            background: #1a1a1a; padding: 15px 20px; border-radius: 8px;
            margin: 20px auto; max-width: 1200px; border: 1px solid #333;
        }
        .controls label { color: #888; font-size: 0.9rem; }
        .controls input[type="date"] {
            background: #0f0f0f; color: #e0e0e0; border: 1px solid #333;
            padding: 8px 12px; border-radius: 5px; font-size: 0.9rem;
        }
        .controls input[type="date"]::-webkit-calendar-picker-indicator {
            filter: invert(1);
        }
        .sync-status {
            display: inline-flex; align-items: center; gap: 10px;
        }
        .sync-indicator { width: 10px; height: 10px; border-radius: 50%; }
        .sync-indicator.active { background: #4ade80; box-shadow: 0 0 10px #4ade80; }
        .sync-indicator.inactive { background: #f87171; }
        .sync-info { font-size: 0.85rem; color: #888; }

        .sync-btn, .filter-btn {
            background: #00d4ff; color: #0f0f0f; border: none;
            padding: 8px 16px; border-radius: 5px; cursor: pointer; font-weight: bold;
            font-size: 0.9rem;
        }
        .sync-btn:hover, .filter-btn:hover { background: #00a8cc; }
        .sync-btn:disabled, .filter-btn:disabled { background: #555; cursor: not-allowed; }

        .section { margin-bottom: 40px; max-width: 1400px; margin-left: auto; margin-right: auto; }
        .section h2 {
            color: #fff; margin-bottom: 20px; padding-bottom: 10px;
            border-bottom: 2px solid #00d4ff; display: flex; align-items: center; gap: 10px;
        }

        .charts-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(500px, 1fr)); gap: 20px; }
        .chart-container {
            background: #1a1a1a; border-radius: 10px; padding: 20px; border: 1px solid #333;
        }
        .chart-container h3 { color: #fff; margin-bottom: 15px; font-size: 1.1rem; }
        .chart-wrapper { position: relative; height: 350px; }

        .activities-table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        .activities-table th, .activities-table td {
            padding: 12px; text-align: left; border-bottom: 1px solid #333;
        }
        .activities-table th { background: #1a1a1a; color: #00d4ff; font-weight: 600; }
        .activities-table tr:hover { background: #1a1a1a; }

        .activity-type {
            display: inline-block; padding: 4px 8px; border-radius: 4px;
            font-size: 0.8rem; font-weight: 500;
        }
        .type-running { background: #2d5a2d; color: #90EE90; }
        .type-strength { background: #5a2d2d; color: #FFB6C1; }
        .type-walking { background: #2d4a5a; color: #87CEEB; }
        .type-other { background: #4a4a4a; color: #ddd; }

        @media (max-width: 768px) {
            .charts-grid { grid-template-columns: 1fr; }
            .chart-wrapper { height: 300px; }
            .controls { flex-direction: column; align-items: stretch; }
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🏃 Garmin Dashboard</h1>
        <p>Your workout data, visualized</p>
        <div class="sync-status">
            <div class="sync-indicator" id="syncIndicator"></div>
            <div class="sync-info">
                <div id="syncStatusText">Checking...</div>
                <div id="lastSyncText"></div>
            </div>
            <button class="sync-btn" id="syncBtn" onclick="triggerSync()">Sync Now</button>
        </div>
    </div>
    
    <div class="stats-grid" id="statsGrid"></div>
    
    <div class="section">
        <h2>🏃 Running</h2>
        <div class="charts-grid">
            <div class="chart-container">
                <h3>Pace Trend</h3>
                <div class="chart-wrapper"><canvas id="paceChart"></canvas></div>
            </div>
            <div class="chart-container">
                <h3>Weekly Mileage</h3>
                <div class="chart-wrapper"><canvas id="mileageChart"></canvas></div>
            </div>
        </div>
    </div>
    
    <div class="section">
        <h2>💪 Strength Training</h2>
        <div class="charts-grid">
            <div class="chart-container">
                <h3>Volume Over Time (Reps)</h3>
                <div class="chart-wrapper"><canvas id="strengthRepsChart"></canvas></div>
            </div>
            <div class="chart-container">
                <h3>Session Duration & Calories</h3>
                <div class="chart-wrapper"><canvas id="strengthDurationChart"></canvas></div>
            </div>
        </div>
    </div>
    
    <div class="section">
        <h2>📋 Recent Activities</h2>
        <table class="activities-table">
            <thead>
                <tr><th>Date</th><th>Type</th><th>Title</th><th>Duration</th><th>Distance</th><th>Calories</th><th>Avg HR</th></tr>
            </thead>
            <tbody id="activitiesBody"></tbody>
        </table>
    </div>
    
    <script>
        Chart.defaults.color = '#888';
        Chart.defaults.borderColor = '#333';
        
        let paceChart, mileageChart, strengthRepsChart, strengthDurationChart;
        
        function formatPace(m) {
            if (!m) return '--';
            const mins = Math.floor(m), secs = Math.round((m - mins) * 60);
            return `${mins}:${secs.toString().padStart(2, '0')}`;
        }
        
        function formatDuration(m) {
            if (!m) return '--';
            const hrs = Math.floor(m / 60), mins = Math.round(m % 60);
            return hrs > 0 ? `${hrs}h ${mins}m` : `${mins}m`;
        }
        
        function getTypeClass(t) {
            if (t.includes('running')) return 'type-running';
            if (t.includes('strength')) return 'type-strength';
            if (t.includes('walking')) return 'type-walking';
            return 'type-other';
        }
        
        async function loadSyncStatus() {
            try {
                const res = await fetch('/api/sync/status');
                const data = await res.json();
                
                const indicator = document.getElementById('syncIndicator');
                const statusText = document.getElementById('syncStatusText');
                const lastSync = document.getElementById('lastSyncText');
                const btn = document.getElementById('syncBtn');
                
                if (data.auto_sync_enabled) {
                    indicator.className = 'sync-indicator active';
                    statusText.textContent = `Auto-sync every ${data.sync_interval_hours}h`;
                    btn.style.display = 'inline-block';
                } else {
                    indicator.className = 'sync-indicator inactive';
                    statusText.textContent = 'Auto-sync disabled (no credentials)';
                    btn.style.display = 'none';
                }
                
                if (data.last_sync) {
                    const d = new Date(data.last_sync);
                    lastSync.textContent = `Last sync: ${d.toLocaleString()}`;
                }
            } catch (e) {
                console.error('Failed to load sync status', e);
            }
        }
        
        async function triggerSync() {
            const btn = document.getElementById('syncBtn');
            btn.textContent = 'Syncing...';
            btn.disabled = true;
            
            try {
                const res = await fetch('/api/sync/now', { method: 'POST' });
                const data = await res.json();
                alert(data.message);
                await loadAllData();
            } catch (e) {
                alert('Sync failed: ' + e.message);
            } finally {
                btn.textContent = 'Sync Now';
                btn.disabled = false;
                loadSyncStatus();
            }
        }
        
        async function loadSummary() {
            const res = await fetch('/api/summary');
            const data = await res.json();
            document.getElementById('statsGrid').innerHTML = `
                <div class="stat-card"><h3>Total Workouts</h3><div class="value">${data.totals.total_activities || 0}</div></div>
                <div class="stat-card"><h3>Total Calories</h3><div class="value">${(data.totals.total_calories || 0).toLocaleString()}</div><div class="unit">kcal</div></div>
                <div class="stat-card"><h3>Running Miles</h3><div class="value">${(data.running.total_miles || 0).toFixed(1)}</div><div class="unit">miles</div></div>
                <div class="stat-card"><h3>Avg Running Pace</h3><div class="value">${formatPace(data.running.avg_pace)}</div><div class="unit">/mile</div></div>
                <div class="stat-card"><h3>Best Pace</h3><div class="value">${formatPace(data.running.best_pace)}</div><div class="unit">/mile</div></div>
                <div class="stat-card"><h3>Total Reps</h3><div class="value">${(data.strength.total_reps || 0).toLocaleString()}</div><div class="unit">strength</div></div>
            `;
        }
        
        async function loadPaceChart() {
            const res = await fetch('/api/running/pace-trend');
            const data = await res.json();
            const ctx = document.getElementById('paceChart').getContext('2d');
            if (paceChart) paceChart.destroy();
            paceChart = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: data.map(d => d.date?.split('T')[0] || d.date?.split(' ')[0]),
                    datasets: [{
                        label: 'Avg Pace', data: data.map(d => d.avg_pace_minutes),
                        borderColor: '#00d4ff', backgroundColor: 'rgba(0,212,255,0.1)', fill: true, tension: 0.3
                    }, {
                        label: 'Best Pace', data: data.map(d => d.best_pace_minutes),
                        borderColor: '#90EE90', borderDash: [5,5], fill: false, tension: 0.3
                    }]
                },
                options: {
                    responsive: true, maintainAspectRatio: false,
                    scales: { y: { reverse: true, title: { display: true, text: 'Pace (min/mile)' },
                        ticks: { callback: v => formatPace(v) } } },
                    plugins: { tooltip: { callbacks: { label: ctx => ctx.dataset.label + ': ' + formatPace(ctx.raw) } } }
                }
            });
        }
        
        async function loadMileageChart() {
            const res = await fetch('/api/running/weekly-mileage');
            const data = await res.json();
            const ctx = document.getElementById('mileageChart').getContext('2d');
            if (mileageChart) mileageChart.destroy();
            mileageChart = new Chart(ctx, {
                type: 'bar',
                data: { labels: data.map(d => d.week), datasets: [{
                    label: 'Weekly Miles', data: data.map(d => d.total_distance),
                    backgroundColor: 'rgba(0,212,255,0.7)', borderColor: '#00d4ff', borderWidth: 1
                }] },
                options: { responsive: true, maintainAspectRatio: false,
                    scales: { y: { beginAtZero: true, title: { display: true, text: 'Miles' } } } }
            });
        }
        
        async function loadStrengthCharts() {
            const res = await fetch('/api/strength/summary');
            const data = await res.json();
            
            const ctx1 = document.getElementById('strengthRepsChart').getContext('2d');
            if (strengthRepsChart) strengthRepsChart.destroy();
            strengthRepsChart = new Chart(ctx1, {
                type: 'bar',
                data: { labels: data.map(d => d.date?.split('T')[0] || d.date?.split(' ')[0]), datasets: [{
                    label: 'Total Reps', data: data.map(d => d.total_reps),
                    backgroundColor: 'rgba(255,182,193,0.7)', borderColor: '#FFB6C1', borderWidth: 1
                }, {
                    label: 'Total Sets', data: data.map(d => d.total_sets),
                    backgroundColor: 'rgba(255,215,0,0.7)', borderColor: '#FFD700', borderWidth: 1
                }] },
                options: { responsive: true, maintainAspectRatio: false,
                    scales: { y: { beginAtZero: true, title: { display: true, text: 'Count' } } } }
            });
            
            const ctx2 = document.getElementById('strengthDurationChart').getContext('2d');
            if (strengthDurationChart) strengthDurationChart.destroy();
            strengthDurationChart = new Chart(ctx2, {
                type: 'line',
                data: { labels: data.map(d => d.date?.split('T')[0] || d.date?.split(' ')[0]), datasets: [{
                    label: 'Duration (min)', data: data.map(d => d.duration_minutes),
                    borderColor: '#FFB6C1', backgroundColor: 'rgba(255,182,193,0.1)', fill: true, tension: 0.3, yAxisID: 'y'
                }, {
                    label: 'Calories', data: data.map(d => d.calories),
                    borderColor: '#FFD700', fill: false, tension: 0.3, yAxisID: 'y1'
                }] },
                options: { responsive: true, maintainAspectRatio: false, scales: {
                    y: { type: 'linear', position: 'left', title: { display: true, text: 'Duration (min)' }, beginAtZero: true },
                    y1: { type: 'linear', position: 'right', title: { display: true, text: 'Calories' }, beginAtZero: true, grid: { drawOnChartArea: false } }
                } }
            });
        }
        
        async function loadActivities() {
            const res = await fetch('/api/activities?limit=20');
            const data = await res.json();
            document.getElementById('activitiesBody').innerHTML = data.map(a => `
                <tr>
                    <td>${a.date ? (a.date.split('T')[0] || a.date.split(' ')[0]) : '--'}</td>
                    <td><span class="activity-type ${getTypeClass(a.activity_type)}">${a.activity_type}</span></td>
                    <td>${a.title || '--'}</td>
                    <td>${formatDuration(a.duration_minutes)}</td>
                    <td>${a.distance ? a.distance.toFixed(2) + ' mi' : '--'}</td>
                    <td>${a.calories || '--'}</td>
                    <td>${a.avg_hr || '--'}</td>
                </tr>
            `).join('');
        }
        
        async function loadAllData() {
            await Promise.all([loadSyncStatus(), loadSummary(), loadPaceChart(), loadMileageChart(), loadStrengthCharts(), loadActivities()]);
        }
        
        loadAllData();
    </script>
</body>
</html>
"""

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)