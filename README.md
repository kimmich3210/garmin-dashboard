# Garmin Dashboard

A self-hosted dashboard for visualizing your Garmin Connect workout data with automatic syncing.

## Features

- **Auto-sync** - Automatically fetches your activities from Garmin Connect every 12 hours
- **Interactive Dashboard** - Beautiful dark-themed visualizations of your workout data
- **Date Filtering** - Filter all charts and metrics by custom date ranges
- **Running Analytics** - Track weekly mileage, treadmill vs outdoor runs, and running schedule patterns
- **Strength Training** - Monitor duration, reps, sets, and calories with moving averages
- **Other Activities** - Track walking, basketball, and other non-primary activities
- **Comprehensive Metrics** - View aggregated statistics across all activity types

## Setup

### Prerequisites

- Python 3.10 or higher
- Garmin Connect account

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/garmin-dashboard.git
   cd garmin-dashboard
   ```

2. **Create a virtual environment** (recommended)
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements_autosync.txt
   ```

4. **Configure credentials**
   ```bash
   cp .env.example .env
   ```

   Edit `.env` and add your Garmin Connect credentials:
   ```
   GARMIN_EMAIL="your-email@example.com"
   GARMIN_PASSWORD="your-password"
   ```

5. **Run the application**
   ```bash
   python run.py
   ```

6. **Open the dashboard**

   Navigate to [http://localhost:8000](http://localhost:8000) in your browser

## Configuration

Edit `.env` to customize:

- `SYNC_INTERVAL_HOURS` - How often to auto-sync (default: 12 hours)
- `ACTIVITY_LOOKBACK_DAYS` - How far back to fetch activities (default: 360 days)

## Security Notes

- Never commit your `.env` file to version control
- Your credentials are stored locally and only used to authenticate with Garmin Connect
- Session tokens are cached in `.garmin_session` to avoid repeated logins

## Project Structure

- `app_autosync.py` - Main FastAPI application with auto-sync logic
- `dashboard_new.html` - Interactive dashboard interface
- `run.py` - Entry point script
- `requirements_autosync.txt` - Python dependencies
- `.env` - Configuration file with credentials (not committed)
- `workouts.db` - SQLite database (created on first run)
