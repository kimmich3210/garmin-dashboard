#!/usr/bin/env python3
"""
Run the Garmin Dashboard with auto-sync enabled.
Loads credentials from .env file.
"""

from dotenv import load_dotenv
import uvicorn

# Load environment variables from .env file
load_dotenv()

if __name__ == "__main__":
    uvicorn.run("app_autosync:app", host="0.0.0.0", port=8000, reload=True)