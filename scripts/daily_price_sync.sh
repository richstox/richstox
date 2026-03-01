#!/bin/bash
# RICHSTOX Daily Price Sync Cron Job
# Runs at 22:00 CET (16:00 ET) after US market close
# Monday through Friday only

# Get the API URL from environment or use default
API_URL="${REACT_APP_BACKEND_URL:-https://ticker-detail-v2.preview.emergentagent.com}"

# Log file
LOG_FILE="/var/log/richstox_daily_sync.log"

# Run the daily sync
echo "$(date '+%Y-%m-%d %H:%M:%S') - Starting daily price sync" >> "$LOG_FILE"

RESPONSE=$(curl -s -X POST "${API_URL}/api/admin/prices/sync-daily" 2>&1)

echo "$(date '+%Y-%m-%d %H:%M:%S') - Response: $RESPONSE" >> "$LOG_FILE"
echo "---" >> "$LOG_FILE"
