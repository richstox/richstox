# RICHSTOX Database Migration Plan
## P0 Phase 3: test_database → richstox_prod

### Overview
This document outlines the migration from `test_database` to `richstox_prod` as part of the P0 Production-Grade Integrity initiative.

### Pre-Migration Checklist
- [ ] Backup `test_database` created
- [ ] All services stopped (backend, scheduler)
- [ ] Disk space verified (need ~1GB free)
- [ ] Index list documented

### Step 1: Create Backup (mongodump)
```bash
# Create timestamped backup archive
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
mongodump --db=test_database --archive=/app/backup_test_database_${TIMESTAMP}.gz --gzip

# Verify backup size
ls -lh /app/backup_test_database_*.gz
```

### Step 2: Restore to richstox_prod (mongorestore)
```bash
# Restore with namespace transformation
mongorestore \
  --archive=/app/backup_test_database_TIMESTAMP.gz \
  --gzip \
  --nsFrom='test_database.*' \
  --nsTo='richstox_prod.*'
```

### Step 3: Verify Index Migration
```bash
# List all indexes in new database
mongosh --quiet richstox_prod --eval '
  print("=== INDEXES in richstox_prod ===");
  db.getCollectionNames().forEach(c => {
    print(c + ":");
    db[c].getIndexes().forEach(idx => print("  - " + JSON.stringify(idx.key)));
  });
'
```

### Critical Indexes to Verify
| Collection | Index Name | Key | Unique |
|------------|-----------|-----|--------|
| stock_prices | ticker_date_unique | {ticker: 1, date: 1} | YES |
| tracked_tickers | ticker_unique | {ticker: 1} | YES |
| company_fundamentals_cache | symbol_unique | {symbol: 1} | YES |
| users | email_unique | {email: 1} | YES |

### Step 4: Update Environment
```bash
# Edit /app/backend/.env
# BEFORE: DB_NAME="test_database"
# AFTER:  DB_NAME="richstox_prod"

# Restart backend
sudo supervisorctl restart backend
```

### Step 5: Verify Startup Guard
After restarting, check backend logs for:
```
✅ ENV/DB Guard: ENV=development, DB_NAME=richstox_prod
```

### Rollback Procedure
If migration fails:
```bash
# 1. Revert .env
sed -i 's/richstox_prod/test_database/g' /app/backend/.env

# 2. Restart backend
sudo supervisorctl restart backend

# 3. (Optional) Drop failed database
mongosh --eval 'db.getSiblingDB("richstox_prod").dropDatabase()'
```

### Post-Migration Tasks
1. Run completeness report: `python scripts/completeness_report.py`
2. Re-backfill all visible tickers via Admin Panel
3. Verify startup guard is GREEN in Admin Panel

### Execution Log
| Step | Timestamp | Status | Notes |
|------|-----------|--------|-------|
| Backup | | | |
| Restore | | | |
| Index Verify | | | |
| ENV Update | | | |
| Startup Guard | | | |
| Completeness Report | | | |
