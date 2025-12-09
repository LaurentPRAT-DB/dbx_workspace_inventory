# Workspace File System vs DBFS

## The Key Difference

Databricks has **two completely separate file systems**. This is why you can see files in the UI but the API might not find them.

## 1. Workspace File System

**What it is:** Stores notebooks, libraries, and workspace objects

**Paths:**
- UI Path: `Workspace → Users → username`
- API Path: `/Users/username`
- API Endpoint: `/api/2.0/workspace/list`

**Contains:**
- Notebooks (`.py`, `.sql`, `.scala`, `.r`)
- Folders/directories
- Libraries
- Repos
- MLflow experiments

**What you see:**
```
Workspace
└── Users
    └── user@example.com
        ├── my_notebook.py
        ├── analysis.sql
        └── project_folder/
            └── helper_notebook.py
```

**Access via script:**
```bash
# Now automatically tried!
python databricks_user_files_simple.py user@example.com --profile PROD --debug
```

## 2. DBFS (Databricks File System)

**What it is:** Stores data files, datasets, and uploaded files

**Paths:**
- UI Path: `Data → DBFS → Users → username`
- API Path: `dbfs:/Users/username` or `/dbfs/Users/username`
- API Endpoint: `/api/2.0/dbfs/list`

**Contains:**
- CSV files
- Parquet files
- JSON files
- Images, videos
- Any uploaded data

**What you see:**
```
DBFS
└── Users
    └── user@example.com
        ├── data.csv
        ├── output.parquet
        └── datasets/
            └── training.csv
```

**Access via script:**
```bash
# Primary method - DBFS API
python databricks_user_files_simple.py user@example.com --profile PROD

# Or with cluster for parallel processing
python databricks_user_files_simple.py --users-file users.csv --cluster-id CLUSTER_ID --profile PROD
```

## Why This Matters

### Scenario: "I can see files in the UI but the API says no files found"

**You're probably looking at:** Workspace File System (notebooks)
**The API was checking:** DBFS (data files)

**Solution:** The script now scans BOTH and cumulates the results!

### Example Output

**When you have files in both locations:**
```
File Listing Status:
  ✅ Successfully scanned both file systems
  ✅ DBFS: 1234 files
  ✅ Workspace: 15 files (notebooks/libraries)

Combined Results:
  Total files: 1,249 (from DBFS + Workspace)
  Total size: 52,828,912 bytes (0.05 GB)
```

**When you have notebooks but no DBFS files:**
```
File Listing Status:
  ✅ Successfully scanned both file systems
  ⊘ DBFS: No files
  ✅ Workspace: 15 files (notebooks/libraries)

Combined Results:
  Total files: 15 (from Workspace)
  Total size: 150,000 bytes (0.00 GB)
```

## How the Script Now Works

### Cumulative Scanning (Not Fallback!)

1. **Scan DBFS API** (data files)
   - Endpoint: `/api/2.0/dbfs/list`
   - Path: `dbfs:/Users/{username}`

2. **Scan Workspace API** (notebooks) - ALWAYS scanned, not just fallback
   - Endpoint: `/api/2.0/workspace/list`
   - Path: `/Users/{username}`

3. **Cumulate results** from both sources
   - File counts are added together
   - Sizes are added together
   - Source is marked as: `both`, `dbfs`, `workspace`, or `none`

### Commands

**Check single user (tries both):**
```bash
python databricks_user_files_simple.py user@example.com --profile PROD --debug
```

**Check multiple users in parallel:**
```bash
python databricks_user_files_simple.py --users-file users.csv --cluster-id CLUSTER_ID --profile PROD
```

## Permissions Required

### For DBFS Access
- Token needs: DBFS read permissions
- Grants access to: Data files in `dbfs:/Users/`

### For Workspace Access
- Token needs: Workspace read permissions
- Grants access to: Notebooks in `/Users/`

### Create Token with Both
```
Databricks UI:
1. Settings → Developer → Access tokens
2. Generate new token
3. Ensure it has:
   - Workspace access
   - DBFS access (usually included by default)
```

## Troubleshooting

### "Both Spark Connect and DBFS API methods failed"

**This is now outdated!** The script will try Workspace API too.

**If you still see this:**
- All three methods failed (Spark, DBFS, Workspace)
- Check token permissions
- Verify user exists
- Run with `--debug` for details

### "Successfully listed files via Workspace API"

**This is good!** It means:
- ✅ You have notebooks/workspace files
- ℹ️  You might not have DBFS data files (normal)

### "No files found in either location"

**This means:**
- No notebooks in Workspace File System
- No data files in DBFS
- User directory is truly empty (normal for new users)

## API Comparison

| Feature | DBFS API | Workspace API |
|---------|----------|---------------|
| Endpoint | `/api/2.0/dbfs/list` | `/api/2.0/workspace/list` |
| Lists | Data files | Notebooks, folders |
| Path | `dbfs:/Users/...` | `/Users/...` |
| File sizes | ✅ Exact sizes | ⚠️ Estimated (10KB avg) |
| UI location | Data → DBFS | Workspace → Users |
| Typical use | Data storage | Code storage |

## Summary

- **Two file systems** = Two different locations that are both scanned
- **Script cumulates results** = File counts and sizes from both DBFS and Workspace are added together
- **DBFS** = Data files (CSV, Parquet, etc.)
- **Workspace** = Notebooks and code
- **file_source column** = Shows which system(s) had files: `both`, `dbfs`, `workspace`, or `none`
- **Most users** = Have notebooks (Workspace) and may also have data (DBFS)

This is working as designed! The script gives you a complete inventory by scanning both file systems.
