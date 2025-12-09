# Databricks Workspace Inventory Tools

Fast, scalable tools to inventory user files across your entire Databricks workspace. List users, scan their home directories, and generate comprehensive reports with **parallel processing** for maximum speed.

## 🚀 Quick Start

### Two-Step Workflow

```bash
# Step 1: Export all workspace users to a file
python databricks_user_list.py --profile PROD --output users.csv

# Step 2: Scan all user directories in parallel (10-100x faster!)
python databricks_user_files_simple.py \
  --users-file users.csv \
  --profile PROD \
  --cluster-id YOUR_CLUSTER_ID \
  --output results.csv
```

**That's it!** You now have a complete inventory of all files across your workspace.

## ✨ Key Features

- **⚡ Parallel Processing** - 10-100x faster by distributing work across cluster workers
- **🔍 Dual File System Scanning** - Automatically scans both DBFS (data files) and Workspace (notebooks) for every user
- **➕ Cumulative Results** - Combines file counts and sizes from both DBFS and Workspace into total inventory
- **💾 Checkpoint & Resume** - Automatically saves progress; resume from where you left off after timeouts
- **🛡️ Robust Error Handling** - Automatic retry with exponential backoff for rate limits (429), server errors (500/503), and network issues
- **📊 Comprehensive Results** - Tracks file count, size, source, and status for each user
- **🎯 Real-Time Progress** - See live updates as workers process users in parallel
- **📈 Scalable** - Efficiently handles 1000+ users with proper cluster sizing
- **🔧 SparkConnect Compatible** - Works with both traditional and serverless clusters

## 📊 What You Get

### User List (`users.csv`)
```
john.doe@company.com
jane.smith@company.com
alice.wong@company.com
...
```

### Inventory Results (`results.csv`)
```csv
username,file_count,total_size,total_size_gb,status,file_source,error
john.doe@company.com,1234,52678912,0.05,success,both,
jane.smith@company.com,567,24234567,0.02,success,dbfs,
alice.wong@company.com,15,150000,0.00,success,workspace,
bob.jones@company.com,0,0,0.00,empty,none,
```

**Note:** The tool automatically scans and **cumulates** results from both file systems:
- **DBFS** (Databricks File System) - Data files, CSV, Parquet, etc.
- **Workspace** - Notebooks, libraries, Python files
- **Both** are scanned for every user, and file counts/sizes are added together

The `file_source` column shows which file system(s) contained files: `both`, `dbfs`, `workspace`, or `none`.

See [WORKSPACE_VS_DBFS.md](WORKSPACE_VS_DBFS.md) for details on the two file systems.

## ⚡ Performance

| Users | Sequential Mode | Parallel Mode (8 workers) | Speedup |
|-------|----------------|---------------------------|---------|
| 10    | ~5 minutes     | ~1 minute                 | 5x      |
| 100   | ~50 minutes    | ~5 minutes                | 10x     |
| 1000  | ~8 hours       | ~30 minutes               | 16x     |

**With parallel processing, you can scan 1000+ users in under an hour!**

---

## 📚 Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Tool 1: User List Export](#tool-1-user-list-export)
- [Tool 2: Workspace Inventory](#tool-2-workspace-inventory)
- [Authentication](#authentication)
- [Output Formats](#output-formats)
- [Performance Tips](#performance-tips)
- [Troubleshooting](#troubleshooting)
- [Advanced Usage](#advanced-usage)

---

## Prerequisites

### Required
- **Databricks workspace** with access to users and their home directories
- **Databricks Personal Access Token** with permissions:
  - `users:read` - List workspace users via SCIM API
  - Read access to user home directories in DBFS (`dbfs:/Users/`)
  - Read access to user workspace directories (`/Users/`)
- **Python 3.10+** installed locally

**Note:** The tool automatically scans both DBFS and Workspace file systems to provide complete coverage.

### For Parallel Processing (Recommended)
- **Running Databricks cluster** (All-Purpose or Job cluster)
  - Databricks Runtime 13.0+ recommended
  - 4-8 workers for optimal performance
  - **Note:** Serverless clusters have limited support

### Optional but Recommended
- **Databricks CLI** - Simplifies authentication
  ```bash
  # macOS
  brew install databricks-cli

  # Other platforms
  pip install databricks-cli
  ```

---

## Installation

1. **Clone this repository**
   ```bash
   git clone https://github.com/LaurentPRAT-DB/dbx_workspace_inventory.git
   cd dbx_workspace_inventory
   ```

2. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure authentication** (choose one method):

   **Option A: Databricks CLI (Recommended)**
   ```bash
   databricks configure --token
   # Enter your workspace URL and token when prompted
   ```

   **Option B: Environment Variables**
   ```bash
   export DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"
   export DATABRICKS_TOKEN="your-personal-access-token"
   ```

---

## Tool 1: User List Export

Export all workspace users to a CSV file (one email per line).

### Basic Usage

```bash
# Export all users
python databricks_user_list.py --profile PROD --output users.csv
```

### Output Example

```
================================================================================
DATABRICKS USER LISTING
================================================================================
Start time: 2025-12-06 14:23:15

Using Databricks CLI configuration (profile: PROD)
Workspace: https://your-workspace.cloud.databricks.com
Fetching users...

Found 150 users

✓ Saved 150 email addresses to: users.csv
  Format: One email per line

================================================================================
COMPLETED SUCCESSFULLY
================================================================================
End time: 2025-12-06 14:23:47
Duration: 32s
Users processed: 150
Output file: users.csv
================================================================================
```

### Command-Line Options

```bash
python databricks_user_list.py [OPTIONS]

Options:
  -p, --profile PROFILE    Databricks CLI profile name
  --workspace-url URL      Workspace URL (overrides profile)
  --token TOKEN            Access token (overrides profile)
  -o, --output FILE        Output CSV file path
  --max-users N            Limit to first N users (for testing)
  --debug                  Enable debug output
  --list-profiles          Show available CLI profiles

Examples:
  # Export all users
  python databricks_user_list.py --profile PROD --output users.csv

  # Test with first 10 users
  python databricks_user_list.py --profile PROD --max-users 10 --output test_users.csv

  # List available profiles
  python databricks_user_list.py --list-profiles
```

---

## Tool 2: Workspace Inventory

Scan user home directories and generate inventory reports. Supports **sequential** and **parallel** processing modes.

### 🚀 Parallel Mode (Recommended)

**10-100x faster** by distributing work across cluster workers!

```bash
# Process all users in parallel
python databricks_user_files_simple.py \
  --users-file users.csv \
  --profile PROD \
  --cluster-id YOUR_CLUSTER_ID \
  --output results.csv
```

### Output Example (Parallel Mode)

```
================================================================================
DATABRICKS USER FILES LISTING
================================================================================
Start time: 2025-12-06 14:35:00
Users to process: 100
Mode: PARALLEL (using cluster 1205-132411-7d3zw4ya)
================================================================================

================================================================================
PARALLEL PROCESSING 100 USERS USING SPARK CLUSTER
================================================================================
Parallel start time: 2025-12-06 14:35:02

Connecting to cluster: 1205-132411-7d3zw4ya
Processing 100 users across Spark workers...
Distributing work to cluster workers...
Collecting results from workers...

Parallel processing completed in 5m 30s

[1/100] john.doe@company.com
  ✓ Files: 1,234, Size: 50.2 MB

[2/100] jane.smith@company.com
  ✓ Files: 567, Size: 23.1 MB

...

================================================================================
SUMMARY
================================================================================
Total users processed: 100
  ✓ Successful (with files): 95
  ⊘ Empty directories: 3
  ✗ Errors: 2

Total files across all users: 125,678
Total size across all users: 45.2 GB (48,567,890,123 bytes)
================================================================================

Results saved to: results.csv

================================================================================
COMPLETED SUCCESSFULLY
================================================================================
End time: 2025-12-06 14:40:35
Duration: 5m 35s
Users processed: 100
Output file: results.csv
================================================================================
```

### Sequential Mode

Process users one at a time (no cluster needed, but slower):

```bash
# Sequential processing (no cluster required)
python databricks_user_files_simple.py \
  john.doe@company.com \
  jane.smith@company.com \
  --profile PROD \
  --output results.csv
```

### Command-Line Options

```bash
python databricks_user_files_simple.py [USERS...] [OPTIONS]

Arguments:
  USERS                    One or more email addresses to process

Options:
  --users-file FILE        File containing usernames (one per line)
  -p, --profile PROFILE    Databricks CLI profile name
  --workspace-url URL      Workspace URL (overrides profile)
  --token TOKEN            Access token (overrides profile)
  --cluster-id ID          Cluster ID (enables PARALLEL processing)
  -o, --output FILE        Output CSV file path
  --no-parallel            Force sequential mode (even with cluster)
  --resume                 Resume from checkpoint file (.checkpoint_progress.json)
  --debug                  Enable debug output

Examples:
  # Single user (detailed output)
  python databricks_user_files_simple.py user@example.com --profile PROD

  # Multiple users from command line
  python databricks_user_files_simple.py user1@example.com user2@example.com --profile PROD

  # Parallel processing (RECOMMENDED for many users)
  python databricks_user_files_simple.py \
    --users-file users.csv \
    --profile PROD \
    --cluster-id 1234-567890-abc123 \
    --output results.csv

  # Resume from checkpoint after timeout/failure
  python databricks_user_files_simple.py \
    --users-file users.csv \
    --profile PROD \
    --cluster-id 1234-567890-abc123 \
    --resume

  # Sequential processing (no cluster)
  python databricks_user_files_simple.py \
    --users-file users.csv \
    --profile PROD \
    --output results.csv
```

### Debug Mode - Real-Time Progress

Enable `--debug` to see per-user progress in real-time, even during parallel processing:

```bash
python databricks_user_files_simple.py \
  --users-file users.csv \
  --profile PROD \
  --cluster-id 1234-567890-abc123 \
  --debug
```

**Example Output:**
```
Users to be distributed:
  1. john.doe@company.com
  2. jane.smith@company.com
  3. alice.wong@company.com
  ...

Distributing work to cluster workers...
Processing users across workers (showing results as they complete)...

[WORKER BATCH] Executor-0 received 4 user(s): john.doe@company.com, jane.smith@company.com, ...
[WORKER BATCH] Executor-1 received 3 user(s): alice.wong@company.com, bob.jones@company.com, ...

[WORKER START] Executor-0 processing john.doe@company.com - 2025-12-08 14:30:05
[WORKER START] Executor-1 processing alice.wong@company.com - 2025-12-08 14:30:05
[WORKER] Executor-1 - [DBFS] Found 0 files for alice.wong@company.com
[WORKER] Executor-1 - [WORKSPACE] Scanning workspace files for alice.wong@company.com...
[WORKER] Executor-1 - Rate limited (429) on /Users/alice.wong@company.com, retrying in 2s (attempt 1/5)
[WORKER] Executor-1 - [WORKSPACE] Found 15 files for alice.wong@company.com
[WORKER COMPLETE] Executor-1 finished alice.wong@company.com - 2025-12-08 14:30:08 (duration: 3.2s, files: 15, size: 150000)
  [1/100] ✓ [Executor-1] [WORKSPACE] alice.wong@company.com: 15 files (146.5 KB) (3.2s)
      ↳ start=2025-12-08 14:30:05 end=2025-12-08 14:30:08
[WORKER COMPLETE] Executor-0 finished john.doe@company.com - 2025-12-08 14:30:15 (duration: 10.3s, files: 1234, size: 55234567)
  [2/100] ✓ [Executor-0] [BOTH] john.doe@company.com: 1234 files (52.7 MB) (10.3s)
      ↳ start=2025-12-08 14:30:05 end=2025-12-08 14:30:15
  ...

================================================================================
PARALLEL EXECUTION CONFIRMED
================================================================================
✓ Work was distributed across 4 different executors:
  • Executor-0: processed 25 user(s)
  • Executor-1: processed 23 user(s)
  • Executor-2: processed 27 user(s)
  • Executor-3: processed 25 user(s)
================================================================================
```

**Status Icons & Source Tags:**
- ✓ Success: Files found
- ⚠ Empty: Directory exists but no files
- ✗ Error: Scan failed (permission, missing directory, etc.)

**File Source Tags:**
- [DBFS]: Files found only in DBFS (data files)
- [WORKSPACE]: Files found only in Workspace (notebooks)
- [BOTH]: Files found in both DBFS and Workspace (cumulated)

**Retry Messages (when --debug enabled):**
- Rate limited (429): API throttling, automatic retry with backoff
- Server error (500/503): Temporary server issue, automatic retry
- Request error: Network issue, automatic retry

**Debug Output Shows:**
- **User distribution list**: All users to be processed before work starts
- **Batch assignment**: Which users are assigned to which executor/worker
- **Worker identification**: Each message shows the executor ID (e.g., `Executor-0`)
- **File source tracking**: Shows whether files came from DBFS or Workspace (`[DBFS]` or `[WORKSPACE]`)
- **Automatic fallback**: Workers try Workspace API if DBFS is empty
- **Worker-level timing**: When each worker starts/completes processing a user
- **Per-user duration**: How long each user took to scan (e.g., `duration: 10.3s`)
- **Parallel execution**: Multiple executors processing simultaneously
- **Parallel execution summary**: Confirms distribution across executors at the end
- **Real-time progress**: Results stream in as workers finish

**Benefits:**
- See progress as workers complete each user
- Identify errors immediately while processing continues
- Verify parallel execution and cluster efficiency
- Track per-user timing to find bottlenecks
- Estimate completion time with `[N/total]` counter
- Perfect for development, testing, and troubleshooting

See [DEBUG_OUTPUT.md](DEBUG_OUTPUT.md) for full details.

---

## Authentication

Three methods (in priority order):

### 1. Command-Line Arguments (Highest Priority)
```bash
python databricks_user_list.py \
  --workspace-url "https://your-workspace.cloud.databricks.com" \
  --token "your-token"
```

### 2. Databricks CLI Profile (Recommended)
```bash
# First time setup
databricks configure --token --profile PROD
# Enter workspace URL: https://your-workspace.cloud.databricks.com
# Enter token: dapi...

# Use the profile
python databricks_user_list.py --profile PROD --output users.csv
```

**Multiple Profiles:**
```bash
# Configure multiple workspaces
databricks configure --token --profile DEV
databricks configure --token --profile PROD

# Use specific profile
python databricks_user_files_simple.py --users-file users.csv --profile PROD --cluster-id ...
```

**Add Cluster ID to Profile:**
```bash
# Edit ~/.databrickscfg
[PROD]
host = https://your-workspace.cloud.databricks.com
token = dapi...
cluster_id = 1234-567890-abc123

# Now cluster_id is automatic
python databricks_user_files_simple.py --users-file users.csv --profile PROD
```

### 3. Environment Variables
```bash
export DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
export DATABRICKS_CLUSTER_ID="1234-567890-abc123"  # Optional

python databricks_user_list.py --output users.csv
```

### Finding Your Cluster ID

**Option 1: Web UI**
1. Go to **Compute** in your Databricks workspace
2. Click on your cluster
3. Copy the cluster ID from the URL or cluster details

**Option 2: Databricks CLI**
```bash
databricks clusters list
```

---

## Output Formats

### User List (`users.csv`)
Simple text file with one email per line:
```
john.doe@company.com
jane.smith@company.com
alice.wong@company.com
```

### Inventory Results (`results.csv`)
CSV with detailed information:
```csv
username,file_count,total_size,total_size_gb,status,file_source,error
john.doe@company.com,1234,52678912,0.05,success,dbfs,
jane.smith@company.com,15,150000,0.00,success,workspace,
bob.jones@company.com,0,0,0.00,empty,unknown,
alice.wong@company.com,0,0,0.00,error,unknown,User not found
```

**Columns:**
- `username` - User email address
- `file_count` - **Cumulative** count from both DBFS and Workspace
- `total_size` - **Cumulative** size in bytes from both DBFS and Workspace
- `total_size_gb` - Total size in GB (rounded)
- `status` - `success`, `empty`, or `error`
- `file_source` - Which file system(s) contained files: `both`, `dbfs`, `workspace`, or `none`
- `error` - Error message (if status is error)

**Understanding file_source:**
- **`both`** - Files found in both DBFS and Workspace (counts and sizes are cumulated)
- **`dbfs`** - Files only in DBFS (Databricks File System): data files, CSV, Parquet, etc.
- **`workspace`** - Files only in Workspace: notebooks, libraries, Python files
- **`none`** - No files found in either location

The tool automatically scans **both** file systems for each user and adds the results together.

---

## Performance Tips

### 1. Use Parallel Processing

**Sequential (No Cluster):**
```bash
# Processes 1 user at a time
python databricks_user_files_simple.py --users-file users.csv --profile PROD
# Duration: ~50 minutes for 100 users
```

**Parallel (With Cluster):**
```bash
# Distributes across all workers
python databricks_user_files_simple.py --users-file users.csv --profile PROD --cluster-id YOUR_ID
# Duration: ~5 minutes for 100 users (10x faster!)
```

### 2. Choose the Right Cluster

**Optimal Configuration:**
- **Workers:** 4-8 workers (more = faster)
- **Type:** All-Purpose or Job cluster
- **Runtime:** Databricks Runtime 13.0+ recommended
- **Size:** Standard_DS3_v2 or larger

**Not Recommended:**
- Single-node clusters (no parallelization)
- Serverless clusters (limited file access support)

### 3. Process in Batches

For very large workspaces (1000+ users):
```bash
# Split users into batches
head -500 users.csv > batch1.csv
tail -n +501 users.csv > batch2.csv

# Process each batch
python databricks_user_files_simple.py --users-file batch1.csv --profile PROD --cluster-id ... --output results1.csv
python databricks_user_files_simple.py --users-file batch2.csv --profile PROD --cluster-id ... --output results2.csv
```

### 4. Test First

Always test with a small subset:
```bash
# Export first 10 users
python databricks_user_list.py --profile PROD --max-users 10 --output test_users.csv

# Test parallel processing
python databricks_user_files_simple.py --users-file test_users.csv --profile PROD --cluster-id ... --output test_results.csv
```

### 5. Reliability for Large Workspaces

The tool includes automatic error recovery for processing 1000+ users:

**Automatic Retry Logic:**
- API rate limits (429) → Exponential backoff up to 32 seconds
- Server errors (500, 503) → Automatic retry with backoff
- Network errors → Connection retry with exponential backoff
- Adaptive delays → Automatically slows down when under pressure

**Checkpoint & Resume:**
For very long runs that may timeout after 30-60 minutes:
```bash
# Initial run (may timeout after processing 50/100 users)
python databricks_user_files_simple.py --users-file users.csv --profile PROD --cluster-id ABC123 --output results.csv

# Resume from checkpoint if timeout occurs
python databricks_user_files_simple.py --users-file users.csv --profile PROD --cluster-id ABC123 --output results.csv --resume
```

Progress is saved automatically to `.checkpoint_progress.json` after each user completion.

**Best Practices:**
- Use `--debug` flag to monitor retry behavior
- Trust the automatic retry logic - it will recover from transient issues
- Use `--resume` for jobs processing 500+ users to handle potential timeouts

---

## Troubleshooting

### "Authentication failed" or "Token invalid"

**Solution:** Verify your token is still valid
```bash
# Test authentication
databricks workspace ls /

# Re-configure if needed
databricks configure --token --profile PROD
```

### "Connection refused" or "Cluster not reachable"

**Solution:** Verify cluster is running
```bash
# Check cluster status
databricks clusters get --cluster-id YOUR_CLUSTER_ID

# Start cluster if stopped
databricks clusters start --cluster-id YOUR_CLUSTER_ID
```

### "Rate limited (429)" or Server Errors (500, 503)

**Good News:** The tool automatically handles these errors with robust retry logic!

**Automatic Error Handling:**
- **Rate Limiting (429)**: Exponential backoff up to 32 seconds, then retry
- **Server Errors (500, 503)**: Automatic retry with backoff up to 16 seconds
- **Network Errors**: Connection timeout/DNS failure retry with backoff
- **Adaptive Delays**: Request spacing automatically increases from 50ms to 1 second when needed
- **Max Retries**: Up to 5 attempts per request with smart backoff

**Debug Output:**
Enable `--debug` to see retry behavior in action:
```bash
python databricks_user_files_simple.py --users-file users.csv --profile PROD --cluster-id ABC123 --debug
```

Example debug output:
```
Rate limited (429) on /Users/john@example.com, retrying in 4s (attempt 2/5)
Server error 503 on /Users/jane@example.com/data, retrying in 2s
```

**If you still see persistent errors:**
- Reduce concurrent processing (use fewer cluster workers)
- The issue is likely temporary - the tool will automatically recover
- Check Databricks workspace health status

### "Python version mismatch"

**Solution:** Match your local Python version to the cluster
```bash
# Check local Python version
python --version

# Install matching version (example)
pyenv install 3.11.0
pyenv local 3.11.0
```

### Empty results or "No files found"

**Causes:**
1. User has no files in either DBFS or Workspace
2. User directory doesn't exist
3. Permission issues

**Solution:**
- Check the `file_source` column - shows whether DBFS, Workspace, or neither had files
- Check the `error` column for specific error messages
- See [WORKSPACE_VS_DBFS.md](WORKSPACE_VS_DBFS.md) to understand the two file systems

**Important:** The tool automatically checks both:
- **DBFS** (`dbfs:/Users/{username}`) - Data files
- **Workspace** (`/Users/{username}`) - Notebooks and code

Most users have notebooks in Workspace but no data files in DBFS. This is normal!

### "OPERATION_ABANDONED" or timeout errors during parallel processing

**Symptom:**
```
⚠️  PARALLEL PROCESSING INTERRUPTED
Processed: 45/100 users before interruption
Last completed: user@example.com

Error: [INVALID_HANDLE.OPERATION_ABANDONED] The handle ... is invalid.
Operation was considered abandoned because of inactivity and removed.
```

**Cause:** Long-running parallel operations (>30-60 minutes) can timeout due to inactivity limits.

**Solution - Use Resume Flag:**
```bash
# Resume from where it left off
python databricks_user_files_simple.py \
  --users-file users.csv \
  --profile PROD \
  --cluster-id YOUR_CLUSTER_ID \
  --resume
```

**How it works:**
1. During parallel processing, progress is saved incrementally to `.checkpoint_progress.json`
2. Each user completion is checkpointed immediately
3. On timeout/failure, you'll see recovery instructions
4. Use `--resume` flag to skip already-completed users
5. Results from previous run are merged with new results

**Example Recovery:**
```bash
# First run (times out after 45 minutes, processed 50/100 users)
python databricks_user_files_simple.py --users-file users.csv --profile PROD --cluster-id ABC123

# Output shows:
# ⚠️  PARALLEL PROCESSING INTERRUPTED
# Processed: 50/100 users before interruption
# Last completed: user50@example.com

# Resume from checkpoint (processes remaining 50 users)
python databricks_user_files_simple.py --users-file users.csv --profile PROD --cluster-id ABC123 --resume

# Output shows:
# RESUMING FROM CHECKPOINT
# Already completed: 50
# Remaining to process: 50
```

**Tips:**
- Checkpoint file (`.checkpoint_progress.json`) is saved after each user completion
- Safe to delete checkpoint file to start fresh: `rm .checkpoint_progress.json`
- Resume works with same `--users-file` and skips completed users
- Final output merges all results (previous + new)

### Parallel processing falls back to sequential

**Causes:**
1. Spark Connect connection failed
2. Cluster not running
3. Python version mismatch

**Solution:**
- Check cluster status
- Use `--debug` flag to see detailed error messages
- Try sequential mode first to isolate the issue

---

## Advanced Usage

### Custom User Lists

Process specific users:
```bash
# Create custom list
cat > priority_users.csv << EOF
john.doe@company.com
jane.smith@company.com
EOF

# Process only those users
python databricks_user_files_simple.py --users-file priority_users.csv --profile PROD --cluster-id ...
```

### Combining with Other Tools

Use results for further analysis:
```bash
# Export users
python databricks_user_list.py --profile PROD --output users.csv

# Scan workspace
python databricks_user_files_simple.py --users-file users.csv --profile PROD --cluster-id ... --output results.csv

# Analyze with pandas
python << EOF
import pandas as pd
df = pd.read_csv('results.csv')

# Top 10 users by file count
print(df.nlargest(10, 'file_count')[['username', 'file_count', 'total_size_gb']])

# Total workspace usage
print(f"Total files: {df['file_count'].sum():,}")
print(f"Total size: {df['total_size_gb'].sum():.2f} GB")
EOF
```

### Automated Reporting

Schedule regular scans:
```bash
#!/bin/bash
# daily_scan.sh

DATE=$(date +%Y%m%d)
PROFILE="PROD"
CLUSTER_ID="1234-567890-abc123"

# Export users
python databricks_user_list.py --profile $PROFILE --output users_$DATE.csv

# Scan workspace
python databricks_user_files_simple.py \
  --users-file users_$DATE.csv \
  --profile $PROFILE \
  --cluster-id $CLUSTER_ID \
  --output results_$DATE.csv

# Archive
mkdir -p archive
mv users_$DATE.csv results_$DATE.csv archive/
```

### Parallel Processing Details

How it works:
1. **Driver** reads user list and creates DataFrame
2. **Spark distributes** users across all workers
3. **Each worker** independently scans assigned users via DBFS API
4. **Results** are collected and aggregated on driver
5. **Output** written to CSV

**Benefits:**
- Scales with cluster size (more workers = faster)
- Fault-tolerant (failed tasks are retried)
- Progress tracking (see live updates)
- Efficient resource usage

---

## Additional Documentation

- **[WORKSPACE_VS_DBFS.md](WORKSPACE_VS_DBFS.md)** - Understanding the two file systems (DBFS vs Workspace)
- **[README_PARALLEL.md](README_PARALLEL.md)** - Deep dive into parallel processing
- **[DEBUG_OUTPUT.md](DEBUG_OUTPUT.md)** - Understanding debug output and worker logs
- **[API_METHODS.md](API_METHODS.md)** - Technical details on API methods used
- **[PARALLEL_FIX.md](PARALLEL_FIX.md)** - How parallel distribution was fixed
- **[CLAUDE.md](CLAUDE.md)** - Full technical documentation and architecture

---

## Support

### Need Help?

1. **Check logs** - Use `--debug` flag for detailed output
2. **Review documentation** - See additional .md files in this repo
3. **Test incrementally** - Start with 1 user, then 10, then more
4. **Verify authentication** - Test with Databricks CLI first

### Common Issues

Most issues fall into these categories:
- **Authentication** - Token expired or invalid
- **Cluster** - Not running or unreachable
- **Permissions** - Can't access user directories
- **Rate limiting** - Too many API requests (handled automatically)

### Performance Benchmarks

Tested on:
- **Workspace:** 1000 users, ~50GB total
- **Cluster:** 8 workers, Standard_DS3_v2
- **Duration:** 28 minutes (parallel mode)
- **vs Sequential:** ~8 hours (17x speedup)

---

## Quick Reference Card

```bash
# ============================================================
# QUICK REFERENCE: Databricks Workspace Inventory
# ============================================================

# STEP 1: Export Users
# --------------------
python databricks_user_list.py --profile PROD --output users.csv

# STEP 2: Scan Workspace (Parallel - FAST!)
# ------------------------------------------
python databricks_user_files_simple.py \
  --users-file users.csv \
  --profile PROD \
  --cluster-id YOUR_CLUSTER_ID \
  --output results.csv

# AUTHENTICATION
# --------------
databricks configure --token --profile PROD

# FIND CLUSTER ID
# ---------------
databricks clusters list

# TEST WITH SMALL SUBSET
# ----------------------
python databricks_user_list.py --profile PROD --max-users 10 --output test.csv
python databricks_user_files_simple.py --users-file test.csv --profile PROD

# TROUBLESHOOTING
# ---------------
# Add --debug flag to any command for detailed output
python databricks_user_files_simple.py --users-file users.csv --profile PROD --debug

# PERFORMANCE TIPS
# ----------------
# 1. Use parallel mode (requires cluster)
# 2. Use 4-8 worker cluster for best results
# 3. Test with small batches first
# 4. Use --resume flag for 500+ users (handles timeouts)
# 5. Trust automatic retry logic for API errors
# 6. Monitor timing logs with --debug to optimize
```

---

## License

This project is provided as-is for Databricks workspace inventory and auditing purposes.

---

## Contributing

Found a bug or have a feature request? Please open an issue on the repository.

---

**Ready to scan your workspace? Start with the [Quick Start](#-quick-start) guide above!**
