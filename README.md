# Databricks Workspace Inventory

Generate complete file inventories across all user directories in your Databricks workspace. Scan both DBFS and Workspace file systems with parallel processing for maximum performance.

## 🚀 Quick Start

```bash
# 1. Export workspace users
python databricks_user_list.py --profile PROD --output users.csv

# 2. Scan all user directories (parallel mode - 10-100x faster!)
python databricks_user_files_simple.py \
  --users-file users.csv \
  --profile PROD \
  --cluster-id YOUR_CLUSTER_ID \
  --output inventory.csv
```

**Result:** Complete inventory of all files with counts, sizes, and source locations.

---

## Why Use This Tool?

- **Complete Coverage** - Scans both DBFS (data files) and Workspace (notebooks) automatically
- **Blazing Fast** - Parallel processing makes scanning 1000+ users **16x faster** (30 min vs 8 hours)
- **Production Ready** - Handles rate limits, server errors, and timeouts automatically
- **Resume Support** - Long-running jobs checkpoint progress and can resume after interruptions
- **Accurate Results** - Reports exact file sizes from DBFS and estimates for Workspace files

---

## What You Get

### Output: Complete Inventory CSV

```csv
username,file_count,total_size,total_size_gb,status,file_source,error
john.doe@company.com,1234,52678912,0.05,success,both,
jane.smith@company.com,567,24234567,0.02,success,dbfs,
alice.wong@company.com,15,150000,0.00,success,workspace,
bob.jones@company.com,0,0,0.00,empty,none,
```

**Key Columns:**
- `file_count` - Total files across DBFS + Workspace
- `total_size` - Combined size in bytes
- `file_source` - Where files were found: `both`, `dbfs`, `workspace`, or `none`
- `status` - `success`, `empty`, or `error`

---

## Performance

| Users | Sequential | Parallel (8 workers) | Speedup |
|-------|------------|---------------------|---------|
| 10    | 5 min      | 1 min               | 5x      |
| 100   | 50 min     | 5 min               | 10x     |
| 1000  | 8 hours    | 30 min              | 16x     |

---

## Installation

### Requirements
- Python 3.10+
- Databricks workspace access
- Personal Access Token with `users:read` and file read permissions
- Running cluster (for parallel mode)

### Setup

```bash
# Clone repository
git clone https://github.com/LaurentPRAT-DB/dbx_workspace_inventory.git
cd dbx_workspace_inventory

# Install dependencies
pip install -r requirements.txt

# Configure authentication
databricks configure --token --profile PROD
# Or set environment variables: DATABRICKS_WORKSPACE_URL, DATABRICKS_TOKEN
```

---

## Usage

### Step 1: Export User List

```bash
# Export all users
python databricks_user_list.py --profile PROD --output users.csv

# Or limit for testing
python databricks_user_list.py --profile PROD --max-users 10 --output test_users.csv
```

**Options:**
- `--profile NAME` - Use Databricks CLI profile
- `--output FILE` - Save users to CSV file
- `--max-users N` - Limit to first N users (testing)
- `--debug` - Show detailed progress

---

### Step 2: Scan User Files

#### Parallel Mode (Recommended)

**10-100x faster** - distributes work across cluster workers:

```bash
python databricks_user_files_simple.py \
  --users-file users.csv \
  --profile PROD \
  --cluster-id 1234-567890-abc123 \
  --output inventory.csv
```

#### Sequential Mode

Processes one user at a time (no cluster required):

```bash
python databricks_user_files_simple.py \
  --users-file users.csv \
  --profile PROD \
  --output inventory.csv
```

#### Single User Scan

Get detailed output for one user:

```bash
python databricks_user_files_simple.py user@example.com --profile PROD
```

---

## Command-Line Options

```
python databricks_user_files_simple.py [USERS...] [OPTIONS]

Arguments:
  USERS                    Email addresses to process

Options:
  --users-file FILE        File with usernames (one per line)
  --profile NAME           Databricks CLI profile
  --workspace-url URL      Workspace URL (overrides profile)
  --token TOKEN            Access token (overrides profile)
  --cluster-id ID          Cluster ID (enables parallel processing)
  --output FILE            Output CSV file path
  --resume                 Resume from checkpoint (.checkpoint_progress.json)
  --no-parallel            Force sequential processing
  --debug                  Show detailed progress and retry attempts
```

---

## Advanced Features

### Checkpoint & Resume

For large workspaces (500+ users), jobs may timeout after 30-60 minutes. The tool automatically saves progress after each user:

```bash
# Initial run (may timeout after 50 users)
python databricks_user_files_simple.py --users-file users.csv --profile PROD --cluster-id ABC123

# Resume from checkpoint
python databricks_user_files_simple.py --users-file users.csv --profile PROD --cluster-id ABC123 --resume
```

Progress is saved to `.checkpoint_progress.json` and includes all completed results.

### Debug Mode

See real-time progress, retry attempts, and worker distribution:

```bash
python databricks_user_files_simple.py \
  --users-file users.csv \
  --profile PROD \
  --cluster-id ABC123 \
  --debug
```

**Debug output shows:**
- Which executor/worker processes each user
- DBFS vs Workspace scanning progress
- Rate limit (429) and server error (500/503) retries with backoff times
- File counts and sizes as they're discovered
- Processing duration per user

**Example:**
```
[WORKER START] Executor-0 processing john.doe@company.com - 2025-12-09 14:30:05
[WORKER] Executor-0 - [DBFS] Found 1200 files for john.doe@company.com
[WORKER] Executor-0 - [WORKSPACE] Scanning workspace files...
[WORKER] Executor-0 - Rate limited (429), retrying in 2s (attempt 1/5)
[WORKER] Executor-0 - [WORKSPACE] Found 34 files for john.doe@company.com
[WORKER COMPLETE] Executor-0 finished john.doe@company.com (duration: 10.3s, files: 1234)
  ✓ [BOTH] john.doe@company.com: 1234 files (52.7 MB)
```

### Batch Processing

For very large workspaces, split into batches:

```bash
# Split user list
head -500 users.csv > batch1.csv
tail -n +501 users.csv > batch2.csv

# Process batches
python databricks_user_files_simple.py --users-file batch1.csv --profile PROD --cluster-id ... --output results1.csv
python databricks_user_files_simple.py --users-file batch2.csv --profile PROD --cluster-id ... --output results2.csv
```

---

## Error Handling

The tool includes **robust automatic error recovery**:

### Rate Limiting (429)
- **Exponential backoff** up to 32 seconds
- **Adaptive delays** increase from 50ms to 1s when under pressure
- **Automatic retry** up to 5 attempts

### Server Errors (500, 503)
- **Automatic retry** with backoff up to 16 seconds
- **Temporary failures** are recovered automatically

### Network Errors
- **Connection timeouts** and DNS failures retry automatically
- **Exponential backoff** with max 5 attempts

### Example (--debug mode):
```
Rate limited (429) on /Users/john@example.com, retrying in 4s (attempt 2/5)
Server error 503 on /Users/jane@example.com/data, retrying in 2s
```

**You don't need to do anything** - the tool handles all retries automatically.

---

## Cluster Configuration

### Recommended Setup

For optimal parallel performance:

```
Cluster Type: All-Purpose or Job cluster
Workers: 4-8 (more workers = faster processing)
Runtime: DBR 13.0+ recommended
Node Type: Standard_DS3_v2 or larger
Access Mode: Single User or Shared (not Isolated)
```

### Not Recommended
- Single-node clusters (no parallelization)
- Serverless clusters (limited file access)

---

## Authentication

### Method 1: Databricks CLI (Recommended)

```bash
# Configure profile
databricks configure --token --profile PROD

# Use profile
python databricks_user_files_simple.py --users-file users.csv --profile PROD --cluster-id ...

# List available profiles
databricks configure --list-profiles
```

### Method 2: Environment Variables

```bash
export DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi1234567890abcdef"

python databricks_user_files_simple.py --users-file users.csv --cluster-id ...
```

### Method 3: Command-Line Arguments

```bash
python databricks_user_files_simple.py \
  --users-file users.csv \
  --workspace-url "https://your-workspace.cloud.databricks.com" \
  --token "dapi1234567890abcdef" \
  --cluster-id ...
```

---

## Understanding File Sources

Databricks has **two separate file systems**:

### DBFS (Databricks File System)
- **Location:** `dbfs:/Users/{username}/`
- **UI:** Data → DBFS → Users
- **Contains:** Data files (CSV, Parquet, JSON, etc.)
- **API:** `/api/2.0/dbfs/list`
- **Sizes:** Exact file sizes

### Workspace File System
- **Location:** `/Users/{username}/`
- **UI:** Workspace → Users
- **Contains:** Notebooks, libraries, Python files
- **API:** `/api/2.0/workspace/list`
- **Sizes:** Estimated at 10KB per file

### How It Works

The tool **always scans both** file systems for every user and **cumulates the results**:

```
User: john@example.com
  DBFS files: 1200 (50 MB)
  Workspace files: 34 (340 KB)
  Total reported: 1234 files (50.34 MB) - file_source: both
```

See [WORKSPACE_VS_DBFS.md](WORKSPACE_VS_DBFS.md) for detailed explanation.

---

## Troubleshooting

### "Authentication failed" or "Token invalid"

```bash
# Test authentication
databricks workspace ls /

# Reconfigure
databricks configure --token --profile PROD
```

### "Cluster not reachable"

```bash
# Check cluster status
databricks clusters get --cluster-id YOUR_CLUSTER_ID

# Start cluster
databricks clusters start --cluster-id YOUR_CLUSTER_ID
```

### Parallel processing not working

**Symptoms:** Falls back to sequential mode

**Solutions:**
1. Verify cluster is running and accessible
2. Check Python version matches cluster (use `--debug` to see mismatch warnings)
3. Ensure cluster has 2+ workers (not single-node)
4. Try sequential mode first to isolate the issue

### "OPERATION_ABANDONED" timeout errors

**What it means:** Long-running parallel jobs (>30-60 min) can timeout due to inactivity limits.

**Solution:** Use the `--resume` flag:

```bash
# Job timed out after processing 50/100 users
python databricks_user_files_simple.py --users-file users.csv --profile PROD --cluster-id ABC123 --resume
```

The tool automatically checkpoints progress. Resume will skip completed users and continue from where it stopped.

### Empty results or "No files found"

**This is normal!** Users may have:
- Files only in DBFS → `file_source: dbfs`
- Files only in Workspace → `file_source: workspace`
- Files in both → `file_source: both`
- No files anywhere → `file_source: none`, `status: empty`

Check the `file_source` column to see which file system(s) had files.

---

## Best Practices

1. **Test with small batches first**
   ```bash
   python databricks_user_list.py --profile PROD --max-users 10 --output test.csv
   python databricks_user_files_simple.py --users-file test.csv --profile PROD --cluster-id ...
   ```

2. **Use parallel mode for 10+ users**
   ```bash
   # 10-100x faster with cluster
   python databricks_user_files_simple.py --users-file users.csv --profile PROD --cluster-id ...
   ```

3. **Enable debug mode for first run**
   ```bash
   python databricks_user_files_simple.py --users-file users.csv --profile PROD --cluster-id ... --debug
   ```

4. **Use resume for 500+ users**
   ```bash
   # Long-running jobs may timeout - resume is your friend
   python databricks_user_files_simple.py --users-file users.csv --profile PROD --cluster-id ... --resume
   ```

5. **Trust the automatic retry logic**
   - Rate limits (429) are handled automatically
   - Server errors (500/503) retry automatically
   - Network errors retry automatically
   - No manual intervention needed

---

## Output Formats

The tool generates CSV output with the following schema:

```csv
username,file_count,total_size,total_size_gb,status,file_source,error
```

**Columns:**
- `username` - User email address
- `file_count` - Cumulative count from DBFS + Workspace
- `total_size` - Cumulative size in bytes
- `total_size_gb` - Total size in GB (rounded to 2 decimals)
- `status` - `success`, `empty`, or `error`
- `file_source` - `both`, `dbfs`, `workspace`, or `none`
- `error` - Error message if status is error

---

## Additional Documentation

- [WORKSPACE_VS_DBFS.md](WORKSPACE_VS_DBFS.md) - Deep dive into the two file systems
- [NOTEBOOK_USAGE.md](NOTEBOOK_USAGE.md) - Running as Databricks notebook (alternative to Python script)
- [CLAUDE.md](CLAUDE.md) - Project architecture and development notes

---

## Quick Reference

```bash
# Export users
python databricks_user_list.py --profile PROD --output users.csv

# Parallel scan (FAST)
python databricks_user_files_simple.py --users-file users.csv --profile PROD --cluster-id ABC123 --output results.csv

# Sequential scan (no cluster)
python databricks_user_files_simple.py --users-file users.csv --profile PROD --output results.csv

# Resume after timeout
python databricks_user_files_simple.py --users-file users.csv --profile PROD --cluster-id ABC123 --resume

# Debug mode
python databricks_user_files_simple.py --users-file users.csv --profile PROD --cluster-id ABC123 --debug

# Single user
python databricks_user_files_simple.py user@example.com --profile PROD
```

---

## License

This project is provided as-is for Databricks workspace inventory and auditing purposes.

---

## Contributing

Found a bug or have a feature request? Please open an issue on the [GitHub repository](https://github.com/LaurentPRAT-DB/dbx_workspace_inventory).

---

**Ready to inventory your workspace?** Start with the [Quick Start](#-quick-start) guide above! 🚀
