# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **Databricks Workspace Inventory Application** that generates complete file inventories across all user directories in a Databricks workspace. It scans both DBFS and Workspace file systems, with parallel processing support for optimal performance on large workspaces.

### Key Architecture Components

- **Main Application**: `databricks_user_files_simple.py` - Primary script for parallel/sequential user file scanning
- **User Export**: `databricks_user_list.py` - Exports user lists via SCIM API
- **Execution Modes**:
  - Parallel mode using Spark Connect (10-100x faster)
  - Sequential mode for local/single-threaded processing
  - Checkpoint/resume support for long-running jobs
- **Authentication**: Supports Databricks CLI profiles (`~/.databrickscfg`) with fallback to environment variables
- **Dual File System Scanning**: Automatically scans both DBFS (`/dbfs/Users/`) and Workspace (`/Workspace/Users/`) for complete coverage

### Core Functions

- `scan_user_directory()` - Main function that scans both DBFS and Workspace for a single user
- `scan_dbfs()` - Recursively scans DBFS directories with retry logic for rate limits
- `scan_workspace()` - Recursively scans Workspace directories with retry logic
- `process_user_batch_parallel()` - Distributes user scanning across Spark workers
- `load_databricks_config()` - Authentication via CLI profiles with environment variable fallback

## Common Development Commands

### Running the Application

**Step 1: Export user list**
```bash
# Export all users from workspace
python databricks_user_list.py --profile PROD --output users.csv

# Or limit for testing
python databricks_user_list.py --profile PROD --max-users 10 --output test_users.csv
```

**Step 2: Scan user directories**

**Parallel mode (recommended for 10+ users):**
```bash
# Using Databricks CLI profile
python databricks_user_files_simple.py \
  --users-file users.csv \
  --profile PROD \
  --cluster-id YOUR_CLUSTER_ID \
  --output inventory.csv

# With debug output
python databricks_user_files_simple.py \
  --users-file users.csv \
  --profile PROD \
  --cluster-id YOUR_CLUSTER_ID \
  --output inventory.csv \
  --debug
```

**Sequential mode (no cluster required):**
```bash
python databricks_user_files_simple.py \
  --users-file users.csv \
  --profile PROD \
  --output inventory.csv
```

**Single user scan:**
```bash
python databricks_user_files_simple.py user@example.com --profile PROD
```

**Resume after timeout:**
```bash
python databricks_user_files_simple.py \
  --users-file users.csv \
  --profile PROD \
  --cluster-id YOUR_CLUSTER_ID \
  --resume
```

### Development & Testing Commands

**Install dependencies:**
```bash
pip install -r requirements.txt
```

**Check dependencies:**
```bash
# Check if requests is available
python -c "import requests; print('Requests OK')"

# Check if PySpark is available (for parallel mode)
python -c "import pyspark; print('PySpark OK')"
```

**Profile management:**
```bash
# List available Databricks CLI profiles
databricks configure --list-profiles

# Configure new profile
databricks configure --token --profile PROD
```

**Test with small batches:**
```bash
# Export limited user list for testing
python databricks_user_list.py --profile PROD --max-users 5 --output test.csv

# Test scan with debug output
python databricks_user_files_simple.py --users-file test.csv --profile PROD --debug
```

### Output Format

The application generates CSV output with the following columns:
```csv
username,file_count,total_size,total_size_gb,status,file_source,error
```

**file_source values:**
- `both` - Files found in both DBFS and Workspace
- `dbfs` - Files only in DBFS
- `workspace` - Files only in Workspace
- `none` - No files found anywhere

## Architecture Notes for Development

### Processing Modes

**Parallel Mode:**
- Triggered when `--cluster-id` is provided
- Uses Spark Connect to distribute user scanning across cluster workers
- 10-100x faster than sequential mode for large user counts
- Requires a running Databricks cluster with 2+ workers
- Detects Python version mismatch between client and cluster

**Sequential Mode:**
- Default when no cluster ID is provided
- Processes users one at a time on the local machine
- Suitable for small user counts (< 10 users) or testing
- No cluster required

### Dual File System Architecture

The application scans **two separate file systems** for each user:

1. **DBFS (Databricks File System)**
   - Location: `dbfs:/Users/{username}/`
   - API: `/api/2.0/dbfs/list`
   - Contains: Data files (CSV, Parquet, JSON, etc.)
   - Provides exact file sizes

2. **Workspace File System**
   - Location: `/Users/{username}/`
   - API: `/api/2.0/workspace/list`
   - Contains: Notebooks, libraries, Python files
   - File sizes estimated at 10KB per file

Results are cumulated across both file systems for each user.

### Authentication Priority (Highest to Lowest)
1. Command-line arguments (`--workspace-url`, `--token`)
2. Databricks CLI profile from `~/.databrickscfg`
3. Environment variables (`DATABRICKS_WORKSPACE_URL`, `DATABRICKS_TOKEN`)

### Retry & Error Handling

The application includes comprehensive automatic retry logic:
- **Rate limiting (429)**: Exponential backoff up to 32 seconds, 5 attempts
- **Server errors (500/503)**: Exponential backoff up to 16 seconds, 5 attempts
- **Network errors**: Automatic retry with exponential backoff
- **Adaptive delays**: Increases from 50ms to 1s when rate limited

## Key Dependencies

- **requests>=2.28.0** - REST API calls (SCIM, DBFS, Workspace APIs)
- **pyspark>=3.5.0** - Optional, required only for parallel mode (Spark Connect)
- **databricks-cli** - Optional, for easier authentication management (`~/.databrickscfg`)

## Key Files

- **databricks_user_files_simple.py** - Main application for scanning user directories
- **databricks_user_list.py** - User list export via SCIM API
- **requirements.txt** - Python dependencies
- **.gitignore** - Excludes deprecated files and sensitive data
- **README.md** - Complete user documentation
- **WORKSPACE_VS_DBFS.md** - Deep dive into dual file system architecture
- **NOTEBOOK_USAGE.md** - Alternative notebook-based execution

## Development Tips

- **Test with small batches first**: Use `--max-users 5` when exporting users for initial testing
- **Use debug mode**: `--debug` flag shows real-time progress, worker distribution, and retry attempts
- **Trust automatic retries**: Rate limits (429) and server errors (500/503) are handled automatically
- **Use resume for large workspaces**: `--resume` flag continues from checkpoint after timeouts
- **Check file_source column**: Shows which file system(s) contained files for each user
- **Parallel mode requires cluster**: Ensure cluster has 2+ workers and is running
- **Test with different profiles**: Use `--profile PROFILE_NAME` to test different workspace configurations

## Checkpoint & Resume System

For large workspaces (500+ users), long-running parallel jobs may timeout after 30-60 minutes:

- Progress is automatically saved to `.checkpoint_progress.json` after each user
- Use `--resume` flag to continue from the last checkpoint
- Checkpoint includes all completed user results
- The checkpoint file is ignored by git (see .gitignore)

## Common Issues & Solutions

### "Python version mismatch" warning
The client and cluster have different Python versions. This may cause UDF failures in parallel mode.
- **Solution**: Use a cluster with matching Python version, or use sequential mode

### "Parallel processing not available" warning
Cluster is not reachable or PySpark is not installed.
- **Solution**: Verify cluster is running, or use sequential mode with `--no-parallel`

### "OPERATION_ABANDONED" timeout
Long-running parallel job timed out due to inactivity limits.
- **Solution**: Use `--resume` to continue from checkpoint

### Rate limiting (429 errors)
Application is being rate limited by Databricks APIs.
- **Solution**: Nothing needed - automatic exponential backoff handles this