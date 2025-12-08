# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks User Files Inventory - Parallel Processing
# MAGIC
# MAGIC This notebook scans user home directories in parallel across cluster workers using DBFS API.
# MAGIC
# MAGIC **Features:**
# MAGIC - Parallel processing using cluster workers
# MAGIC - DBFS API method (no /dbfs mount required)
# MAGIC - Real-time progress with worker identification
# MAGIC - Reads user list from file (DBFS or workspace)
# MAGIC - Exports results to CSV
# MAGIC - Automatic authentication from notebook context
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Cluster with multiple workers (for parallelism)
# MAGIC - User list file in DBFS (e.g., /dbfs/tmp/users.txt)
# MAGIC - Notebook attached to a running cluster

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Set these parameters before running:

# COMMAND ----------

# Configuration
USERS_FILE = "/dbfs/tmp/users.txt"  # Path to file with user emails (one per line)
OUTPUT_FILE = "/dbfs/tmp/results.csv"  # Output CSV file path
DEBUG_MODE = True  # Enable detailed debug output

# COMMAND ----------

# MAGIC %md
# MAGIC ## Authentication Setup (Automatic)
# MAGIC
# MAGIC Authentication is obtained automatically from the Databricks notebook context.
# MAGIC No need to configure secrets or provide tokens manually.

# COMMAND ----------

# Get authentication from Databricks notebook context
# These are automatically available when running in a Databricks notebook
WORKSPACE_URL = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

print("="*80)
print("AUTHENTICATION")
print("="*80)
print(f"✓ Workspace URL: {WORKSPACE_URL}")
print(f"✓ Authentication token obtained from notebook context")
print("="*80)
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

from datetime import datetime
from typing import Dict, List
import json
import time

def format_size(size_bytes: int) -> str:
    """Format byte size in human readable format."""
    if size_bytes == 0:
        return "0 B"

    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"

def process_user_on_worker(user_data: str) -> Dict:
    """
    Process a single user on a Spark worker.
    This function runs on cluster workers for parallel processing.

    Args:
        user_data: JSON string containing user info and credentials

    Returns:
        Dictionary with user processing results
    """
    import json
    import time
    from datetime import datetime

    try:
        data = json.loads(user_data)
        username = data["username"]
        workspace_url = data["workspace_url"]
        token = data["token"]
        debug = data.get("debug", False)

        # Get worker/executor information
        worker_info = "Unknown"
        try:
            from pyspark import TaskContext
            task_context = TaskContext.get()
            if task_context:
                worker_info = f"Executor-{task_context.partitionId()}"
        except:
            pass

        # Debug: Print start time on worker
        start_time = datetime.now()
        if debug:
            print(f"[WORKER START] {worker_info} processing {username} - {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Import requests on the worker
        import requests

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        home_path = f"/Users/{username}"
        file_count = 0
        total_size = 0
        dir_count = 0
        request_count = 0
        rate_limit_delay = 0.05

        def list_recursive(path: str, depth: int = 0, max_depth: int = 10):
            nonlocal file_count, total_size, dir_count, request_count, rate_limit_delay

            if depth > max_depth:
                return

            max_retries = 5
            retry_count = 0

            while retry_count < max_retries:
                try:
                    if request_count > 0:
                        time.sleep(rate_limit_delay)

                    request_count += 1

                    url = f"{workspace_url}/api/2.0/dbfs/list"
                    response = requests.get(
                        url,
                        headers=headers,
                        json={"path": path},
                        timeout=30
                    )

                    if response.status_code == 404:
                        return
                    elif response.status_code == 429:
                        retry_count += 1
                        wait_time = min(2 ** retry_count, 32)
                        time.sleep(wait_time)
                        rate_limit_delay = min(rate_limit_delay * 1.5, 1.0)
                        continue
                    elif response.status_code != 200:
                        return

                    data = response.json()
                    files = data.get("files", [])

                    for file_info in files:
                        file_path = file_info.get("path", "")
                        is_dir = file_info.get("is_dir", False)
                        file_size = file_info.get("file_size", 0)

                        if is_dir:
                            dir_count += 1
                            list_recursive(file_path, depth + 1, max_depth)
                        else:
                            file_count += 1
                            total_size += file_size

                    return

                except Exception:
                    retry_count += 1
                    if retry_count >= max_retries:
                        return
                    time.sleep(min(2 ** retry_count, 16))

        # Process the user
        list_recursive(home_path)

        # Debug: Print completion time on worker
        end_time = datetime.now()
        duration = end_time - start_time
        duration_seconds = duration.total_seconds()
        if debug:
            print(f"[WORKER COMPLETE] {worker_info} finished {username} - {end_time.strftime('%Y-%m-%d %H:%M:%S')} "
                  f"(duration: {duration_seconds:.1f}s, files: {file_count}, size: {total_size})")

        return {
            "username": username,
            "file_count": file_count,
            "total_size": total_size,
            "dir_count": dir_count,
            "status": "success" if file_count > 0 else "empty",
            "error": None
        }

    except Exception as e:
        # Debug: Print error completion time on worker
        if 'start_time' in locals() and 'data' in locals() and data.get("debug", False):
            end_time = datetime.now()
            duration = end_time - start_time
            duration_seconds = duration.total_seconds()
            username_str = data.get("username", "unknown")
            worker_info_str = worker_info if 'worker_info' in locals() else "Unknown"
            print(f"[WORKER ERROR] {worker_info_str} failed {username_str} - {end_time.strftime('%Y-%m-%d %H:%M:%S')} "
                  f"(duration: {duration_seconds:.1f}s, error: {str(e)})")

        return {
            "username": data.get("username", "unknown") if 'data' in locals() else "unknown",
            "file_count": 0,
            "total_size": 0,
            "dir_count": 0,
            "status": "error",
            "error": str(e)
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read User List

# COMMAND ----------

print("="*80)
print("READING USER LIST")
print("="*80)
print(f"Reading from: {USERS_FILE}\n")

# Read users from file
usernames = []
try:
    with open(USERS_FILE, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                usernames.append(line)

    print(f"✓ Loaded {len(usernames)} user(s) from file\n")

    if DEBUG_MODE:
        print("Users to be processed:")
        for idx, username in enumerate(usernames, 1):
            print(f"  {idx}. {username}")
        print()

except Exception as e:
    print(f"✗ Error reading user list: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cluster Information

# COMMAND ----------

if DEBUG_MODE:
    print("="*80)
    print("CLUSTER INFORMATION")
    print("="*80)

    # Use native Spark session provided by Databricks notebook
    # Use spark.conf.get for shared cluster compatibility
    default_parallelism = int(spark.conf.get('spark.default.parallelism', '8'))
    print(f"Default parallelism: {default_parallelism} concurrent tasks")
    print(f"Maximum concurrent users: ~{default_parallelism}")

    # Try to get executor info
    try:
        num_executors = len(spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()) - 1  # Exclude driver
        if num_executors > 0:
            print(f"Active executors: {num_executors}")
            cores_per_executor = default_parallelism // num_executors if num_executors > 0 else default_parallelism
            print(f"Estimated cores per executor: ~{cores_per_executor}")
    except:
        print("Could not retrieve executor count")

    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare for Parallel Processing

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType

# Prepare user data for workers
user_data_list = []
for username in usernames:
    user_data = {
        "username": username,
        "workspace_url": WORKSPACE_URL,
        "token": TOKEN,
        "debug": DEBUG_MODE
    }
    user_data_list.append(json.dumps(user_data))

# Create DataFrame
users_df = spark.createDataFrame([{"user_data": ud} for ud in user_data_list])

# Repartition for parallel processing
num_partitions = min(len(user_data_list), 200)
users_df = users_df.repartition(num_partitions)

if DEBUG_MODE:
    print("="*80)
    print("PARALLEL PROCESSING SETUP")
    print("="*80)
    print(f"Total users: {len(usernames)}")
    print(f"Partitions created: {num_partitions}")
    print(f"Average users per partition: ~{len(usernames) / num_partitions:.1f}")

    # Use num_partitions directly (avoid RDD API for DatabricksConnect compatibility)
    print(f"Actual partitions: {num_partitions}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Parallel Processing

# COMMAND ----------

# Define output schema
output_schema = StructType([
    StructField("username", StringType(), True),
    StructField("file_count", LongType(), True),
    StructField("total_size", LongType(), True),
    StructField("dir_count", LongType(), True),
    StructField("status", StringType(), True),
    StructField("error", StringType(), True)
])

# Process in parallel using mapInPandas
def process_users_batch(iterator):
    import pandas as pd
    import json

    # Get worker/executor information
    task_context = None
    try:
        from pyspark import TaskContext
        task_context = TaskContext.get()
    except:
        pass

    for pdf in iterator:
        rows = []
        batch_users = []

        # First, collect usernames in this batch for logging
        for user_data_str in pdf['user_data']:
            try:
                data = json.loads(user_data_str)
                username = data.get("username", "unknown")
                batch_users.append(username)
            except:
                pass

        # Log batch assignment if debug mode
        if batch_users:
            debug_mode = False
            try:
                first_data = json.loads(pdf['user_data'].iloc[0])
                debug_mode = first_data.get("debug", False)
            except:
                pass

            if debug_mode:
                worker_id = f"Executor-{task_context.partitionId()}" if task_context else "Unknown"
                print(f"[WORKER BATCH] {worker_id} received {len(batch_users)} user(s): {', '.join(batch_users)}")

        # Process each user in this batch
        for user_data_str in pdf['user_data']:
            result = process_user_on_worker(user_data_str)
            rows.append(result)

        if rows:
            yield pd.DataFrame(rows)
        else:
            yield pd.DataFrame(columns=["username", "file_count", "total_size", "dir_count", "status", "error"])

# Start timing
parallel_start_time = datetime.now()

print("="*80)
print(f"STARTING PARALLEL EXECUTION - {parallel_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)
print(f"Processing {len(usernames)} users across cluster workers...")
print(f"Method: DBFS API via REST calls (no /dbfs mount required)")
print("="*80)
print()

# Execute parallel processing
result_df = users_df.mapInPandas(process_users_batch, schema=output_schema)

# Collect results
if DEBUG_MODE:
    print("Processing users across workers (showing results as they complete)...\n")
    results = []
    processed_count = 0

    # Use toLocalIterator for incremental result streaming
    for row in result_df.toLocalIterator():
        processed_count += 1
        result = {
            "username": row.username,
            "file_count": int(row.file_count or 0),
            "total_size": int(row.total_size or 0),
            "dir_count": int(row.dir_count or 0),
            "status": row.status,
            "error": row.error
        }
        results.append(result)

        # Show per-user progress
        size_str = format_size(result["total_size"])
        status_icon = "✓" if result["status"] == "success" else ("⚠" if result["status"] == "empty" else "✗")
        error_msg = f" - {result['error']}" if result['error'] else ""
        print(f"  [{processed_count}/{len(usernames)}] {status_icon} {result['username']}: "
              f"{result['file_count']} files ({size_str}){error_msg}")

    print()
else:
    print("Collecting results from workers...\n")
    results_rows = result_df.collect()

    # Convert to list of dicts
    results = []
    for row in results_rows:
        results.append({
            "username": row.username,
            "file_count": int(row.file_count or 0),
            "total_size": int(row.total_size or 0),
            "dir_count": int(row.dir_count or 0),
            "status": row.status,
            "error": row.error
        })

# Calculate duration
parallel_end_time = datetime.now()
parallel_duration = parallel_end_time - parallel_start_time

total_seconds = int(parallel_duration.total_seconds())
hours, remainder = divmod(total_seconds, 3600)
minutes, seconds = divmod(remainder, 60)

if hours > 0:
    duration_str = f"{hours}h {minutes}m {seconds}s"
elif minutes > 0:
    duration_str = f"{minutes}m {seconds}s"
else:
    duration_str = f"{seconds}s"

print(f"Parallel processing completed in {duration_str}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Summary

# COMMAND ----------

print("="*80)
print("SUMMARY")
print("="*80)

successful = [r for r in results if r["status"] == "success"]
empty = [r for r in results if r["status"] == "empty"]
errors = [r for r in results if r["status"] == "error"]

total_files = sum(r["file_count"] for r in results)
total_size_all = sum(r["total_size"] for r in results)

print(f"Total users processed: {len(results)}")
print(f"  ✓ Successful (with files): {len(successful)}")
print(f"  ⊘ Empty directories: {len(empty)}")
print(f"  ✗ Errors: {len(errors)}")
print()
print(f"Total files across all users: {total_files:,}")
print(f"Total size across all users: {format_size(total_size_all)} ({total_size_all:,} bytes)")
print("="*80)
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Results

# COMMAND ----------

import csv

print("="*80)
print("EXPORTING RESULTS")
print("="*80)
print(f"Output file: {OUTPUT_FILE}\n")

try:
    with open(OUTPUT_FILE, 'w', newline='') as csvfile:
        fieldnames = ['username', 'file_count', 'total_size', 'total_size_gb', 'status', 'error']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for r in results:
            writer.writerow({
                'username': r['username'],
                'file_count': r['file_count'],
                'total_size': r['total_size'],
                'total_size_gb': round(r['total_size'] / (1024**3), 2),
                'status': r['status'],
                'error': r['error'] or ''
            })

    print(f"✓ Results saved to: {OUTPUT_FILE}")
    print(f"✓ {len(results)} rows written")
    print()

except Exception as e:
    print(f"✗ Error writing CSV: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Results (Preview)

# COMMAND ----------

# Convert to Spark DataFrame for display
results_display_df = spark.createDataFrame(results)
display(results_display_df.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completion
# MAGIC
# MAGIC Processing complete! Results have been saved to the output file and displayed above.
# MAGIC
# MAGIC **Next Steps:**
# MAGIC - Download the CSV file from DBFS
# MAGIC - Analyze results for storage usage patterns
# MAGIC - Identify users with large file counts
# MAGIC - Review any errors for troubleshooting
