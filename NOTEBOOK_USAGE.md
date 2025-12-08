# Databricks Notebook - Parallel User Files Processing

This notebook version runs **directly on a Databricks cluster**, avoiding Spark Connect issues while maintaining full parallel processing capabilities.

## Key Differences from Python Script

| Feature | Python Script | Notebook |
|---------|--------------|----------|
| Execution | Local machine via Spark Connect | Directly on Databricks cluster |
| RDD API | Not available (Spark Connect) | ✅ Fully available |
| Connection | Remote via `sc://` URL | ✅ Native Spark session |
| File access | Local file system | DBFS or workspace files |
| Performance | Network overhead | ✅ No network overhead |
| Use case | Local development/testing | Production workloads |

## Setup Instructions

### 1. Upload the Notebook

**Option A: Import from file**
```
1. Go to Databricks Workspace
2. Click "Workspace" → "Users" → Your folder
3. Right-click → "Import"
4. Upload: databricks_user_files_parallel_notebook.py
```

**Option B: Create new notebook and paste**
```
1. Create new Python notebook
2. Copy contents of databricks_user_files_parallel_notebook.py
3. Paste into notebook
4. Run cells
```

### 2. Prepare User List File

Create a file with user emails (one per line):

```
john.doe@company.com
jane.smith@company.com
alice.wong@company.com
```

**Option A: Upload to DBFS**
```bash
# From local machine
databricks fs cp users.txt dbfs:/tmp/users.txt --profile PROD

# Or use Databricks UI
# Data → Add Data → Upload File → dbfs:/tmp/users.txt
```

**Option B: Create in notebook**
```python
# Run this in a notebook cell
dbutils.fs.put("/tmp/users.txt", """
john.doe@company.com
jane.smith@company.com
alice.wong@company.com
""", overwrite=True)
```

### 3. Configure Parameters

Edit the configuration cell:

```python
# Configuration
USERS_FILE = "/dbfs/tmp/users.txt"  # Path to your user list
OUTPUT_FILE = "/dbfs/tmp/results.csv"  # Where to save results
DEBUG_MODE = True  # Enable detailed output
```

**Note:** Authentication is automatic! The notebook obtains credentials from the Databricks context - no need to configure tokens or secrets.

### 4. Attach to Cluster

**Cluster Requirements:**
- **Workers**: 2+ for parallelism (4-8 recommended)
- **Runtime**: DBR 13.0+ recommended
- **Access Mode**: Single user or Shared (not Isolated)
- **Type**: All-purpose or Job cluster

**Cluster Configuration Example:**
```
Cluster Mode: Standard
Workers: 4-8 (for good parallelism)
Runtime: 13.3 LTS
Node Type: Standard_DS3_v2 (or similar)
```

## Running the Notebook

### Step-by-Step Execution

1. **Attach to cluster**: Select your cluster from dropdown
2. **Run Configuration cell**: Set USERS_FILE, OUTPUT_FILE
3. **Run All**: Click "Run All" or execute cells sequentially

### What You'll See

**With DEBUG_MODE = True:**
```
================================================================================
AUTHENTICATION
================================================================================
✓ Workspace URL: https://your-workspace.cloud.databricks.com
✓ Authentication token obtained from notebook context
================================================================================

================================================================================
READING USER LIST
================================================================================
Reading from: /dbfs/tmp/users.txt

✓ Loaded 10 user(s) from file

Users to be processed:
  1. john.doe@company.com
  2. jane.smith@company.com
  ...

================================================================================
CLUSTER INFORMATION
================================================================================
Default parallelism: 32 concurrent tasks
Maximum concurrent users: ~32
Active executors: 8
Estimated cores per executor: ~4

================================================================================
PARALLEL PROCESSING SETUP
================================================================================
Total users: 10
Partitions created: 10
Average users per partition: ~1.0
Actual partitions: 10

================================================================================
STARTING PARALLEL EXECUTION - 2025-12-08 14:30:00
================================================================================
Processing 10 users across cluster workers...
Method: DBFS API via REST calls (no /dbfs mount required)
================================================================================

[WORKER BATCH] Executor-0 received 2 user(s): john.doe@company.com, jane.smith@company.com
[WORKER BATCH] Executor-1 received 2 user(s): alice.wong@company.com, bob.jones@company.com
[WORKER BATCH] Executor-2 received 2 user(s): carol.white@company.com, dave.miller@company.com
...

[WORKER START] Executor-0 processing john.doe@company.com - 2025-12-08 14:30:05
[WORKER START] Executor-1 processing alice.wong@company.com - 2025-12-08 14:30:05
[WORKER START] Executor-2 processing carol.white@company.com - 2025-12-08 14:30:05
[WORKER COMPLETE] Executor-1 finished alice.wong@company.com - 2025-12-08 14:30:15 (duration: 10.3s, files: 1234, size: 55234567)
  [1/10] ✓ alice.wong@company.com: 1234 files (52.7 MB)
...

Parallel processing completed in 2m 15s

================================================================================
SUMMARY
================================================================================
Total users processed: 10
  ✓ Successful (with files): 8
  ⊘ Empty directories: 1
  ✗ Errors: 1

Total files across all users: 12,345
Total size across all users: 1.5 GB (1,578,912,345 bytes)
================================================================================

✓ Results saved to: /dbfs/tmp/results.csv
✓ 10 rows written
```

## Output Files

### CSV Format

```csv
username,file_count,total_size,total_size_gb,status,error
john.doe@company.com,1234,52678912,0.05,success,
jane.smith@company.com,567,24234567,0.02,success,
alice.wong@company.com,0,0,0.00,empty,
bob.jones@company.com,0,0,0.00,error,User directory does not exist
```

### Download Results

**Option A: Download via UI**
```
1. Data → DBFS → tmp → results.csv
2. Click download icon
```

**Option B: Download via CLI**
```bash
databricks fs cp dbfs:/tmp/results.csv ./results.csv --profile PROD
```

**Option C: View in notebook**
```python
# Run this in a new cell
import pandas as pd
results_df = pd.read_csv("/dbfs/tmp/results.csv")
display(results_df)
```

## Advantages of Notebook Version

### ✅ No Spark Connect Issues
- Native Spark session (not remote)
- Full RDD API access
- No network connection required
- Better performance

### ✅ Full Databricks Integration
- Automatic authentication from notebook context
- Access to dbutils
- Display() for visualization
- Native file access
- No need to manage tokens or secrets

### ✅ Interactive Debugging
- Run cells individually
- Inspect intermediate results
- Modify parameters on the fly
- See real-time output

### ✅ Scheduling Support
- Can be scheduled as a Job
- Parameterized execution
- Email notifications
- Retry logic

## Scheduling as a Job

### Create Job via UI

```
1. Jobs → Create Job
2. Task type: Notebook
3. Select: databricks_user_files_parallel_notebook
4. Cluster: Select or create
5. Parameters (optional):
   - users_file: dbfs:/tmp/users.txt
   - output_file: dbfs:/tmp/results.csv
6. Schedule: Daily, weekly, etc.
```

### Create Job via CLI

```bash
databricks jobs create --json '{
  "name": "User Files Inventory",
  "tasks": [{
    "task_key": "inventory_task",
    "notebook_task": {
      "notebook_path": "/Users/your-email/databricks_user_files_parallel_notebook",
      "source": "WORKSPACE"
    },
    "new_cluster": {
      "spark_version": "13.3.x-scala2.12",
      "node_type_id": "Standard_DS3_v2",
      "num_workers": 8
    }
  }],
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "UTC"
  }
}'
```

## Performance Tips

### Cluster Sizing

| Users | Recommended Workers | Expected Duration |
|-------|-------------------|-------------------|
| 10    | 2-4               | 1-3 minutes       |
| 50    | 4-8               | 3-7 minutes       |
| 100   | 8-16              | 5-10 minutes      |
| 500   | 16-32             | 15-30 minutes     |
| 1000+ | 32-64             | 25-45 minutes     |

### Optimization

1. **Right-size workers**: Match worker count to user count (up to 32)
2. **Use autoscaling**: min_workers=2, max_workers=16
3. **Batch processing**: Process in waves if user count > 200
4. **Monitor progress**: Use DEBUG_MODE=True for first run
5. **Cache results**: Store output in Delta for analysis

## Troubleshooting

### Issue: "File not found: /dbfs/tmp/users.txt"

**Solution:**
```python
# Check if file exists
dbutils.fs.ls("dbfs:/tmp/")

# Create the file
dbutils.fs.put("/tmp/users.txt", "user@example.com\n", overwrite=True)
```

### Issue: "No active executors"

**Cause:** Single-node cluster or cluster not fully started

**Solution:**
- Wait for cluster to fully start
- Use multi-node cluster (not single-node)
- Check cluster has workers > 0

### Issue: Permission denied

**Cause:** Notebook user doesn't have required permissions

**Solution:**
- Ensure you have read access to user directories in DBFS
- Ensure you have permission to read the user list file
- Contact workspace admin if permissions are needed

## Comparison: Notebook vs Python Script

**Use Notebook when:**
- ✅ Running on Databricks cluster
- ✅ Need RDD API access
- ✅ Want interactive debugging
- ✅ Scheduling as a Job
- ✅ Large-scale production workloads

**Use Python Script when:**
- ✅ Running from local machine
- ✅ Integration with local tools
- ✅ CI/CD pipelines
- ✅ Development/testing
- ✅ Need portability

## Summary

The notebook version provides:
- **Native cluster execution** (no Spark Connect)
- **Full Spark API access** (including RDD)
- **Better performance** (no network overhead)
- **Automatic authentication** (from notebook context)
- **Databricks integration** (display, dbutils)
- **Interactive debugging** (cell-by-cell execution)
- **Job scheduling** (automated runs)

Perfect for production workloads and large-scale user inventory processing!
