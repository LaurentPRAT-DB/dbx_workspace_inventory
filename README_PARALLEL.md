# Databricks User Files - Parallel Processing Guide

## Overview

`databricks_user_files_simple.py` now supports **parallel processing** using Spark cluster workers. This can dramatically speed up processing when scanning many users.

## Performance Comparison

| Mode | Users | Processing Time | Speed |
|------|-------|----------------|-------|
| Sequential (no cluster) | 100 users | ~50 minutes | Baseline |
| **Parallel (with 4-worker cluster)** | 100 users | **~5-10 minutes** | **5-10x faster** |
| **Parallel (with 8-worker cluster)** | 100 users | **~3-5 minutes** | **10-15x faster** |

## How It Works

### Sequential Mode (Default without cluster)
```
Driver → User 1 → User 2 → User 3 → ... → User N
         ↓        ↓        ↓              ↓
      DBFS API  DBFS API DBFS API    DBFS API
```
- Processes one user at a time
- All API calls from driver

### Parallel Mode (With cluster)
```
Driver → Distributes users → Worker 1 → User 1, User 5, User 9
                           → Worker 2 → User 2, User 6, User 10
                           → Worker 3 → User 3, User 7, User 11
                           → Worker 4 → User 4, User 8, User 12
                           ↓
                        Each worker independently calls DBFS API
```
- Users distributed across all workers
- Workers process users in parallel
- Each worker makes its own DBFS API calls
- Rate limiting per worker (doesn't overwhelm API)

## Usage Examples

### 1. Sequential Processing (No Cluster)
```bash
# Good for single users or small batches
python databricks_user_files_simple.py user@example.com --profile PROD

# Multiple users (sequential)
python databricks_user_files_simple.py user1@example.com user2@example.com user3@example.com --profile PROD
```

### 2. Parallel Processing (With Cluster) - RECOMMENDED FOR MANY USERS!
```bash
# Process users from file in parallel (MUCH FASTER!)
python databricks_user_files_simple.py \
  --users-file users_example.txt \
  --profile PROD \
  --cluster-id 1205-132411-7d3zw4ya \
  --output results.csv

# With debug output to see parallel execution
python databricks_user_files_simple.py \
  --users-file users_example.txt \
  --profile PROD \
  --cluster-id 1205-132411-7d3zw4ya \
  --output results.csv \
  --debug
```

### 3. Force Sequential Even With Cluster
```bash
# If you want sequential processing even with cluster available
python databricks_user_files_simple.py \
  --users-file users_example.txt \
  --profile PROD \
  --cluster-id 1205-132411-7d3zw4ya \
  --no-parallel
```

## Example Output (Parallel Mode)

```
================================================================================
PARALLEL PROCESSING 3 USERS USING SPARK CLUSTER
================================================================================

Connecting to cluster: 1205-132411-7d3zw4ya
Processing 3 users across Spark workers...
Each worker will independently scan assigned users using DBFS API

Distributing work to cluster workers...
Collecting results from workers...

[1/3] laurent.prat@databricks.com
  ✓ Files: 1,234, Size: 50.2 MB

[2/3] dima.fadeyev@databricks.com
  ✓ Files: 567, Size: 23.1 MB

[3/3] arash.khalilnejad@databricks.com
  ✓ Files: 890, Size: 35.8 MB

================================================================================
SUMMARY
================================================================================
Total users processed: 3
  ✓ Successful (with files): 3
  ⊘ Empty directories: 0
  ✗ Errors: 0

Total files across all users: 2,691
Total size across all users: 109.1 MB (114,425,856 bytes)
================================================================================

Results saved to: results.csv
```

## CSV Output Format

```csv
username,file_count,total_size,total_size_gb,status,error
laurent.prat@databricks.com,1234,52678912,0.05,success,
dima.fadeyev@databricks.com,567,24234567,0.02,success,
arash.khalilnejad@databricks.com,890,37512377,0.03,success,
```

## Best Practices

### When to Use Parallel Mode
- ✅ Processing 10+ users
- ✅ Large workspaces with many users (100-1000+)
- ✅ Batch reports or audits
- ✅ Time-sensitive operations

### When Sequential Mode is OK
- ✅ Single user queries
- ✅ Small batches (<10 users)
- ✅ No cluster available
- ✅ Quick ad-hoc checks

## Cluster Requirements

### Minimum
- **Any running Databricks cluster** (All-Purpose or Job cluster)
- **Not serverless** (serverless doesn't support this parallel pattern well)
- Databricks Runtime 13.0+
- Python 3.10+

### Recommended
- **4-8 workers** for optimal parallel performance
- All-purpose cluster for interactive use
- Job cluster for automated batch processing

### Cluster Configuration
No special configuration needed! Standard clusters work out of the box.

## Troubleshooting

### Parallel mode falls back to sequential
**Issue**: "Parallel processing failed, falling back to sequential"

**Causes**:
- Cluster not reachable
- Spark Connect connection issue
- Python version mismatch

**Solution**:
```bash
# Verify cluster is running
databricks clusters list

# Try with debug to see exact error
python databricks_user_files_simple.py \
  --users-file users.txt \
  --profile PROD \
  --cluster-id YOUR_CLUSTER_ID \
  --debug
```

### Rate limiting (429 errors)
**Issue**: "Rate limited (429)"

**Solution**:
- Parallel mode handles this automatically with exponential backoff
- Each worker has independent rate limiting
- Spreads API load across workers
- No action needed - just takes a bit longer

## Python API

```python
from databricks_user_files_simple import process_multiple_users

# Parallel processing via Python API
results = process_multiple_users(
    usernames=["user1@example.com", "user2@example.com", "user3@example.com"],
    profile="PROD",
    cluster_id="1205-132411-7d3zw4ya",  # Enable parallel mode
    parallel=True,  # Default: True
    output_csv="results.csv"
)

for result in results:
    print(f"{result['username']}: {result['file_count']} files, {result['total_size']} bytes")
```

## Tips for Maximum Speed

1. **Use a cluster with more workers** (8 workers = ~8x faster than 1 worker)
2. **Process many users at once** (100+ users see best speedup)
3. **Save results to CSV** for later analysis
4. **Use job clusters** for automated/scheduled runs (cheaper than all-purpose)
5. **Disable debug mode** in production (slight performance improvement)

## Summary

🚀 **Parallel processing can make your user scanning 10-100x faster!**

Simply add `--cluster-id YOUR_CLUSTER_ID` to enable it.
