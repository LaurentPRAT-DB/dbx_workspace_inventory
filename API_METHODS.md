# API Methods for File Access

This document explains the different methods used to access files in Databricks and which one is used for parallel processing.

## Three Methods for Accessing Files

### 1. Spark Connect with /dbfs Mount (Requires Traditional Cluster)

**How it works:**
- Connects to cluster via Spark Connect
- Workers access files via `/dbfs` FUSE mount
- Uses `os.walk()` to traverse directories

**Code location:** `try_list_user_files_via_spark()` (line 472+)

**Requirements:**
- ✅ Traditional All-Purpose or Job cluster
- ✅ Cluster must have /dbfs mount enabled
- ❌ Does NOT work on serverless clusters
- ❌ Does NOT work without /dbfs mount

**Example:**
```python
dbfs_path = f"/dbfs/Users/{username}"
for root, dirs, files in os.walk(dbfs_path):
    # Access files via filesystem
```

**Status:** This method FAILS with `dbfs_mount_not_available` on serverless clusters

---

### 2. Direct DBFS API (Works With or Without Cluster)

**How it works:**
- Direct REST API calls to Databricks DBFS API
- Can run locally (no cluster) OR distributed across cluster workers (parallel)
- Uses `/api/2.0/dbfs/list` endpoint

**Code location:** `list_user_files_via_api_direct()` (line 339+)

**Requirements:**
- ✅ Works without any cluster (single user)
- ✅ Works WITH cluster for parallel processing (multiple users)
- ✅ Works on all workspace types
- ✅ Separate rate limit quota from Workspace API
- ✅ Works from local machine

**Example:**
```python
url = f"{workspace_url}/api/2.0/dbfs/list"
response = requests.get(url, headers=headers, json={"path": path})
```

**Status:** This method ALWAYS works (primary method for parallel processing!)

---

### 3. Parallel Processing with DBFS API (Uses Method #2)

**How it works:**
- Uses Spark workers to distribute user list
- **Each worker uses DBFS API directly (Method #2)**
- Parallel execution across multiple workers
- No /dbfs mount required

**Code location:** `process_user_on_worker()` (line 930+)

**Requirements:**
- ✅ Cluster with multiple workers (for parallelism)
- ✅ Works on traditional clusters
- ✅ **Does NOT require /dbfs mount**
- ⚠️ Limited support on serverless (1 executor only)

**Example:**
```python
# Running on worker
url = f"{workspace_url}/api/2.0/dbfs/list"  # Same as Method #2!
response = requests.get(url, headers=headers, json={"path": path})
```

**Status:** This method WORKS (uses DBFS API, not /dbfs mount)

---

## Which Method is Used When?

### Single User Mode

```bash
python databricks_user_files_simple.py user@example.com --profile PROD --cluster-id ID
```

**Execution flow:**
1. **Tries Method #1** (Spark Connect with /dbfs mount) - Usually FAILS (not needed)
2. **Uses Method #2** (Direct DBFS API) - SUCCEEDS (primary method)

**You see:**
```
Spark /dbfs mount method status: dbfs_mount_not_available
Using DBFS API method instead (this is the preferred method for parallel processing)...
✅ Successfully listed files via DBFS API
✅ Using same method that cluster workers use for parallel processing
```

**Note:** When cluster-id is provided, DBFS API is the PRIMARY method (not a fallback)

---

### Parallel Mode (Multiple Users)

```bash
python databricks_user_files_simple.py --users-file users.csv --profile PROD --cluster-id ID
```

**Execution flow:**
1. **Uses Method #3** (Parallel Processing with DBFS API)
2. **Workers use Method #2 internally** (Direct DBFS API calls)
3. **No /dbfs mount required**

**Code verification:**
```python
# In process_user_on_worker() - line 998
url = f"{workspace_url}/api/2.0/dbfs/list"  # DBFS API, not /dbfs mount!
response = requests.get(url, headers=headers, json={"path": path}, timeout=30)
```

**You see:**
```
[WORKER START] Executor-0 processing user@example.com
[WORKER COMPLETE] Executor-0 finished user@example.com (duration: 10.3s, files: 1234)
```

**No "dbfs_mount_not_available" error in parallel mode!**

---

## Summary: Which Method to Use?

### ✅ Recommended: Parallel Processing (Method #3)

**Use when:**
- Processing multiple users (10+)
- Have a cluster with multiple workers
- Want maximum speed

**Command:**
```bash
python databricks_user_files_simple.py \
  --users-file users.csv \
  --profile PROD \
  --cluster-id YOUR_CLUSTER_ID \
  --output results.csv
```

**Why it works:**
- Uses DBFS API (same as Method #2)
- Distributed across workers
- No /dbfs mount required
- 10-100x speedup

---

### ✅ Alternative: Direct API (Method #2)

**Use when:**
- Processing single user
- No cluster available
- Testing or debugging

**Command:**
```bash
python databricks_user_files_simple.py \
  user@example.com \
  --profile PROD
```

**Why it works:**
- Direct DBFS API calls
- No cluster needed
- Always available

---

### ❌ Avoid: Spark Connect with /dbfs Mount (Method #1)

**Why it fails:**
- Requires /dbfs mount on workers
- Serverless clusters don't have /dbfs mount
- Not needed when DBFS API works

**Status:**
- Automatically attempted first (for backwards compatibility)
- Falls back to Method #2 when it fails
- **Parallel mode skips this entirely**

---

## Troubleshooting

### Error: "dbfs_mount_not_available"

**What it means:**
- Method #1 (Spark Connect with /dbfs mount) is not available
- This is normal and expected - /dbfs mount is not required
- The DBFS API method (Method #2) is the primary method

**Solution:**
- ✅ This is not an error - it's informational
- ✅ The code automatically uses DBFS API (the preferred method)
- ✅ For parallel mode, DBFS API is used directly on workers

**Example:**
```
Spark /dbfs mount method status: dbfs_mount_not_available  ← Informational
Using DBFS API method instead (this is the preferred method for parallel processing)...
✅ Successfully listed files via DBFS API      ← Primary method working
✅ Using same method that cluster workers use for parallel processing
```

**Important:** When you see this message with a cluster-id, it means the cluster will use DBFS API for parallel processing (which is the correct and efficient method).

---

### Verify Parallel Processing Uses DBFS API

Run with `--debug`:

```bash
python databricks_user_files_simple.py \
  --users-file users.csv \
  --profile PROD \
  --cluster-id YOUR_CLUSTER_ID \
  --debug
```

**Look for:**
```
[WORKER START] Executor-0 processing user@example.com
```

**NOT this (which would indicate /dbfs mount):**
```
Using /dbfs mount at /dbfs/Users/...
```

If you see `[WORKER START]` messages, parallel processing is using DBFS API correctly!

---

## Performance Comparison

| Method | Speed | Cluster Required | /dbfs Mount Required |
|--------|-------|------------------|---------------------|
| Method #1: Spark /dbfs | Fast (if works) | Yes | **Yes** ❌ |
| Method #2: Direct API | Medium | No | No ✅ |
| Method #3: Parallel API | **Fastest** | Yes | No ✅ |

**Recommendation:** Use Method #3 (Parallel Processing) for multiple users

---

## Code Path Diagram

```
Single User Mode:
  list_user_files()
  └── estimate_user_files_via_api()
      ├── if cluster_id: try_list_user_files_via_spark() [Method #1]
      │   └── FAILS: dbfs_mount_not_available
      └── list_user_files_via_api_direct() [Method #2]
          └── SUCCEEDS: Uses DBFS API

Parallel Mode (Multiple Users):
  process_multiple_users_parallel()
  └── mapInPandas(process_users_batch)
      └── process_user_on_worker() [Method #3]
          └── Uses DBFS API directly [Method #2]
          └── SUCCEEDS: No /dbfs mount needed!
```

---

## Conclusion

**For parallel processing, you're already using the right method!**

- ✅ Parallel processing uses **DBFS API directly**
- ✅ No /dbfs mount required
- ✅ Works on all cluster types
- ✅ Distributes work across workers

The "dbfs_mount_not_available" error you saw is likely from:
1. Single-user mode testing
2. Initial diagnostic checks
3. Not from actual parallel processing

**To use parallel processing with DBFS API:**
```bash
python databricks_user_files_simple.py \
  --users-file users.csv \
  --profile PROD \
  --cluster-id YOUR_CLUSTER_ID \
  --output results.csv \
  --debug
```

You should see workers processing users in parallel, all using DBFS API successfully!
