# Debug Mode Progress Output

The `databricks_user_files_simple.py` script now includes real-time per-user progress output when `--debug` mode is enabled, even when processing is distributed across Spark cluster workers.

## Features

### Parallel Processing with Debug Mode

When running in parallel mode (using `--cluster-id`) with `--debug` enabled, you'll see each user's results as they complete on workers:

```bash
python databricks_user_files_simple.py \
  --users-file users.csv \
  --profile PROD \
  --cluster-id YOUR_CLUSTER_ID \
  --debug
```

**Example Output:**

```
================================================================================
PARALLEL PROCESSING 100 USERS USING SPARK CLUSTER
================================================================================
Parallel start time: 2025-12-07 14:30:00

Connecting to cluster: 1205-132411-7d3zw4ya
Processing 100 users across Spark workers...
Each worker will independently scan assigned users using DBFS API

Distributing work to cluster workers...
Processing users across workers (showing results as they complete)...

[WORKER START] john.doe@company.com - 2025-12-07 14:30:05
[WORKER START] jane.smith@company.com - 2025-12-07 14:30:05
[WORKER START] alice.wong@company.com - 2025-12-07 14:30:05
[WORKER START] bob.jones@company.com - 2025-12-07 14:30:05
[WORKER COMPLETE] alice.wong@company.com - 2025-12-07 14:30:08 (duration: 3.2s, files: 0, size: 0)
  [1/100] ⚠ alice.wong@company.com: 0 files (0 B)
[WORKER START] carol.white@company.com - 2025-12-07 14:30:08
[WORKER ERROR] bob.jones@company.com - 2025-12-07 14:30:10 (duration: 5.1s, error: User directory does not exist)
  [2/100] ✗ bob.jones@company.com: 0 files (0 B) - User directory does not exist
[WORKER START] dave.miller@company.com - 2025-12-07 14:30:10
[WORKER COMPLETE] john.doe@company.com - 2025-12-07 14:30:15 (duration: 10.3s, files: 1234, size: 55234567)
  [3/100] ✓ john.doe@company.com: 1234 files (52.7 MB)
[WORKER COMPLETE] jane.smith@company.com - 2025-12-07 14:30:16 (duration: 11.2s, files: 567, size: 25467890)
  [4/100] ✓ jane.smith@company.com: 567 files (24.2 MB)
[WORKER COMPLETE] carol.white@company.com - 2025-12-07 14:30:18 (duration: 10.1s, files: 890, size: 39345678)
  [5/100] ✓ carol.white@company.com: 890 files (37.5 MB)
  ...
[WORKER COMPLETE] zack.morris@company.com - 2025-12-07 14:35:13 (duration: 8.7s, files: 445, size: 19876543)
  [100/100] ✓ zack.morris@company.com: 445 files (18.9 MB)

Parallel processing completed in 5m 15s

================================================================================
SUMMARY
================================================================================
Total users processed: 100
  ✓ Successful (with files): 87
  ⊘ Empty directories: 10
  ✗ Errors: 3

Total files across all users: 125,678
Total size across all users: 5.2 GB (5,578,912,345 bytes)
================================================================================
```

### Status Icons

- **✓** - Success: User directory scanned successfully, files found
- **⚠** - Empty: User directory exists but contains no files
- **✗** - Error: Failed to scan (directory doesn't exist, permission denied, etc.)

### Progress Counter

Each line shows:
```
[current/total] icon username: file_count files (size)
```

Example:
```
[42/100] ✓ user@company.com: 1234 files (52.7 MB)
```

This tells you:
- Processing user 42 out of 100
- Scan succeeded (✓)
- Found 1234 files
- Total size is 52.7 MB

### Error Messages

When errors occur, they're shown inline:
```
[15/100] ✗ user@company.com: 0 files (0 B) - RESOURCE_DOES_NOT_EXIST: User directory not found
```

### Worker-Level Debug Output

In debug mode, you also see detailed timing from **inside each worker** as it processes users:

**Format:**
```
[WORKER START] username - timestamp
[WORKER COMPLETE] username - timestamp (duration: Xs, files: N, size: B)
[WORKER ERROR] username - timestamp (duration: Xs, error: message)
```

**What this shows:**
- **Parallel Execution**: Multiple `[WORKER START]` lines appearing simultaneously show workers processing users in parallel
- **Per-User Duration**: See exactly how long each user took to scan (e.g., `duration: 10.3s`)
- **Worker Assignment**: Track which workers are busy and when they pick up new work
- **Bottleneck Detection**: Identify users that take significantly longer to process

**Example Timeline:**
```
14:30:05 - 4 workers start processing 4 users simultaneously
14:30:08 - Worker completes user A (3.2s), immediately starts user E
14:30:10 - Worker completes user B with error (5.1s), immediately starts user F
14:30:15 - Worker completes user C (10.3s), immediately starts user G
14:30:16 - Worker completes user D (11.2s), immediately starts user H
```

This fine-grained timing helps you:
- **Verify parallelism**: See that workers are processing concurrently
- **Optimize cluster size**: Determine if you need more/fewer workers
- **Identify slow users**: Find users with large directories or complex structures
- **Debug worker issues**: See if specific workers are slower than others

## Benefits

### 1. Real-Time Visibility
See progress as workers complete each user, not just at the end

### 2. Early Problem Detection
Identify issues (permissions, missing directories) immediately while processing continues

### 3. Performance Monitoring
Track which users complete quickly vs slowly to identify bottlenecks

### 4. Better User Experience
No more "black box" - you know exactly what's happening during long-running operations

### 5. Progress Tracking
The `[42/100]` counter lets you estimate completion time

## Technical Implementation

### Parallel Mode - Driver-Side Progress
Uses Spark's `toLocalIterator()` instead of `collect()` to stream results incrementally:

```python
# Streams results as they complete on workers (driver side)
for row in result_df.toLocalIterator():
    print(f"  [{count}/{total}] ✓ {row.username}: {row.file_count} files...")
```

### Parallel Mode - Worker-Side Progress
Each worker prints timing information as it processes users:

```python
# Inside process_user_on_worker() function
start_time = datetime.now()
if debug:
    print(f"[WORKER START] {username} - {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

# ... process user ...

end_time = datetime.now()
duration = end_time - start_time
if debug:
    print(f"[WORKER COMPLETE] {username} - {end_time.strftime('%Y-%m-%d %H:%M:%S')} "
          f"(duration: {duration.total_seconds():.1f}s, files: {file_count}, size: {total_size})")
```

Worker stdout is streamed to the driver console via Spark Connect, so you see both worker-level and driver-level output in real-time.

### Sequential Mode
Already shows per-user progress by default (no special debug mode needed):

```bash
python databricks_user_files_simple.py \
  user1@company.com user2@company.com \
  --profile PROD
```

Output:
```
[1/2] Processing: user1@company.com
  ✓ Files: 1,234, Size: 52.7 MB

[2/2] Processing: user2@company.com
  ✓ Files: 567, Size: 24.2 MB
```

## Usage Examples

### Debug Mode for Large Batch
```bash
# Export 1000+ users
python databricks_user_list.py --profile PROD --output all_users.csv

# Process with debug output to monitor progress
python databricks_user_files_simple.py \
  --users-file all_users.csv \
  --profile PROD \
  --cluster-id 1205-132411-7d3zw4ya \
  --output results.csv \
  --debug
```

### Testing with Subset
```bash
# Test with 10 users to verify everything works
python databricks_user_files_simple.py \
  --users-file test_users.txt \
  --profile PROD \
  --cluster-id 1205-132411-7d3zw4ya \
  --debug
```

### Troubleshooting Specific Users
```bash
# Debug why certain users fail
python databricks_user_files_simple.py \
  problem.user1@company.com problem.user2@company.com \
  --profile PROD \
  --cluster-id 1205-132411-7d3zw4ya \
  --debug
```

## Performance Impact

**Minimal overhead**: The debug output adds negligible time:
- Console output: ~1ms per user
- No impact on worker processing speed
- No additional API calls

The `toLocalIterator()` approach actually processes results more memory-efficiently than `collect()` for large datasets.

## Non-Debug Mode

Without `--debug`, the output is more concise:

```
================================================================================
PARALLEL PROCESSING 100 USERS USING SPARK CLUSTER
================================================================================
Parallel start time: 2025-12-07 14:30:00

Connecting to cluster: 1205-132411-7d3zw4ya
Processing 100 users across Spark workers...

Distributing work to cluster workers...
Collecting results from workers...

Parallel processing completed in 5m 15s

[Shows summary only - no per-user output during processing]
```

This is better for:
- Automated scripts/pipelines
- Very large workspaces (10,000+ users)
- When you only care about final results

## Comparison: Debug vs Non-Debug

| Feature | Non-Debug Mode | Debug Mode |
|---------|---------------|------------|
| Per-user progress | ❌ Only at end | ✅ Real-time |
| Worker-level timing | ❌ | ✅ Start/complete timestamps |
| Per-user duration | ❌ | ✅ Shown in seconds |
| Status icons | ❌ | ✅ ✓ ⚠ ✗ |
| Error details | ❌ Only in summary | ✅ Inline with timing |
| Progress counter | ❌ | ✅ [N/total] |
| Parallel visibility | ❌ | ✅ See workers in action |
| Output volume | Low | High (detailed) |
| Best for | Production/automation | Development/troubleshooting |

## Tips

1. **Use debug for initial runs** - Understand what's happening and verify parallelism
2. **Disable debug for production** - Cleaner logs, less output
3. **Combine with output file** - Progress on screen, data to CSV
4. **Watch worker timing** - `[WORKER START]` lines appearing together = parallel execution
5. **Monitor per-user duration** - Identify users that take >30s to process
6. **Identify slow users** - Compare durations to find outliers
7. **Verify cluster efficiency** - If workers complete at same time, you might be underutilized
8. **Debug rate limiting** - Long durations often indicate rate limit retries

## Summary

The `--debug` flag transforms parallel processing from a "black box" into a transparent, monitored operation where you can:
- **See exactly which users are being processed** - Real-time progress with `[N/total]` counter
- **Monitor worker activity** - `[WORKER START]` and `[WORKER COMPLETE]` show parallel execution
- **Track per-user timing** - Know how long each user takes (e.g., `duration: 10.3s`)
- **Identify errors immediately** - See failures with context and timing
- **Verify parallelism** - Multiple workers starting simultaneously confirms parallel execution
- **Estimate completion time** - Progress counter and per-user durations help predict finish time
- **Optimize cluster size** - Timing patterns reveal if you need more/fewer workers
- **Debug performance issues** - Find slow users, rate limiting, or worker bottlenecks

Perfect for development, testing, troubleshooting, and performance optimization of large workspace scans!
