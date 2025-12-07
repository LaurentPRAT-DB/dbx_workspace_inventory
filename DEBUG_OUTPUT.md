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

  [1/100] ✓ john.doe@company.com: 1234 files (52.7 MB)
  [2/100] ✓ jane.smith@company.com: 567 files (24.2 MB)
  [3/100] ⚠ alice.wong@company.com: 0 files (0 B)
  [4/100] ✗ bob.jones@company.com: 0 files (0 B) - User directory does not exist
  [5/100] ✓ carol.white@company.com: 890 files (37.5 MB)
  ...
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

### Parallel Mode
Uses Spark's `toLocalIterator()` instead of `collect()` to stream results incrementally:

```python
# Streams results as they complete on workers
for row in result_df.toLocalIterator():
    print(f"  [{count}/{total}] ✓ {row.username}: {row.file_count} files...")
```

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
| Status icons | ❌ | ✅ |
| Error details | ❌ Only in summary | ✅ Inline |
| Progress counter | ❌ | ✅ [N/total] |
| Output volume | Low | Medium |
| Best for | Production/automation | Development/troubleshooting |

## Tips

1. **Use debug for initial runs** - Understand what's happening
2. **Disable debug for production** - Cleaner logs
3. **Combine with output file** - Progress on screen, data to CSV
4. **Monitor rate limits** - Debug shows when retries occur
5. **Identify slow users** - See which users take longest

## Summary

The `--debug` flag transforms parallel processing from a "black box" into a transparent, monitored operation where you can:
- See exactly which users are being processed
- Monitor progress in real-time
- Identify errors immediately
- Estimate completion time
- Debug issues without waiting for full completion

Perfect for development, testing, and troubleshooting large workspace scans!
