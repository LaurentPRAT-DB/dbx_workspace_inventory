# Timing Logs Feature

Both `databricks_user_list.py` and `databricks_user_files_simple.py` now include comprehensive timing logs to track execution duration.

## Features

### Start Time Logging
- Records exact start time of execution
- Displays at the beginning of the run
- Shows processing mode (sequential vs parallel)

### End Time Logging
- Records exact end time of execution
- Calculates total duration
- Formats duration in human-readable format (hours/minutes/seconds)

### Duration Calculation
- Automatically calculates time elapsed
- Smart formatting:
  - Less than 1 minute: shows seconds only (e.g., "45s")
  - Less than 1 hour: shows minutes and seconds (e.g., "5m 30s")
  - Over 1 hour: shows hours, minutes, and seconds (e.g., "2h 15m 45s")

### Error Handling
- Logs timing even when errors occur
- Shows how long the script ran before failing

## databricks_user_list.py

### Example Output

```bash
$ python databricks_user_list.py --profile PROD --output users.csv

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

### Timing Information Shown
- Start time (timestamp)
- End time (timestamp)
- Total duration (formatted)
- Number of users processed
- Output file location (if applicable)

## databricks_user_files_simple.py

### Example Output (Sequential Mode)

```bash
$ python databricks_user_files_simple.py user@example.com --profile PROD

================================================================================
DATABRICKS USER FILES LISTING
================================================================================
Start time: 2025-12-06 14:30:00
Users to process: 1
Mode: SEQUENTIAL
================================================================================

[Processing output...]

================================================================================
COMPLETED SUCCESSFULLY
================================================================================
End time: 2025-12-06 14:32:15
Duration: 2m 15s
Users processed: 1
================================================================================
```

### Example Output (Parallel Mode)

```bash
$ python databricks_user_files_simple.py --users-file users.txt --profile PROD --cluster-id 1205-132411-7d3zw4ya

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

[Results summary...]

================================================================================
COMPLETED SUCCESSFULLY
================================================================================
End time: 2025-12-06 14:40:35
Duration: 5m 35s
Users processed: 100
Output file: results.csv
================================================================================
```

### Timing Information Shown
- Overall start time (entire script execution)
- Overall end time (entire script execution)
- Overall duration (entire script execution)
- Parallel processing start time (if using parallel mode)
- Parallel processing completion time (if using parallel mode)
- Parallel processing duration (if using parallel mode)
- Number of users processed
- Output file location (if applicable)
- Processing mode (Sequential vs Parallel)

## Error Case Example

If an error occurs, timing is still captured:

```bash
================================================================================
ERROR
================================================================================
Error: Connection timeout to cluster
End time: 2025-12-06 14:38:22
Duration: 3m 22s
================================================================================
```

## Benefits

1. **Performance Tracking**: Know exactly how long each run takes
2. **Comparison**: Compare sequential vs parallel performance
3. **Debugging**: Identify slow operations
4. **Reporting**: Include timing in reports and logs
5. **SLA Monitoring**: Track if processing meets time requirements
6. **Cost Analysis**: Understand cluster usage time for cost calculations

## Use Cases

### Compare Sequential vs Parallel Performance

```bash
# Sequential - measure baseline
python databricks_user_files_simple.py --users-file users.txt --profile PROD --no-parallel
# Duration: 45m 30s

# Parallel with 4-worker cluster
python databricks_user_files_simple.py --users-file users.txt --profile PROD --cluster-id CLUSTER_ID
# Duration: 5m 15s
# Speedup: 8.7x faster!
```

### Track Large Workspace Scans

```bash
# Export all users
python databricks_user_list.py --profile PROD --output all_users.csv
# Duration: 2m 15s (500 users)

# Process all users
python databricks_user_files_simple.py --users-file all_users.csv --profile PROD --cluster-id CLUSTER_ID --output results.csv
# Duration: 12m 30s (500 users in parallel)
```

### Audit Log Example

Create an audit log with timing:
```bash
echo "$(date): Starting workspace audit" >> audit.log
python databricks_user_list.py --profile PROD --output users.csv 2>&1 | tee -a audit.log
python databricks_user_files_simple.py --users-file users.csv --profile PROD --cluster-id CLUSTER_ID --output results.csv 2>&1 | tee -a audit.log
echo "$(date): Audit complete" >> audit.log
```

## Summary

Both scripts now provide comprehensive timing information that helps you:
- Monitor performance
- Compare different execution modes
- Debug slow operations
- Generate reports with execution metrics
- Track SLA compliance
- Calculate processing costs based on execution time

The timing logs are automatically included in every run, with clear formatting and human-readable duration displays.
