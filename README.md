# Databricks Workspace Inventory Application

A PySpark application that inventories all items stored in user home directories within a Databricks workspace. The application uses Spark workers to parallelize the scanning process for efficient processing across multiple users.

## Features

- **User Discovery**: Automatically retrieves all users from the Databricks workspace using the SCIM API
- **Parallel Processing**: Leverages PySpark cluster workers to scan multiple user directories simultaneously
- **Recursive Scanning**: Recursively lists all files and directories in each user's home directory
- **Comprehensive Output**: Generates a CSV file with detailed inventory information including:
  - User information (name, ID, email, display name)
  - File/directory paths and names
  - File sizes
  - Directory structure
  - Modification timestamps
  - Error information (if any)
- **Spark Connect Support**: Run locally while connecting to remote Databricks clusters
- **Serverless Compute Support**: Leverage Databricks serverless compute for automatic scaling
- **Cluster Capability Profiling**: Automatically detects and profiles cluster capabilities (serverless vs traditional, features, limitations)
- **Databricks CLI Integration**: Automatic credential detection from CLI configuration with profile support

## Prerequisites

### For Running on Databricks (Notebooks/Jobs)
- Databricks workspace with cluster access
- Databricks Personal Access Token (PAT) with appropriate permissions:
  - `users:read` permission to list workspace users
  - Read access to user home directories
- PySpark cluster (Databricks runtime includes this)

### For Running Locally with Spark Connect
- Databricks workspace with a running cluster
- Databricks Personal Access Token (PAT) with appropriate permissions
- Python 3.8+ with pip
- PySpark 3.5.0+ (installed via requirements.txt)
- (Optional) Databricks CLI for easier authentication
- Cluster ID of the target Databricks cluster

## Setup

### 1. Environment Variables

Set the following environment variables in your Databricks cluster configuration:

- `DATABRICKS_WORKSPACE_URL`: Your Databricks workspace URL (e.g., `https://your-workspace.cloud.databricks.com`)
- `DATABRICKS_TOKEN`: Your Databricks Personal Access Token
- `OUTPUT_PATH`: (Optional) Path where the CSV file will be saved. Default: `/tmp/workspace_inventory`
- `OUTPUT_FORMAT`: (Optional) Output format. Options: `csv`, `parquet`, `delta`. Default: `csv`

### 2. Setting Environment Variables in Databricks

#### Option A: Cluster Environment Variables (Recommended)

1. Go to your Databricks cluster configuration
2. Navigate to **Advanced Options** → **Environment Variables**
3. Add the following:
   ```
   DATABRICKS_WORKSPACE_URL=https://your-workspace.cloud.databricks.com
   DATABRICKS_TOKEN=your-token-here
   OUTPUT_PATH=/dbfs/workspace_inventory
   OUTPUT_FORMAT=csv
   ```

#### Option B: Set in Notebook

You can also set environment variables at the beginning of your notebook:

```python
import os
os.environ["DATABRICKS_WORKSPACE_URL"] = "https://your-workspace.cloud.databricks.com"
os.environ["DATABRICKS_TOKEN"] = "your-token-here"
os.environ["OUTPUT_PATH"] = "/dbfs/workspace_inventory"
os.environ["OUTPUT_FORMAT"] = "csv"
```

### 3. Create Personal Access Token

1. In Databricks, go to **User Settings** → **Access Tokens**
2. Click **Generate New Token**
3. Give it a name (e.g., "Workspace Inventory")
4. Set expiration (or leave as no expiration)
5. Copy the token immediately (you won't be able to see it again)

**Required Permissions:**
- The token needs permission to read user information via SCIM API
- The cluster/service principal needs read access to user home directories

### 4. Install Dependencies

The application requires the `requests` library. In Databricks, you can install it via:

```python
%pip install requests
```

Or add it to your cluster's libraries if running as a job.

## Usage

### Running in a Databricks Notebook

1. Create a new Python notebook in your Databricks workspace
2. Upload or paste the `workspace_inventory.py` code
3. Ensure environment variables are set (see Setup section)
4. Run the notebook

```python
# Option 1: Import and run
from workspace_inventory import main
main()

# Option 2: Run directly
%run /path/to/workspace_inventory
```

### Running as a Databricks Job

1. Create a new Job in Databricks
2. Add a new task with type **Python script**
3. Upload `workspace_inventory.py` or point to a DBFS path
4. Set environment variables in cluster configuration
5. Configure the cluster (recommended: use an existing cluster with appropriate permissions)
6. Run the job

### Running Locally with Spark Connect

You can now run this script locally while connecting to a remote Databricks cluster using **Spark Connect**. This allows you to:
- Develop and test locally
- Use your local IDE and debugging tools
- Execute Spark jobs on a Databricks cluster remotely

#### Prerequisites for Local Execution

1. **Enable Spark Connect on Your Databricks Cluster**
   
   Spark Connect must be enabled on your Databricks cluster:
   - Go to your cluster configuration
   - Under **Advanced Options** → **Spark** tab
   - Add the following Spark config:
     ```
     spark.connect.enabled true
     ```
   - Or use a cluster with Databricks Runtime 13.3 LTS or above (Spark Connect is enabled by default)

2. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Install Databricks CLI** (Optional but recommended)
   
   **macOS:**
   ```bash
   brew install databricks-cli
   ```
   
   **Other platforms:**
   ```bash
   pip install databricks-cli
   ```

3. **Configure Databricks CLI** (if using CLI for auth)
   ```bash
   databricks configure --token
   ```
   
   This will prompt you for:
   - Databricks Host: `https://your-workspace.cloud.databricks.com`
   - Token: Your Personal Access Token

#### Quick Start with Databricks CLI (Recommended)

The script automatically uses `~/.databrickscfg` for authentication when available:

```bash
# 1. Configure Databricks CLI (first time only)
databricks configure --token

# 2. List available profiles (optional)
python workspace_inventory.py --list-profiles

# 3. Run with default profile
python workspace_inventory.py --cluster-id your-cluster-id

# Or specify a profile
python workspace_inventory.py --profile PROD --cluster-id your-cluster-id

# Or use serverless compute
python workspace_inventory.py --profile PROD --serverless
```

#### Quick Start with Helper Script

The helper script simplifies execution with profile support:

```bash
# Use default profile
./run_local.sh --cluster-id your-cluster-id

# Use specific profile
./run_local.sh --profile PROD --cluster-id your-cluster-id

# Use serverless compute
./run_local.sh --profile PROD --serverless

# List available profiles
./run_local.sh --list-profiles
```

#### Command-Line Options

The script supports comprehensive command-line arguments:

```bash
python workspace_inventory.py [OPTIONS]

Options:
  -p, --profile PROFILE       Databricks CLI profile name from ~/.databrickscfg
  --workspace-url URL         Databricks workspace URL (overrides profile)
  --token TOKEN               Databricks personal access token (overrides profile)
  --cluster-id ID             Cluster ID for Spark Connect
  --serverless                Use serverless compute (auto-scaling)
  --output PATH               Output path for results (default: /tmp/workspace_inventory)
  --format {csv,parquet,delta} Output format (default: csv)
  --no-spark-connect          Disable Spark Connect mode
  --list-profiles             List available Databricks CLI profiles and exit
  -h, --help                  Show help message

Examples:
  # Use default profile with cluster ID
  python workspace_inventory.py --cluster-id cluster-123

  # Use specific profile
  python workspace_inventory.py --profile PROD --cluster-id cluster-123

  # Use serverless compute
  python workspace_inventory.py --profile PROD --serverless

  # Override output location and format
  python workspace_inventory.py --profile DEV --cluster-id cluster-123 \
    --output /dbfs/inventory --format parquet

  # List available profiles
  python workspace_inventory.py --list-profiles
```

**Configuration Priority:**
1. Command-line arguments (highest priority)
2. Databricks CLI profile from `~/.databrickscfg`
3. Environment variables (lowest priority)

#### Option 1: Using Databricks CLI Profiles

If you have multiple workspaces, you can configure multiple profiles:

```bash
# Configure first profile
databricks configure --token --profile DEFAULT

# Configure additional profile
databricks configure --token --profile PROD

# Use specific profile
python workspace_inventory.py --profile PROD --cluster-id cluster-123
```

The script automatically detects and uses credentials from `~/.databrickscfg`.

#### Option 2: Using Environment Variables

Set all required environment variables manually:

```bash
# Required: Workspace and authentication
export DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token-here"
export DATABRICKS_CLUSTER_ID="your-cluster-id"

# Optional: Enable Spark Connect explicitly
export USE_SPARK_CONNECT="true"

# Optional: Output configuration
export OUTPUT_PATH="/dbfs/workspace_inventory"
export OUTPUT_FORMAT="csv"

# Run the script
python workspace_inventory.py
```

#### Finding Your Cluster ID

To find your cluster ID:
1. Go to your Databricks workspace
2. Navigate to **Compute** → Select your cluster
3. Look at the URL: `https://your-workspace.cloud.databricks.com/#setting/clusters/[CLUSTER-ID]/configuration`
4. The cluster ID is the part in brackets

Or use the Databricks CLI:
```bash
databricks clusters list
```

#### Option 3: Using Serverless Compute (Recommended for Cost-Effective Scaling)

Serverless compute automatically manages infrastructure and scales based on workload. Perfect for on-demand processing without managing clusters.

**Prerequisites:**
1. Install Databricks Connect SDK (version must match your Databricks Runtime):
   ```bash
   pip install databricks-connect
   # Or specify version: pip install databricks-connect==14.0.0
   ```

2. Configure serverless compute in your `.env` file or environment:
   ```bash
   export DATABRICKS_SERVERLESS_COMPUTE_ID="auto"
   export USE_SPARK_CONNECT="true"
   ```

**Using Serverless Compute:**
```bash
# Set serverless compute mode
export DATABRICKS_SERVERLESS_COMPUTE_ID="auto"
export USE_SPARK_CONNECT="true"
export OUTPUT_PATH="/dbfs/workspace_inventory"
export OUTPUT_FORMAT="csv"

# Run the script
python workspace_inventory.py
```

**Serverless Compute Benefits:**
- ✅ **Auto-scaling**: Automatically scales resources based on workload
- ✅ **Cost-effective**: Pay only for compute time used
- ✅ **No cluster management**: Infrastructure managed automatically
- ✅ **Fast startup**: No cluster startup time
- ✅ **Cluster capability profiling**: Automatically detects and profiles capabilities

**Serverless Compute Limitations:**
- ⚠️ No R language support
- ⚠️ Limited Spark RDD APIs
- ⚠️ User-defined function (UDF) restrictions
- ⚠️ Session timeout after 10 minutes of inactivity

The application will automatically profile serverless compute capabilities and display them when running.

#### Local Execution Benefits

- **Full Functionality**: All features work as if running on Databricks
- **Local Development**: Use your favorite IDE and tools
- **Remote Execution**: Spark jobs run on the Databricks cluster
- **dbutils Support**: Available on worker nodes via Spark Connect

#### Troubleshooting Local Execution

**Error: "DATABRICKS_CLUSTER_ID must be set"**
- Make sure you've set the `DATABRICKS_CLUSTER_ID` environment variable
- The cluster must be running and accessible

**Error: "Connection refused"**
- Verify your cluster is running: `databricks clusters get --cluster-id <id>`
- Check firewall settings if connecting from a corporate network
- Ensure the cluster allows Spark Connect connections

**Error: "Authentication failed"**
- Verify your token is valid: `databricks workspace ls /`
- Check that the token hasn't expired
- Re-run `databricks configure --token` if using CLI

## Output

The application generates a CSV file with the following columns:

| Column | Description |
|--------|-------------|
| `user_name` | Username |
| `user_id` | Unique user ID |
| `user_display_name` | User's display name |
| `user_email` | User's email address |
| `path` | Full path to the file/directory |
| `name` | Name of the file/directory |
| `size` | File size in bytes (null for directories) |
| `is_directory` | Boolean indicating if it's a directory |
| `modification_time` | Last modification timestamp |
| `error` | Error message if access failed (null otherwise) |

### Output Location

- **CSV format**: The file will be saved as `part-*.csv` in the specified output path
- **Parquet/Delta format**: Saved directly to the output path

To find the CSV file:

```python
# List files in output directory
dbutils.fs.ls("/tmp/workspace_inventory")

# Or read the CSV directly
df = spark.read.csv("/tmp/workspace_inventory", header=True)
df.show()
```

## How It Works

1. **User Discovery**: The application uses the Databricks SCIM API to retrieve all users in the workspace
2. **Parallelization**: Each user is assigned to a Spark worker node for processing
3. **Directory Scanning**: Each worker recursively scans the user's home directory (`/Users/{username}`)
4. **Data Aggregation**: Results from all workers are collected and combined into a single DataFrame
5. **Output Generation**: The DataFrame is written to a CSV file (or Parquet/Delta format)

## Performance Considerations

- The application parallelizes work across all available Spark workers
- Each user's home directory is processed independently
- Large directories may take longer to scan
- Consider using a cluster with multiple workers for faster processing
- The recursive scan has a default max depth of 10 levels to prevent infinite loops

## Troubleshooting

### Error: "dbutils not available"

This script requires Databricks runtime. Ensure you're running it on a Databricks cluster, not locally.

### Error: "DATABRICKS_WORKSPACE_URL and DATABRICKS_TOKEN must be set"

Set the required environment variables in your cluster configuration or notebook.

### Error: "Permission denied" when accessing user directories

The cluster or service principal needs read access to user home directories. Ensure proper permissions are configured.

### Empty results

- Check that users exist in the workspace
- Verify that user home directories exist at `/Users/{username}`
- Check for permission errors in the output CSV file

### API Rate Limiting

If you have many users, the SCIM API may rate limit. The application includes pagination support, but you may need to add retry logic for very large workspaces.

## Security Notes

- **Token Security**: Never commit tokens to version control. Use environment variables or Databricks secrets.
- **Permissions**: The token should have minimal required permissions (users:read)
- **Output Location**: Be careful about where you store the output file, as it contains workspace structure information

## Example Output Summary

```
============================================================
INVENTORY SUMMARY
============================================================
  Total users processed: 25
  Total items found: 1,234
    - Files: 987
    - Directories: 247
  Total size: 5,432,109,876 bytes (5.06 GB)
============================================================
```

## License

This project is provided as-is for workspace inventory purposes.

## Support

For issues or questions:
1. Check the troubleshooting section
2. Review Databricks documentation for SCIM API and dbutils
3. Verify cluster permissions and token permissions

