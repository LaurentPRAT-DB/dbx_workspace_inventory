# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **Databricks Workspace Inventory Application** - a PySpark application that inventories all items stored in user home directories within a Databricks workspace. The application uses Spark workers to parallelize the scanning process for efficient processing across multiple users.

### Key Architecture Components

- **Single Python Module**: `workspace_inventory.py` - contains all core functionality
- **Execution Modes**: 
  - Native Databricks runtime (notebooks/jobs)
  - Local execution via Spark Connect to remote Databricks clusters
  - Serverless compute support via Databricks Connect SDK
- **Authentication**: Supports Databricks CLI profiles (`~/.databrickscfg`) with fallback to environment variables
- **Processing Strategy**: Driver-side enumeration + worker-side traversal for optimal performance with dbutils availability

### Core Functions

- `get_databricks_users()` - Retrieves users via SCIM API with pagination and streaming
- `process_user_directory()` - Worker function that scans user home directories
- `driver_enumerate_frontier()` + `worker_traverse_start()` - Hybrid driver/worker approach
- `create_spark_connect_session()` + `create_serverless_session()` - Session management
- `get_databricks_cli_config()` - Authentication via CLI profiles

## Common Development Commands

### Running the Application

**Local execution (recommended for development):**
```bash
# Using default Databricks CLI profile
python workspace_inventory.py --cluster-id your-cluster-id

# Using specific profile
python workspace_inventory.py --profile PROD --cluster-id your-cluster-id

# Using serverless compute
python workspace_inventory.py --profile PROD --serverless

# Using helper script
./run_local.sh --profile PROD --cluster-id your-cluster-id
```

**Environment variables:**
```bash
export DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token"
export DATABRICKS_CLUSTER_ID="your-cluster-id"  # or DATABRICKS_SERVERLESS_COMPUTE_ID=auto
python workspace_inventory.py
```

### Development & Testing Commands

**Install dependencies:**
```bash
pip install -r requirements.txt
```

**Check dependencies:**
```bash
# Check if PySpark is available
python -c "import pyspark; print('PySpark OK')"

# Check if requests is available  
python -c "import requests; print('Requests OK')"

# Check if Databricks Connect is available (for serverless)
python -c "import databricks.connect; print('Databricks Connect OK')"
```

**Profile management:**
```bash
# List available Databricks CLI profiles
python workspace_inventory.py --list-profiles

# Configure new profile
databricks configure --token --profile PROD
```

**Debug mode:**
```bash
# Enable debug output for user fetching progress
python workspace_inventory.py --debug --max-users 5
```

### Output Formats and Locations

```bash
# Different output formats
python workspace_inventory.py --format csv --output /tmp/inventory
python workspace_inventory.py --format parquet --output /dbfs/inventory  
python workspace_inventory.py --format delta --output /dbfs/inventory
```

## Architecture Notes for Development

### Execution Modes Detection
- **Native Databricks Runtime**: Detected via `DATABRICKS_RUNTIME_VERSION` environment variable
- **Spark Connect**: Used when `DATABRICKS_CLUSTER_ID` is set or `--cluster-id` provided
- **Serverless**: Triggered by `DATABRICKS_SERVERLESS_COMPUTE_ID=auto` or `--serverless` flag

### Processing Strategy Evolution
The application uses a sophisticated fallback strategy:
1. **Driver-side listing with dbutils** (fastest) - lists directories on driver, distributes traversal to workers
2. **Distributed RDD processing** - traditional RDD.flatMap approach 
3. **DataFrame.mapInPandas** - Spark Connect compatible distributed processing
4. **Sequential fallback** - local processing when distributed modes fail

### Authentication Priority (Highest to Lowest)
1. Command-line arguments (`--workspace-url`, `--token`)
2. Databricks CLI profile from `~/.databrickscfg` 
3. Environment variables (`DATABRICKS_WORKSPACE_URL`, `DATABRICKS_TOKEN`)

### Python Version Compatibility
The application includes Python version detection between client and server to warn about potential UDF failures in Spark Connect mode. Use `--expected-python 3.11` to override server Python version detection.

## Key Dependencies

- **pyspark>=3.5.0** - Core Spark functionality and Spark Connect support
- **requests>=2.28.0** - SCIM API calls for user retrieval
- **databricks-connect** - Optional, required only for serverless compute
- **databricks-cli** - Optional, for easier authentication management

## Configuration Files

- **databricks.yml** - Databricks asset bundle configuration
- **requirements.txt** - Python dependencies
- **config.example.env** - Template for environment variables
- **run_local.sh** - Helper script for local execution with profile support
- **scripts/load_dbx_env.sh** - Environment variable loader script

## Development Tips

- Use `--debug` flag to see progress during user fetching (helpful for large workspaces)
- Use `--max-users N` to limit processing during development/testing
- Use `--force-sequential` to bypass distributed processing (useful for debugging)
- Test with different profiles using `--profile PROFILE_NAME`
- For serverless development, ensure databricks-connect version matches your Databricks Runtime

## Error Handling Patterns

The application implements comprehensive fallbacks:
- SCIM API pagination with error handling
- Multiple dbutils access methods (IPython, Spark JVM, sys.modules)
- Filesystem access via dbutils, /dbfs mount, and Hadoop FileSystem
- Authentication fallbacks from CLI config to environment variables
- Processing mode fallbacks from distributed to sequential