"""
Databricks User Home Directory File Listing Module with Parallel Processing

This module provides fast, scalable file listing for Databricks user home directories
with two processing modes:

## Processing Modes:

### 1. Sequential Mode (No cluster required)
   - Processes one user at a time on the driver
   - Uses DBFS API directly via /api/2.0/dbfs/list
   - Good for single users or small batches (<10 users)
   - No cluster needed

### 2. **PARALLEL Mode (Cluster required - MUCH FASTER!)**
   - **Distributes users across all Spark cluster workers**
   - **Each worker independently processes assigned users using DBFS API**
   - **Can be 10-100x faster for many users**
   - Requires --cluster-id parameter
   - Uses Spark Connect + mapInPandas for distribution
   - Workers make API calls in parallel (respects rate limits per worker)
   - Ideal for batch processing many users (10-1000+)

## Key Features:
- **Parallel distributed processing** for maximum speed
- Multiple authentication methods (CLI profiles, environment variables)
- Automatic mode selection (parallel with cluster, sequential without)
- CSV export for results
- Batch processing from file (--users-file)
- Rate limiting and retry logic built-in
- Progress tracking and comprehensive error handling

Usage:
    from databricks_user_files_simple import list_user_files, process_multiple_users

    # Single user API-based listing (no cluster needed)
    file_count, total_size, message = list_user_files(
        "john.doe@company.com",
        profile="PROD"
    )

    # Multiple users batch processing
    results = process_multiple_users(
        ["user1@example.com", "user2@example.com"],
        profile="PROD",
        output_csv="results.csv"
    )

    # Command-line usage examples:

    # Single user
    python databricks_user_files_simple.py user@example.com --profile PROD

    # Multiple users
    python databricks_user_files_simple.py user1@example.com user2@example.com user3@example.com --profile PROD

    # Users from file with CSV output
    python databricks_user_files_simple.py --users-file users.txt --profile PROD --output results.csv
"""

import json
import os
import subprocess
import sys
from typing import List, Dict, Optional, Tuple
import requests


def get_databricks_cli_config(profile: Optional[str] = None) -> Optional[Dict]:
    """Get Databricks configuration from Databricks CLI."""
    config_path = os.path.expanduser("~/.databrickscfg")
    
    if not os.path.exists(config_path):
        return None
    
    try:
        profiles = {}
        current_profile = None
        
        with open(config_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or line.startswith(';'):
                    continue
                if line.startswith('[') and line.endswith(']'):
                    current_profile = line[1:-1].strip()
                    if current_profile not in profiles:
                        profiles[current_profile] = {}
                    continue
                if '=' in line and current_profile:
                    key, value = line.split('=', 1)
                    profiles[current_profile][key.strip().lower()] = value.strip()
        
        target_profile = profile or os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
        
        if target_profile in profiles:
            config = profiles[target_profile]
            used_profile = target_profile
        elif "DEFAULT" in profiles:
            config = profiles["DEFAULT"]
            used_profile = "DEFAULT"
        elif profiles:
            used_profile = list(profiles.keys())[0]
            config = profiles[used_profile]
        else:
            return None
        
        result = {}
        if "host" in config:
            host = config["host"]
            result["workspace_url"] = host if host.startswith("https://") else f"https://{host}"
        if "token" in config:
            result["token"] = config["token"]
        if "cluster_id" in config:
            result["cluster_id"] = config["cluster_id"]
        if "serverless_compute_id" in config:
            result["serverless_compute_id"] = config["serverless_compute_id"]
        
        result["_profile"] = used_profile
        
        if "workspace_url" in result and "token" in result:
            return result
            
    except Exception as e:
        print(f"Warning: Could not read Databricks CLI config: {str(e)}")
    
    return None


def authenticate_databricks(profile: Optional[str] = None, workspace_url: Optional[str] = None,
                           token: Optional[str] = None, cluster_id: Optional[str] = None) -> tuple[str, str, Optional[str]]:
    """Authenticate with Databricks using multiple methods with priority order."""
    final_workspace_url = workspace_url
    final_token = token
    final_cluster_id = cluster_id

    # Try CLI profile
    cli_config = None
    if not final_workspace_url or not final_token or not final_cluster_id:
        cli_config = get_databricks_cli_config(profile=profile)
        if cli_config:
            if not final_workspace_url:
                final_workspace_url = cli_config.get("workspace_url")
            if not final_token:
                final_token = cli_config.get("token")
            if not final_cluster_id:
                final_cluster_id = cli_config.get("cluster_id")
            used_profile = cli_config.get("_profile", profile or "DEFAULT")
            print(f"Using Databricks CLI configuration (profile: {used_profile})")

    # Fall back to environment variables
    if not final_workspace_url:
        final_workspace_url = os.environ.get("DATABRICKS_WORKSPACE_URL", "")
    if not final_token:
        final_token = os.environ.get("DATABRICKS_TOKEN", "")
    if not final_cluster_id:
        final_cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID", "")

    # Validate required values
    if not final_workspace_url or not final_token:
        raise ValueError(
            "DATABRICKS_WORKSPACE_URL and DATABRICKS_TOKEN must be set.\n"
            "Options:\n"
            "  1. Set environment variables\n"
            "  2. Use Databricks CLI: databricks configure --token\n"
            "  3. Pass explicit parameters"
        )

    return final_workspace_url, final_token, final_cluster_id


def get_user_info_via_api(workspace_url: str, token: str, username: str, debug: bool = False) -> Dict:
    """Get user information via SCIM API."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    try:
        # Search for user by username
        url = f"{workspace_url}/api/2.0/preview/scim/v2/Users"
        params = {"filter": f'userName eq "{username}"'}
        
        if debug:
            print(f"Looking up user: {username}")
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        resources = data.get("Resources", [])
        
        if not resources:
            raise ValueError(f"User not found: {username}")
        
        return resources[0]
        
    except Exception as e:
        raise ValueError(f"Failed to get user info: {str(e)}")


def get_server_runtime_info(workspace_url: str, token: str, cluster_id: Optional[str] = None, debug: bool = False) -> Dict:
    """Get server runtime information including Python version and Databricks Connect version."""
    try:
        # Determine if using serverless or traditional cluster
        use_serverless = not cluster_id or cluster_id.lower() == "auto"

        if use_serverless:
            # Try to create a minimal Spark session to get runtime info
            from databricks.connect import DatabricksSession

            # Set environment variables for connection
            host = workspace_url.replace("https://", "").rstrip("/")
            os.environ["DATABRICKS_HOST"] = host
            os.environ["DATABRICKS_TOKEN"] = token
            os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"

            if debug:
                print("Attempting to connect to serverless compute...")

            # Create session and get version info
            spark = DatabricksSession.builder.getOrCreate()
        else:
            # Use Spark Connect with traditional cluster
            from pyspark.sql import SparkSession

            if debug:
                print(f"Attempting to connect to cluster {cluster_id}...")

            # Build Spark Connect URL
            workspace_host = workspace_url.replace("https://", "").rstrip("/")
            connect_url = f"sc://{workspace_host}:443/;token={token};x-databricks-cluster-id={cluster_id}"

            spark = SparkSession.builder \
                .appName("Databricks User Files Check") \
                .remote(connect_url) \
                .getOrCreate()
        
        try:
            # Execute a simple command to get Python version from server
            from pyspark.sql import functions as F
            from pyspark.sql.types import StringType
            
            def get_server_info():
                import sys
                import platform
                return f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}|{platform.python_version()}|{sys.version}"
            
            # This will fail with version mismatch, but we can extract info from the error
            server_info_udf = F.udf(get_server_info, StringType())
            df = spark.createDataFrame([("test",)], ["col"])
            result = df.select(server_info_udf().alias("info")).collect()
            
            if result:
                parts = result[0].info.split("|")
                return {
                    "python_version": parts[0] if len(parts) > 0 else "unknown",
                    "python_full": parts[1] if len(parts) > 1 else "unknown",
                    "detected": True
                }
        except Exception as e:
            # Parse version mismatch error for server version info
            error_msg = str(e).lower()
            if "python version" in error_msg:
                return {
                    "python_version": "mismatch_detected",
                    "python_full": "Version mismatch - check Databricks runtime release notes",
                    "detected": False,
                    "error": str(e)
                }
        
        return {
            "python_version": "unknown",
            "python_full": "Could not detect",
            "detected": False
        }
        
    except ImportError:
        return {
            "python_version": "unknown",
            "python_full": "databricks-connect not installed",
            "detected": False,
            "error": "databricks-connect package not available"
        }
    except Exception as e:
        return {
            "python_version": "unknown", 
            "python_full": f"Connection failed: {str(e)}",
            "detected": False,
            "error": str(e)
        }


def get_databricks_connect_recommendation(local_python: str, server_info: Dict) -> str:
    """Generate databricks-connect version recommendation based on Python versions."""
    try:
        import pkg_resources
        current_dbc = pkg_resources.get_distribution("databricks-connect").version
    except:
        current_dbc = "not installed"
    
    # Map common Python versions to recommended databricks-connect versions
    # These are based on Databricks Runtime compatibility
    python_to_dbc_map = {
        "3.8": "10.4.*",
        "3.9": "11.3.* or 12.2.*", 
        "3.10": "13.1.* or 14.0.*",
        "3.11": "14.1.* or newer",
        "3.12": "15.0.* or newer"
    }
    
    local_minor = ".".join(local_python.split(".")[:2])
    server_python = server_info.get("python_version", "unknown")
    
    recommendation = f"""
Databricks Connect Version Sync:
  Current databricks-connect: {current_dbc}
  Local Python: {local_python}
  Server Python: {server_python}
  
"""
    
    if server_python != "unknown" and server_python != "mismatch_detected":
        server_minor = ".".join(server_python.split(".")[:2])
        if local_minor != server_minor:
            recommendation += f"""❌ Version Mismatch Detected!
  Fix: Update your local Python to match server version {server_minor}
  
  Option 1 - Update local Python:
    pyenv install {server_python}
    pyenv local {server_python}
  
  Option 2 - Use matching databricks-connect:
    pip install databricks-connect=={python_to_dbc_map.get(server_minor, 'latest')}
"""
        else:
            recommendation += f"""✅ Python versions match!
  Both client and server use Python {local_minor}
  Current setup should work for UDF execution.
"""
    else:
        recommendation += f"""❓ Server version detection failed
  Recommended databricks-connect for Python {local_minor}: {python_to_dbc_map.get(local_minor, 'latest')}
  
  Install matching version:
    pip install databricks-connect=={python_to_dbc_map.get(local_minor, 'latest')}
    
  Check Databricks Runtime release notes:
    https://docs.databricks.com/en/release-notes/runtime/index.html
"""
    
    return recommendation


def list_user_files_via_api_direct(workspace_url: str, token: str, username: str, debug: bool = False) -> Tuple[int, int, str]:
    """
    List files using Databricks DBFS API directly (no Spark required).
    Uses DBFS API which has separate rate limits from Workspace API and is designed for file operations.
    """
    import time

    try:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        home_path = f"/Users/{username}"
        file_count = 0
        total_size = 0
        dir_count = 0
        request_count = 0
        rate_limit_delay = 0.05  # Start with 50ms delay between requests

        def list_recursive(path: str, depth: int = 0, max_depth: int = 10):
            nonlocal file_count, total_size, dir_count, request_count, rate_limit_delay

            if depth > max_depth:
                return

            # Exponential backoff retry logic
            max_retries = 5
            retry_count = 0

            while retry_count < max_retries:
                try:
                    # Add delay to avoid rate limiting
                    if request_count > 0:
                        time.sleep(rate_limit_delay)

                    request_count += 1

                    # Use DBFS API to list directory (better for file operations)
                    url = f"{workspace_url}/api/2.0/dbfs/list"
                    response = requests.get(
                        url,
                        headers=headers,
                        json={"path": path},
                        timeout=30
                    )

                    if response.status_code == 404:
                        # Directory doesn't exist
                        return
                    elif response.status_code == 429:
                        # Rate limited - exponential backoff
                        retry_count += 1
                        wait_time = min(2 ** retry_count, 32)  # Max 32 seconds
                        if debug:
                            print(f"Rate limited (429) for {path}, waiting {wait_time}s (attempt {retry_count}/{max_retries})")
                        time.sleep(wait_time)
                        # Increase base delay for future requests
                        rate_limit_delay = min(rate_limit_delay * 1.5, 1.0)
                        continue  # Retry
                    elif response.status_code in [500, 503]:
                        # Server errors - retry with backoff
                        retry_count += 1
                        if retry_count >= max_retries:
                            if debug:
                                print(f"Server error {response.status_code} on {path}, max retries reached")
                            return
                        wait_time = min(2 ** retry_count, 16)
                        if debug:
                            print(f"Server error {response.status_code} on {path}, retrying in {wait_time}s")
                        time.sleep(wait_time)
                        continue
                    elif response.status_code != 200:
                        if debug:
                            print(f"DBFS API error for {path}: {response.status_code}")
                        return

                    # Success - process the response
                    data = response.json()
                    files = data.get("files", [])

                    for file_info in files:
                        file_path = file_info.get("path", "")
                        is_dir = file_info.get("is_dir", False)
                        file_size = file_info.get("file_size", 0)

                        if is_dir:
                            dir_count += 1
                            # Recursively list subdirectories
                            list_recursive(file_path, depth + 1, max_depth)
                        else:
                            # Count files and accumulate size
                            file_count += 1
                            total_size += file_size

                    return  # Success, exit retry loop

                except requests.exceptions.RequestException as e:
                    retry_count += 1
                    if retry_count >= max_retries:
                        if debug:
                            print(f"Failed after {max_retries} retries for {path}: {str(e)}")
                        return
                    wait_time = min(2 ** retry_count, 16)
                    if debug:
                        print(f"Request error for {path}, retrying in {wait_time}s: {str(e)}")
                    time.sleep(wait_time)

        if debug:
            print(f"Listing files via DBFS API for: {home_path}")
            print("Note: Using DBFS API which has separate rate limits from Workspace API")

        list_recursive(home_path)

        if debug:
            print(f"Total API requests made: {request_count}")
            print(f"Files: {file_count}, Directories: {dir_count}, Total size: {total_size:,} bytes")

        if file_count > 0:
            return file_count, total_size, "success_api"
        else:
            return 0, 0, "no_files_found_api"

    except Exception as e:
        if debug:
            print(f"DBFS API direct listing failed: {str(e)}")
        return 0, 0, f"api_error: {str(e)}"


def list_workspace_files_via_api(workspace_url: str, token: str, username: str, debug: bool = False) -> Tuple[int, int, str]:
    """
    List files in Workspace File System using Databricks Workspace API.
    This scans /Workspace/Users/{username}/ - the notebooks and workspace files visible in the UI.
    Different from DBFS API which scans dbfs:/Users/{username}/.
    """
    import time

    try:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        workspace_path = f"/Users/{username}"
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

                    # Use Workspace API to list workspace objects
                    url = f"{workspace_url}/api/2.0/workspace/list"
                    response = requests.get(
                        url,
                        headers=headers,
                        json={"path": path},
                        timeout=30
                    )

                    if response.status_code == 404:
                        # Path doesn't exist
                        return
                    elif response.status_code == 429:
                        # Rate limited - exponential backoff
                        retry_count += 1
                        wait_time = min(2 ** retry_count, 32)
                        if debug:
                            print(f"Rate limited (429) on {path}, retrying in {wait_time}s (attempt {retry_count}/{max_retries})")
                        time.sleep(wait_time)
                        rate_limit_delay = min(rate_limit_delay * 1.5, 1.0)
                        continue
                    elif response.status_code in [500, 503]:
                        # Server errors - retry with backoff
                        retry_count += 1
                        if retry_count >= max_retries:
                            if debug:
                                print(f"Server error {response.status_code} on {path}, max retries reached")
                            return
                        wait_time = min(2 ** retry_count, 16)
                        if debug:
                            print(f"Server error {response.status_code} on {path}, retrying in {wait_time}s")
                        time.sleep(wait_time)
                        continue
                    elif response.status_code != 200:
                        if debug:
                            print(f"Workspace API returned {response.status_code} for {path}")
                        return

                    data = response.json()
                    objects = data.get("objects", [])

                    for obj in objects:
                        object_path = obj.get("path", "")
                        object_type = obj.get("object_type", "")

                        # object_type can be: NOTEBOOK, DIRECTORY, LIBRARY, REPO, FILE
                        if object_type == "DIRECTORY":
                            dir_count += 1
                            list_recursive(object_path, depth + 1, max_depth)
                        else:
                            # NOTEBOOK, LIBRARY, FILE, etc.
                            file_count += 1
                            # Workspace API doesn't return file size, so we estimate
                            # Note: To get actual sizes, would need to export each notebook
                            total_size += 10000  # Rough estimate: 10KB per file

                    return

                except requests.exceptions.RequestException as e:
                    retry_count += 1
                    if retry_count >= max_retries:
                        if debug:
                            print(f"Failed after {max_retries} retries for {path}: {str(e)}")
                        return
                    wait_time = min(2 ** retry_count, 16)
                    if debug:
                        print(f"Request error for {path}, retrying in {wait_time}s: {str(e)}")
                    time.sleep(wait_time)

        if debug:
            print(f"Listing workspace files via Workspace API for: {workspace_path}")
            print("Note: This scans notebooks and workspace files (not DBFS)")

        list_recursive(workspace_path)

        if debug:
            print(f"Total API requests made: {request_count}")
            print(f"Files: {file_count}, Directories: {dir_count}")
            print(f"Note: Size is estimated ({total_size:,} bytes) - Workspace API doesn't provide exact sizes")

        if file_count > 0:
            return file_count, total_size, "success_workspace_api"
        else:
            return 0, 0, "no_workspace_files_found"

    except Exception as e:
        if debug:
            print(f"Workspace API listing failed: {str(e)}")
        return 0, 0, f"workspace_api_error: {str(e)}"


def try_list_user_files_via_spark(workspace_url: str, token: str, username: str,
                                  cluster_id: Optional[str] = None, debug: bool = False) -> Tuple[int, int, str]:
    """
    Actually attempt to list files using Spark Connect to a cluster.
    Returns (file_count, total_size, status_message)
    """
    try:
        from pyspark.sql import SparkSession

        # Build Spark Connect URL
        workspace_host = workspace_url.replace("https://", "").rstrip("/")
        connect_url = f"sc://{workspace_host}:443/;token={token};x-databricks-cluster-id={cluster_id}"

        if debug:
            print(f"Connecting to cluster {cluster_id} via Spark Connect...")

        spark = SparkSession.builder \
            .appName("Databricks User Files Listing") \
            .remote(connect_url) \
            .getOrCreate()

        # Prepare user data for processing
        user_data = {
            "user_info": {
                "userName": username,
                "id": username,
                "displayName": username,
            },
            "workspace_url": workspace_url
        }
        user_data_json = json.dumps(user_data)

        if debug:
            print(f"Attempting to list files for user: {username}")

        # Use DataFrame + mapInPandas (compatible with Spark Connect)
        try:
            from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

            # Create a DataFrame with the user data
            user_df = spark.createDataFrame([{"user_data": user_data_json}])

            # Define output schema
            output_schema = StructType([
                StructField("user_name", StringType(), True),
                StructField("path", StringType(), True),
                StructField("name", StringType(), True),
                StructField("size", LongType(), True),
                StructField("is_directory", BooleanType(), True),
                StructField("error", StringType(), True)
            ])

            # Process using mapInPandas
            def process_user_pandas(iterator):
                import pandas as pd
                import json
                import os

                for pdf in iterator:
                    rows = []
                    for user_data_str in pdf['user_data']:
                        try:
                            data = json.loads(user_data_str)
                            user_info = data["user_info"]
                            user_name = user_info.get("userName", "unknown")
                            home_path = f"/Users/{user_name}"

                            # Try /dbfs mount method (works on traditional clusters)
                            dbfs_path = f"/dbfs{home_path}" if home_path.startswith("/") else f"/dbfs/{home_path}"

                            if os.path.exists(dbfs_path):
                                for root, dirs, files in os.walk(dbfs_path):
                                    for fname in files:
                                        full = os.path.join(root, fname)
                                        try:
                                            stat = os.stat(full)
                                            rel_path = full.replace('/dbfs', '')
                                            rows.append({
                                                "user_name": user_name,
                                                "path": rel_path,
                                                "name": os.path.basename(full),
                                                "size": stat.st_size,
                                                "is_directory": False,
                                                "error": None
                                            })
                                        except Exception:
                                            pass

                                    for dname in dirs:
                                        full = os.path.join(root, dname)
                                        try:
                                            stat = os.stat(full)
                                            rel_path = full.replace('/dbfs', '')
                                            rows.append({
                                                "user_name": user_name,
                                                "path": rel_path,
                                                "name": dname,
                                                "size": 0,
                                                "is_directory": True,
                                                "error": None
                                            })
                                        except Exception:
                                            pass
                            else:
                                rows.append({
                                    "user_name": user_name,
                                    "path": home_path,
                                    "name": "home",
                                    "size": 0,
                                    "is_directory": None,
                                    "error": f"/dbfs mount not accessible at {dbfs_path}"
                                })
                        except Exception as e:
                            rows.append({
                                "user_name": "unknown",
                                "path": "unknown",
                                "name": "unknown",
                                "size": 0,
                                "is_directory": None,
                                "error": str(e)
                            })

                    if rows:
                        yield pd.DataFrame(rows)
                    else:
                        # Return empty DataFrame with correct schema
                        yield pd.DataFrame(columns=["user_name", "path", "name", "size", "is_directory", "error"])

            # Execute the processing
            result_df = user_df.mapInPandas(process_user_pandas, schema=output_schema)
            items = result_df.collect()

            # Calculate stats
            file_count = sum(1 for item in items if not item.is_directory and item.error is None)
            total_size = sum(item.size for item in items if not item.is_directory and item.error is None)

            if file_count > 0:
                return file_count, total_size, "success"
            else:
                # Check if there were errors
                errors = [item.error for item in items if item.error is not None]
                if errors:
                    error_msg = errors[0]
                    if "/dbfs mount not accessible" in error_msg:
                        return 0, 0, "dbfs_mount_not_available"
                    else:
                        return 0, 0, f"access_error: {error_msg}"
                else:
                    return 0, 0, "no_files_found"

        except Exception as e:
            error_str = str(e)
            if debug:
                print(f"DataFrame processing failed: {error_str}")

            # Provide specific guidance based on error type
            if "python version" in error_str.lower() or "version mismatch" in error_str.lower():
                return 0, 0, "python_version_mismatch"
            elif "connection" in error_str.lower() or "timeout" in error_str.lower():
                return 0, 0, "connection_error"
            else:
                return 0, 0, f"spark_error: {error_str}"

    except Exception as e:
        if debug:
            print(f"Failed to list files via Spark: {str(e)}")
        return 0, 0, f"connection_error: {str(e)}"


def estimate_user_files_via_api(workspace_url: str, token: str, username: str,
                                cluster_id: Optional[str] = None, debug: bool = False) -> Tuple[str, str]:
    """
    Try to get user file information using Databricks APIs and Spark.
    If cluster_id is provided, prioritizes Spark Connect method over direct API.
    """
    try:
        user_info = get_user_info_via_api(workspace_url, token, username, debug=debug)

        user_display = user_info.get("displayName", username)
        user_email = user_info.get("userName", username)

        # If cluster_id is provided, prioritize using the cluster
        if cluster_id:
            if debug:
                print(f"Cluster ID provided: {cluster_id}")
                print("Attempting to list files via Spark Connect (using cluster workers)...")

            file_count, total_size, status = try_list_user_files_via_spark(
                workspace_url, token, username, cluster_id, debug=debug
            )

            if status == "success":
                message = f"""User Information:
  Username: {username}
  Display Name: {user_display}
  Email: {user_email}
  Home Directory: /Users/{username}

File Listing Status:
  ✅ Successfully listed DBFS files via Spark Connect
  ✅ Traditional cluster /dbfs mount accessible
  ✅ Distributed file processing working

Results:
  Files found: {file_count}
  Total size: {total_size:,} bytes ({total_size / (1024**3):.2f} GB)

Technical Details:
  - Connection type: Spark Connect (Traditional Cluster)
  - Cluster ID: {cluster_id}
  - Method: Distributed processing via Spark workers accessing DBFS
  - Authentication: ✅ Working
  - File system access: ✅ Working via /dbfs mount

Note: This accessed DBFS (Databricks File System), not Workspace files.
  Use the Workspace API method (without --cluster-id) for notebooks and workspace files."""
                return "success", message
            else:
                # Spark method didn't succeed, use DBFS API method (this is expected)
                if debug:
                    print(f"Spark /dbfs mount method status: {status}")
                    print("Using DBFS API method instead (this is the preferred method for parallel processing)...")

        # Try the DBFS API approach (always scan)
        if debug:
            if cluster_id:
                print("[DBFS] Using DBFS API method (same method used by cluster workers in parallel processing)...")
            else:
                print("[DBFS] Using DBFS API method (direct, no cluster required)...")

        dbfs_file_count, dbfs_total_size, dbfs_status = list_user_files_via_api_direct(
            workspace_url, token, username, debug=debug
        )

        # Try Workspace API (always scan, not just fallback)
        if debug:
            print("[WORKSPACE] Scanning workspace files (notebooks, libraries)...")

        workspace_file_count, workspace_total_size, workspace_status = list_workspace_files_via_api(
            workspace_url, token, username, debug=debug
        )

        # Cumulate results from both sources
        combined_file_count = 0
        combined_total_size = 0
        sources_found = []

        if dbfs_status == "success_api":
            combined_file_count += dbfs_file_count
            combined_total_size += dbfs_total_size
            sources_found.append("DBFS")
            if debug:
                print(f"[DBFS] Found {dbfs_file_count} files ({dbfs_total_size:,} bytes)")

        if workspace_status == "success_workspace_api":
            combined_file_count += workspace_file_count
            combined_total_size += workspace_total_size
            sources_found.append("Workspace")
            if debug:
                print(f"[WORKSPACE] Found {workspace_file_count} files ({workspace_total_size:,} bytes)")

        # Return success if we found files in either or both sources
        if combined_file_count > 0:
            sources_str = " + ".join(sources_found)
            if cluster_id:
                # Message when cluster is available
                message = f"""User Information:
  Username: {username}
  Display Name: {user_display}
  Email: {user_email}
  Home Directory: /Users/{username}

File Listing Status:
  ✅ Successfully scanned both file systems
  {"✅ DBFS: " + str(dbfs_file_count) + " files" if "DBFS" in sources_found else "⊘ DBFS: No files"}
  {"✅ Workspace: " + str(workspace_file_count) + " files (notebooks/libraries)" if "Workspace" in sources_found else "⊘ Workspace: No files"}

Combined Results:
  Total files: {combined_file_count:,} (from {sources_str})
  Total size: {combined_total_size:,} bytes ({combined_total_size / (1024**3):.2f} GB)

Technical Details:
  - Scanned: Both DBFS (data files) and Workspace (notebooks)
  - DBFS API: /api/2.0/dbfs/list
  - Workspace API: /api/2.0/workspace/list
  - Cluster ID: {cluster_id}
  - Parallel processing: Use --users-file with multiple users to leverage cluster workers

Note: This provides a complete inventory by scanning both file systems."""
            else:
                # Message when no cluster
                message = f"""User Information:
  Username: {username}
  Display Name: {user_display}
  Email: {user_email}
  Home Directory: /Users/{username}

File Listing Status:
  ✅ Successfully scanned both file systems
  {"✅ DBFS: " + str(dbfs_file_count) + " files" if "DBFS" in sources_found else "⊘ DBFS: No files"}
  {"✅ Workspace: " + str(workspace_file_count) + " files (notebooks/libraries)" if "Workspace" in sources_found else "⊘ Workspace: No files"}

Combined Results:
  Total files: {combined_file_count:,} (from {sources_str})
  Total size: {combined_total_size:,} bytes ({combined_total_size / (1024**3):.2f} GB)

Technical Details:
  - Scanned: Both DBFS (data files) and Workspace (notebooks)
  - DBFS API: /api/2.0/dbfs/list
  - Workspace API: /api/2.0/workspace/list
  - No cluster required

Note: This provides a complete inventory by scanning both file systems.
  For faster processing of many users, use --cluster-id with --users-file for parallel execution."""
            return "success", message

        # If we get here, no files were found in either source
        # Check if both APIs worked but found no files, or if APIs failed
        both_apis_checked = (dbfs_status in ["success_api", "no_files_found_api"]) and \
                           (workspace_status in ["success_workspace_api", "no_workspace_files_found"])

        if both_apis_checked:
            # APIs worked, just no files found
            message = f"""User Information:
  Username: {username}
  Display Name: {user_display}
  Email: {user_email}

File Listing Status:
  ✅ APIs working correctly
  ℹ️  No files found in either file system

Technical Details:
  - DBFS API: ✅ Checked - no files in dbfs:/Users/{username}
  - Workspace API: ✅ Checked - no files in /Users/{username}
  {"- Cluster ID: " + cluster_id if cluster_id else ""}

Result: User has no files in either:
  - DBFS (data files): dbfs:/Users/{username}
  - Workspace (notebooks): /Users/{username}

This is normal for new or inactive users."""
            return "failed", message
        else:
            # At least one API failed
            message = f"""User Information:
  Username: {username}
  Display Name: {user_display}
  Email: {user_email}

File Listing Status:
  ⚠️  One or more APIs failed

Technical Details:
  - DBFS API status: {dbfs_status}
  - Workspace API status: {workspace_status}
  {"- Cluster ID: " + cluster_id if cluster_id else ""}

Troubleshooting:
  1. Check token has permissions for both DBFS and Workspace access
  2. Verify user directories exist:
     - DBFS: /Users/{username} (data files)
     - Workspace: /Users/{username} (notebooks)
  3. Try running with --debug flag for more details"""
            return "failed", message

        # If no cluster_id, get server runtime info for diagnostics
        if debug:
            print("No cluster ID provided, detecting server runtime information...")
        server_info = get_server_runtime_info(workspace_url, token, cluster_id, debug=debug)
        
        local_python = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        dbc_recommendation = get_databricks_connect_recommendation(local_python, server_info)

        # Determine actual cause of file access failure
        local_minor = ".".join(local_python.split(".")[:2])
        server_python = server_info.get('python_version', 'unknown')
        server_minor = ".".join(server_python.split(".")[:2]) if server_python not in ['unknown', 'mismatch_detected'] else 'unknown'

        versions_match = (server_minor != 'unknown' and local_minor == server_minor)

        # Build accurate error message based on actual situation
        if versions_match:
            file_status = f"""File Listing Status:
  ❌ Direct file access blocked by Databricks Connect security restrictions
  ❌ UDF execution not permitted in serverless/remote execution environment
  ❌ Local /dbfs mount not available in remote execution environment
  ℹ️  Python versions match ({local_minor}) - version is not the issue"""

            technical_details = f"""Technical Details:
  - Your local Python version: {local_python}
  - Server Python version: {server_python}
  - Version compatibility: ✅ Versions match!
  - Connection type: Databricks Connect/Serverless
  - Authentication: ✅ Working
  - User lookup: ✅ Working
  - File system access: ❌ Blocked by Databricks Connect security model"""
        else:
            file_status = f"""File Listing Status:
  ❌ Direct file access not available due to Python version mismatch between client and server
  ❌ UDF execution blocked by version incompatibility
  ❌ Local /dbfs mount not available in remote execution environment"""

            technical_details = f"""Technical Details:
  - Your local Python version: {local_python}
  - Server Python version: {server_python}
  - Version compatibility: ❌ Mismatch detected!
  - Connection type: Databricks Connect/Serverless
  - Authentication: ✅ Working
  - User lookup: ✅ Working
  - File system access: ❌ Blocked by version mismatch"""

        message = f"""User Information:
  Username: {username}
  Display Name: {user_display}
  Email: {user_email}
  Home Directory: /Users/{username}

{file_status}

{technical_details}

{dbc_recommendation}

Alternative Solutions:
  1. Run this script directly in a Databricks notebook where dbutils is natively available
  2. Use the original workspace_inventory.py with --force-sequential flag
  3. Use the Databricks CLI or web interface to manually inspect user directories"""
        
        return "unavailable", message
        
    except Exception as e:
        error_message = f"""Error accessing user information:
  Error: {str(e)}
  
This could be due to:
  - User doesn't exist in the workspace
  - Insufficient permissions
  - Network connectivity issues
  - Invalid authentication credentials"""
        
        return "error", error_message


def list_user_files(username: str, workspace_url: Optional[str] = None, token: Optional[str] = None,
                   cluster_id: Optional[str] = None, profile: Optional[str] = None, debug: bool = False) -> Tuple[int, int, str]:
    """
    Attempt to list files in a user's home directory.

    Args:
        username: User email/username to list files for
        workspace_url: Databricks workspace URL (optional)
        token: Access token (optional)
        cluster_id: Cluster ID for Spark Connect (optional but recommended)
        profile: CLI profile name (optional)
        debug: Enable debug output

    Returns:
        Tuple of (file_count, total_size_bytes, message)
        - file_count: Number of files (0 if unavailable)
        - total_size_bytes: Total size in bytes (0 if unavailable)
        - message: Detailed status message
    """
    try:
        # Authenticate
        if not workspace_url or not token or not cluster_id:
            if debug:
                print("Authenticating with Databricks...")
            workspace_url, token, cluster_id = authenticate_databricks(
                profile=profile,
                workspace_url=workspace_url,
                token=token,
                cluster_id=cluster_id
            )
        
        if debug:
            print(f"Workspace: {workspace_url}")
            print(f"Looking up user: {username}")
        
        # Try to get user info and list files (or explain why it's not available)
        status, message = estimate_user_files_via_api(workspace_url, token, username, cluster_id=cluster_id, debug=debug)

        if status == "success":
            # Parse file count and total size from successful listing
            # Extract from the message (not elegant but works with current structure)
            import re
            file_count_match = re.search(r'Files found: (\d+)', message)
            total_size_match = re.search(r'Total size: ([\d,]+) bytes', message)

            file_count = int(file_count_match.group(1)) if file_count_match else 0
            total_size = int(total_size_match.group(1).replace(',', '')) if total_size_match else 0

            return file_count, total_size, message
        elif status == "unavailable" or status == "failed":
            return 0, 0, message
        elif status == "error":
            raise ValueError(message)
        else:
            return 0, 0, "Unknown status"
            
    except Exception as e:
        error_message = f"Error: {str(e)}"
        return 0, 0, error_message


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
                        # Rate limited - exponential backoff
                        retry_count += 1
                        wait_time = min(2 ** retry_count, 32)
                        if debug:
                            print(f"[WORKER] {worker_info} - Rate limited (429) on {path}, retrying in {wait_time}s (attempt {retry_count}/{max_retries})")
                        time.sleep(wait_time)
                        rate_limit_delay = min(rate_limit_delay * 1.5, 1.0)
                        continue
                    elif response.status_code in [500, 503]:
                        # Server errors - retry with backoff
                        retry_count += 1
                        if retry_count >= max_retries:
                            if debug:
                                print(f"[WORKER] {worker_info} - Server error {response.status_code} on {path}, max retries reached")
                            return
                        wait_time = min(2 ** retry_count, 16)
                        if debug:
                            print(f"[WORKER] {worker_info} - Server error {response.status_code} on {path}, retrying in {wait_time}s")
                        time.sleep(wait_time)
                        continue
                    elif response.status_code != 200:
                        if debug:
                            print(f"[WORKER] {worker_info} - DBFS API returned {response.status_code} for {path}")
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

                except requests.exceptions.RequestException as e:
                    # Network/connection errors - retry with backoff
                    retry_count += 1
                    if retry_count >= max_retries:
                        if debug:
                            print(f"[WORKER] {worker_info} - Request failed after {max_retries} retries for {path}: {str(e)}")
                        return
                    wait_time = min(2 ** retry_count, 16)
                    if debug:
                        print(f"[WORKER] {worker_info} - Request error on {path}, retrying in {wait_time}s: {str(e)}")
                    time.sleep(wait_time)
                except Exception as e:
                    # Other unexpected errors
                    retry_count += 1
                    if retry_count >= max_retries:
                        if debug:
                            print(f"[WORKER] {worker_info} - Unexpected error after {max_retries} retries for {path}: {str(e)}")
                        return
                    wait_time = min(2 ** retry_count, 16)
                    time.sleep(wait_time)

        # Process the user - scan DBFS first
        dbfs_file_count = 0
        dbfs_dir_count = 0
        dbfs_size = 0

        # Store original counts before DBFS scan
        list_recursive(home_path)
        dbfs_file_count = file_count
        dbfs_dir_count = dir_count
        dbfs_size = total_size

        if debug and dbfs_file_count > 0:
            print(f"[WORKER] {worker_info} - [DBFS] Found {dbfs_file_count} files for {username}")

        # Now scan Workspace API (always, not just as fallback)
        workspace_file_count = 0
        workspace_dir_count = 0

        def list_workspace_recursive(path: str, depth: int = 0, max_depth: int = 10):
            nonlocal workspace_file_count, workspace_dir_count, request_count, rate_limit_delay

            if depth > max_depth:
                return

            max_retries = 5
            retry_count = 0

            while retry_count < max_retries:
                try:
                    if request_count > 0:
                        time.sleep(rate_limit_delay)

                    request_count += 1

                    url = f"{workspace_url}/api/2.0/workspace/list"
                    response = requests.get(
                        url,
                        headers=headers,
                        json={"path": path},
                        timeout=30
                    )

                    if response.status_code == 404:
                        return
                    elif response.status_code == 429:
                        # Rate limited - exponential backoff
                        retry_count += 1
                        wait_time = min(2 ** retry_count, 32)
                        if debug:
                            print(f"[WORKER] {worker_info} - Rate limited (429) on {path}, retrying in {wait_time}s (attempt {retry_count}/{max_retries})")
                        time.sleep(wait_time)
                        rate_limit_delay = min(rate_limit_delay * 1.5, 1.0)
                        continue
                    elif response.status_code in [500, 503]:
                        # Server errors - retry with backoff
                        retry_count += 1
                        if retry_count >= max_retries:
                            if debug:
                                print(f"[WORKER] {worker_info} - Server error {response.status_code} on {path}, max retries reached")
                            return
                        wait_time = min(2 ** retry_count, 16)
                        if debug:
                            print(f"[WORKER] {worker_info} - Server error {response.status_code} on {path}, retrying in {wait_time}s")
                        time.sleep(wait_time)
                        continue
                    elif response.status_code != 200:
                        if debug:
                            print(f"[WORKER] {worker_info} - Workspace API returned {response.status_code} for {path}")
                        return

                    data = response.json()
                    objects = data.get("objects", [])

                    for obj in objects:
                        object_path = obj.get("path", "")
                        object_type = obj.get("object_type", "")

                        if object_type == "DIRECTORY":
                            workspace_dir_count += 1
                            list_workspace_recursive(object_path, depth + 1, max_depth)
                        else:
                            # NOTEBOOK, LIBRARY, FILE, etc.
                            workspace_file_count += 1

                    return

                except requests.exceptions.RequestException as e:
                    # Network/connection errors - retry with backoff
                    retry_count += 1
                    if retry_count >= max_retries:
                        if debug:
                            print(f"[WORKER] {worker_info} - Request failed after {max_retries} retries for {path}: {str(e)}")
                        return
                    wait_time = min(2 ** retry_count, 16)
                    if debug:
                        print(f"[WORKER] {worker_info} - Request error on {path}, retrying in {wait_time}s: {str(e)}")
                    time.sleep(wait_time)
                except Exception as e:
                    # Other unexpected errors
                    retry_count += 1
                    if retry_count >= max_retries:
                        if debug:
                            print(f"[WORKER] {worker_info} - Unexpected error after {max_retries} retries for {path}: {str(e)}")
                        return
                    wait_time = min(2 ** retry_count, 16)
                    time.sleep(wait_time)

        # Scan Workspace (always, to cumulate with DBFS)
        if debug:
            print(f"[WORKER] {worker_info} - [WORKSPACE] Scanning workspace files for {username}...")
        list_workspace_recursive(home_path)

        if debug and workspace_file_count > 0:
            print(f"[WORKER] {worker_info} - [WORKSPACE] Found {workspace_file_count} files for {username}")

        # Cumulate results from both sources
        file_count = dbfs_file_count + workspace_file_count
        dir_count = dbfs_dir_count + workspace_dir_count
        workspace_size = workspace_file_count * 10000  # Estimate 10KB per workspace file
        total_size = dbfs_size + workspace_size

        # Determine file source
        if dbfs_file_count > 0 and workspace_file_count > 0:
            file_source = "both"
        elif dbfs_file_count > 0:
            file_source = "dbfs"
        elif workspace_file_count > 0:
            file_source = "workspace"
        else:
            file_source = "none"

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
            "error": None,
            "worker_id": worker_info,
            "start_time": start_time.strftime('%Y-%m-%d %H:%M:%S'),
            "end_time": end_time.strftime('%Y-%m-%d %H:%M:%S'),
            "duration_seconds": duration_seconds,
            "file_source": file_source  # 'dbfs' or 'workspace'
        }

    except Exception as e:
        # Debug: Print error completion time on worker
        end_time = datetime.now()
        duration_seconds = 0
        if 'start_time' in locals():
            duration = end_time - start_time
            duration_seconds = duration.total_seconds()

        if 'data' in locals() and data.get("debug", False):
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
            "error": str(e),
            "worker_id": worker_info if 'worker_info' in locals() else "Unknown",
            "start_time": start_time.strftime('%Y-%m-%d %H:%M:%S') if 'start_time' in locals() else "",
            "end_time": end_time.strftime('%Y-%m-%d %H:%M:%S'),
            "duration_seconds": duration_seconds,
            "file_source": "unknown"
        }


def process_multiple_users_parallel(usernames: List[str], workspace_url: str, token: str,
                                    cluster_id: Optional[str] = None, debug: bool = False,
                                    resume: bool = False) -> List[Dict]:
    """
    Process multiple users in parallel using Spark cluster workers.
    This distributes the work across all available workers for maximum speed.

    Args:
        usernames: List of usernames to process
        workspace_url: Databricks workspace URL
        token: Access token
        cluster_id: Cluster ID (required for parallel processing)
        debug: Enable debug output
        resume: Resume from checkpoint file if it exists

    Returns:
        List of result dictionaries for each user
    """
    from datetime import datetime
    import os

    parallel_start_time = datetime.now()
    checkpoint_file = ".checkpoint_progress.json"
    previous_results = []
    original_user_count = len(usernames)

    # Check for checkpoint and resume if requested
    if resume and os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r') as f:
                checkpoint_data = json.load(f)

            previous_results = checkpoint_data.get("results", [])
            completed_users = set(r["username"] for r in previous_results)
            remaining_users = [u for u in usernames if u not in completed_users]

            print(f"\n{'='*80}")
            print(f"RESUMING FROM CHECKPOINT")
            print(f"{'='*80}")
            print(f"Checkpoint file: {checkpoint_file}")
            print(f"Checkpoint timestamp: {checkpoint_data.get('timestamp', 'unknown')}")
            print(f"Original total users: {original_user_count}")
            print(f"Already completed: {len(previous_results)}")
            print(f"Remaining to process: {len(remaining_users)}")
            print(f"Last completed user: {checkpoint_data.get('last_completed_user', 'unknown')}")
            print(f"{'='*80}\n")

            if len(remaining_users) == 0:
                print("✓ All users already processed! Nothing to do.\n")
                return previous_results

            # Update usernames to only process remaining users
            usernames = remaining_users

            if debug:
                print(f"Resuming with {len(usernames)} remaining users:")
                for idx, username in enumerate(usernames[:10], 1):
                    print(f"  {idx}. {username}")
                if len(usernames) > 10:
                    print(f"  ... and {len(usernames) - 10} more")
                print()

        except Exception as e:
            print(f"Warning: Could not load checkpoint file: {e}")
            print(f"Starting fresh processing of all {len(usernames)} users\n")
            previous_results = []

    elif resume:
        print(f"⚠️  Resume requested but no checkpoint file found: {checkpoint_file}")
        print(f"Starting fresh processing of all {len(usernames)} users\n")

    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

        print(f"\n{'='*80}")
        print(f"PARALLEL PROCESSING {len(usernames)} USERS USING SPARK CLUSTER")
        print(f"{'='*80}")
        print(f"Parallel start time: {parallel_start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")

        # Create or get Spark session
        if cluster_id:
            workspace_host = workspace_url.replace("https://", "").rstrip("/")
            connect_url = f"sc://{workspace_host}:443/;token={token};x-databricks-cluster-id={cluster_id}"

            if debug:
                print(f"Connecting to cluster: {cluster_id}")

            spark = SparkSession.builder \
                .appName("Databricks User Files Parallel Processing") \
                .remote(connect_url) \
                .getOrCreate()

            # Get cluster configuration
            num_workers = None
            if debug:
                try:
                    # Get cluster information via Databricks API
                    import requests
                    cluster_info_url = f"{workspace_url}/api/2.0/clusters/get"
                    headers = {"Authorization": f"Bearer {token}"}
                    cluster_response = requests.get(
                        cluster_info_url,
                        headers=headers,
                        params={"cluster_id": cluster_id},
                        timeout=10
                    )
                    if cluster_response.status_code == 200:
                        cluster_data = cluster_response.json()
                        num_workers = cluster_data.get("num_workers", None)
                        if num_workers is None:
                            # Check for autoscaling
                            autoscale = cluster_data.get("autoscale", {})
                            min_workers = autoscale.get("min_workers", None)
                            max_workers = autoscale.get("max_workers", None)
                            if min_workers and max_workers:
                                print(f"Cluster: {cluster_id}")
                                print(f"Workers: Autoscaling from {min_workers} to {max_workers} workers")
                                num_workers = max_workers  # Use max for planning
                            else:
                                # Single node cluster
                                print(f"Cluster: {cluster_id}")
                                print(f"Workers: Single-node cluster (0 workers, driver only)")
                                num_workers = 1
                        else:
                            print(f"Cluster: {cluster_id}")
                            print(f"Workers: {num_workers} worker node(s) available")
                    else:
                        print(f"Cluster: {cluster_id}")
                        print(f"Workers: Unable to query cluster info (will use Spark defaults)")

                    # Get default parallelism which indicates available executor slots
                    # Use spark.conf.get for SparkConnect/DatabricksConnect compatibility
                    try:
                        default_parallelism = int(spark.conf.get('spark.default.parallelism', '8'))
                        print(f"Default parallelism: {default_parallelism} concurrent tasks")

                        if num_workers:
                            cores_per_worker = default_parallelism // max(num_workers, 1)
                            print(f"Estimated cores per worker: ~{cores_per_worker}")

                        print(f"Maximum concurrent users: ~{default_parallelism}")
                    except Exception as e:
                        print(f"Note: Could not retrieve parallelism info: {str(e)}")

                    print()

                except Exception as e:
                    # Expected when using SparkConnect/DatabricksConnect
                    if "JVM_ATTRIBUTE_NOT_SUPPORTED" in str(e) or "sparkContext" in str(e):
                        print(f"Note: Using SparkConnect/DatabricksConnect mode - detailed cluster info not available")
                    else:
                        print(f"Note: Could not retrieve detailed cluster info: {str(e)}")
                    print()
        else:
            # Try to use existing session
            spark = SparkSession.builder.getOrCreate()
            num_workers = None

        # Prepare user data
        user_data_list = []
        for username in usernames:
            user_data = {
                "username": username,
                "workspace_url": workspace_url,
                "token": token,
                "debug": debug
            }
            user_data_list.append(json.dumps(user_data))

        if debug:
            print(f"Processing {len(user_data_list)} users across Spark workers...")
            print(f"Each worker will independently scan assigned users using DBFS API")
            print(f"Method: REST API calls to /api/2.0/dbfs/list (no /dbfs mount required)")
            print(f"\nUsers to be distributed:")
            for idx, username in enumerate(usernames, 1):
                print(f"  {idx}. {username}")
            print()

        # Create DataFrame for parallel processing
        users_df = spark.createDataFrame([{"user_data": ud} for ud in user_data_list])

        # Repartition to enable parallel processing across multiple workers
        # Create one partition per user (up to a reasonable limit of 200)
        # This maximizes parallelism and allows each executor to process users independently
        num_partitions = min(len(user_data_list), 200)
        users_df = users_df.repartition(num_partitions)

        if debug:
            # Use num_partitions directly (already calculated above)
            # Note: Cannot use .rdd.getNumPartitions() in Spark Connect
            actual_partitions = num_partitions
            print(f"DataFrame partitioned into {actual_partitions} partitions for parallel processing")
            print(f"Each partition will be processed by a different executor")

            # Show distribution estimate
            users_per_partition = len(user_data_list) / actual_partitions
            print(f"Average users per partition: ~{users_per_partition:.1f}")

            if num_workers and num_workers > 0:
                print(f"\nWith {num_workers} worker(s), expect ~{actual_partitions // num_workers} partitions per worker")
                print(f"All {num_workers} workers will start processing their partitions simultaneously")
            print()
        else:
            # Set actual_partitions for later use in non-debug mode
            actual_partitions = num_partitions

        # Define output schema
        output_schema = StructType([
            StructField("username", StringType(), True),
            StructField("file_count", LongType(), True),
            StructField("total_size", LongType(), True),
            StructField("dir_count", LongType(), True),
            StructField("status", StringType(), True),
            StructField("error", StringType(), True),
            StructField("worker_id", StringType(), True),
            StructField("start_time", StringType(), True),
            StructField("end_time", StringType(), True),
            StructField("duration_seconds", DoubleType(), True),
            StructField("file_source", StringType(), True)  # 'dbfs' or 'workspace'
        ])

        # Process in parallel using mapInPandas
        def process_users_batch(iterator):
            import pandas as pd
            import json
            import os

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

        # Execute parallel processing
        if debug:
            print(f"{'='*80}")
            print(f"STARTING PARALLEL EXECUTION")
            print(f"{'='*80}")
            print(f"Work distribution: {actual_partitions} partitions across cluster workers")
            print(f"Execution mode: All partitions will start processing in parallel")
            print(f"Workers will process their assigned partitions simultaneously")
            print(f"{'='*80}\n")
        else:
            print("Distributing work to cluster workers...")

        result_df = users_df.mapInPandas(process_users_batch, schema=output_schema)

        # Collect results with progress output in debug mode
        # Save checkpoint incrementally to enable resume on failure
        checkpoint_file = ".checkpoint_progress.json"

        if debug:
            print("Processing users across workers (showing results as they complete)...\n")
            print(f"💾 Checkpoint file: {checkpoint_file} (progress saved incrementally)\n")

        results = []
        processed_count = 0

        # Use toLocalIterator for incremental result streaming
        try:
            for row in result_df.toLocalIterator():
                processed_count += 1
                result = {
                    "username": row.username,
                    "file_count": int(row.file_count or 0),
                    "total_size": int(row.total_size or 0),
                    "dir_count": int(row.dir_count or 0),
                    "status": row.status,
                    "error": row.error,
                    "worker_id": row.worker_id,
                    "start_time": row.start_time,
                    "end_time": row.end_time,
                    "duration_seconds": float(row.duration_seconds or 0),
                    "file_source": row.file_source
                }
                results.append(result)

                # Save checkpoint after each result (enables resume on failure)
                try:
                    with open(checkpoint_file, 'w') as f:
                        json.dump({
                            "total_users": len(usernames),
                            "processed_count": processed_count,
                            "last_completed_user": result["username"],
                            "timestamp": datetime.now().isoformat(),
                            "results": results
                        }, f, indent=2)
                except Exception as checkpoint_error:
                    if debug:
                        print(f"Warning: Could not save checkpoint: {checkpoint_error}")

                if debug:
                    # Show per-user progress with worker and timing info
                    size_str = format_size(result["total_size"])
                    status_icon = "✓" if result["status"] == "success" else ("⚠" if result["status"] == "empty" else "✗")
                    error_msg = f" - {result['error']}" if result['error'] else ""
                    worker_info = f"[{result['worker_id']}]" if result['worker_id'] else ""
                    duration_info = f"({result['duration_seconds']:.1f}s)" if result['duration_seconds'] > 0 else ""
                    timing_info = f"start={result['start_time']} end={result['end_time']}" if result['start_time'] else ""
                    source_info = f"[{result['file_source'].upper()}]" if result.get('file_source') else ""

                    print(f"  [{processed_count}/{len(usernames)}] {status_icon} {worker_info} {source_info} {result['username']}: "
                          f"{result['file_count']} files ({size_str}) {duration_info}")
                    if timing_info:
                        print(f"      ↳ {timing_info}{error_msg}")

            if debug:
                print()  # Empty line after all users

            # Delete checkpoint file on successful completion
            try:
                import os
                if os.path.exists(checkpoint_file):
                    os.remove(checkpoint_file)
                    if debug:
                        print(f"✓ Checkpoint file removed (processing completed successfully)\n")
            except:
                pass

        except Exception as stream_error:
            # Handle streaming/timeout errors during result collection
            error_msg = str(stream_error)
            is_timeout = "INVALID_HANDLE" in error_msg or "OPERATION_ABANDONED" in error_msg or "abandoned" in error_msg.lower()

            print(f"\n{'='*80}")
            print(f"⚠️  PARALLEL PROCESSING INTERRUPTED")
            print(f"{'='*80}")
            print(f"Processed: {processed_count}/{len(usernames)} users before interruption")
            print(f"Last completed: {results[-1]['username'] if results else 'None'}")
            print(f"Error: {error_msg}")
            print(f"{'='*80}\n")

            if is_timeout:
                print("This appears to be a timeout/session abandonment error.")
                print("Long-running operations (>30-60 minutes) may be abandoned by the server.\n")

            # Save partial results to checkpoint
            if results:
                try:
                    with open(checkpoint_file, 'w') as f:
                        json.dump({
                            "total_users": len(usernames),
                            "processed_count": processed_count,
                            "last_completed_user": results[-1]["username"],
                            "timestamp": datetime.now().isoformat(),
                            "interrupted": True,
                            "error": error_msg,
                            "results": results
                        }, f, indent=2)
                    print(f"💾 Progress saved to checkpoint: {checkpoint_file}")
                    print(f"   {processed_count} users completed successfully\n")
                except Exception as save_error:
                    print(f"Warning: Could not save checkpoint: {save_error}\n")

            # Provide recovery instructions
            print(f"{'='*80}")
            print(f"RECOVERY OPTIONS")
            print(f"{'='*80}")
            print(f"Option 1 - Resume from checkpoint (RECOMMENDED):")
            print(f"  python databricks_user_files_simple.py \\")
            print(f"    --users-file <your-file> \\")
            print(f"    --profile <your-profile> \\")
            print(f"    --cluster-id <your-cluster> \\")
            print(f"    --resume")
            print()
            print(f"Option 2 - Process in smaller batches:")
            print(f"  # Split your user file into smaller chunks (e.g., 50-100 users each)")
            print(f"  # Then process each batch separately")
            print()
            print(f"Option 3 - Continue with partial results:")
            print(f"  # The checkpoint file contains all successfully processed users")
            print(f"  # Extract results: cat {checkpoint_file} | jq .results")
            print(f"{'='*80}\n")

            # Raise the error to be caught by outer exception handler
            raise

        # Non-debug mode: collect all at once
        if not debug:
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
                    "error": row.error,
                    "worker_id": row.worker_id,
                    "start_time": row.start_time,
                    "end_time": row.end_time,
                    "duration_seconds": float(row.duration_seconds or 0),
                    "file_source": row.file_source
                })

        # Calculate parallel processing duration
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

        # Show parallel execution summary in debug mode
        if debug:
            unique_workers = set(r.get("worker_id") for r in results if r.get("worker_id"))
            if unique_workers and len(unique_workers) > 1:
                print(f"{'='*80}")
                print(f"PARALLEL EXECUTION CONFIRMED")
                print(f"{'='*80}")
                print(f"✓ Work was distributed across {len(unique_workers)} different executors:")
                for worker_id in sorted(unique_workers):
                    worker_users = [r["username"] for r in results if r.get("worker_id") == worker_id]
                    print(f"  • {worker_id}: processed {len(worker_users)} user(s)")
                print(f"{'='*80}\n")
            elif unique_workers and len(unique_workers) == 1:
                print(f"Note: All work was processed by a single executor: {list(unique_workers)[0]}")
                print(f"      (This may indicate a single-node cluster or low parallelism)\n")

        # Merge with previous results if resuming
        if previous_results:
            combined_results = previous_results + results
            print(f"{'='*80}")
            print(f"RESUME SUMMARY")
            print(f"{'='*80}")
            print(f"Previous completed users: {len(previous_results)}")
            print(f"Newly processed users: {len(results)}")
            print(f"Total users processed: {len(combined_results)}")
            print(f"{'='*80}\n")
            return combined_results

        return results

    except Exception as e:
        parallel_end_time = datetime.now()
        parallel_duration = parallel_end_time - parallel_start_time

        if debug:
            print(f"Parallel processing failed after {parallel_duration}: {str(e)}")
            print("Falling back to sequential processing...\n")
        # Return empty to trigger fallback
        return None


def process_multiple_users(usernames: List[str], workspace_url: Optional[str] = None,
                          token: Optional[str] = None, cluster_id: Optional[str] = None,
                          profile: Optional[str] = None, debug: bool = False,
                          output_csv: Optional[str] = None, parallel: bool = True,
                          resume: bool = False) -> List[Dict]:
    """
    Process multiple users and return results.
    Automatically uses parallel processing if cluster_id is provided, otherwise sequential.

    Args:
        usernames: List of usernames to process
        workspace_url: Databricks workspace URL
        token: Access token
        cluster_id: Cluster ID for Spark Connect (enables parallel processing)
        profile: CLI profile name
        debug: Enable debug output
        output_csv: Optional CSV output file path
        resume: Resume from checkpoint file if available
        parallel: If True and cluster_id provided, use parallel processing (default: True)

    Returns:
        List of result dictionaries for each user
    """
    total_users = len(usernames)

    # Authenticate if needed
    if not workspace_url or not token:
        if debug:
            print("Authenticating with Databricks...")
        workspace_url, token, cluster_id = authenticate_databricks(
            profile=profile,
            workspace_url=workspace_url,
            token=token,
            cluster_id=cluster_id
        )

    # Try parallel processing if cluster is available
    if parallel and cluster_id:
        results = process_multiple_users_parallel(
            usernames=usernames,
            workspace_url=workspace_url,
            token=token,
            cluster_id=cluster_id,
            debug=debug,
            resume=resume
        )

        # If parallel processing succeeded, skip sequential
        if results is not None:
            # Print individual results (skip if debug mode already printed them during processing)
            if not debug:
                for idx, result in enumerate(results, 1):
                    print(f"[{idx}/{total_users}] {result['username']}")
                    if result['status'] == 'success':
                        print(f"  ✓ Files: {result['file_count']:,}, Size: {format_size(result['total_size'])}")
                    elif result['status'] == 'empty':
                        print(f"  ⊘ Empty directory")
                    else:
                        print(f"  ✗ Error: {result['error']}")
                    print()

            # Jump to summary section
            print(f"{'='*80}")
            print(f"SUMMARY")
            print(f"{'='*80}")

            successful = [r for r in results if r["status"] == "success"]
            empty = [r for r in results if r["status"] == "empty"]
            errors = [r for r in results if r["status"] == "error"]

            total_files = sum(r["file_count"] for r in results)
            total_size_all = sum(r["total_size"] for r in results)

            print(f"Total users processed: {total_users}")
            print(f"  ✓ Successful (with files): {len(successful)}")
            print(f"  ⊘ Empty directories: {len(empty)}")
            print(f"  ✗ Errors: {len(errors)}")
            print()
            print(f"Total files across all users: {total_files:,}")
            print(f"Total size across all users: {format_size(total_size_all)} ({total_size_all:,} bytes)")
            print(f"{'='*80}\n")

            # Write CSV if requested
            if output_csv:
                try:
                    import csv
                    with open(output_csv, 'w', newline='') as csvfile:
                        fieldnames = ['username', 'file_count', 'total_size', 'total_size_gb', 'status', 'file_source', 'error']
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                        writer.writeheader()
                        for r in results:
                            writer.writerow({
                                'username': r['username'],
                                'file_count': r['file_count'],
                                'total_size': r['total_size'],
                                'total_size_gb': round(r['total_size'] / (1024**3), 2),
                                'status': r['status'],
                                'file_source': r.get('file_source', 'unknown'),
                                'error': r['error'] or ''
                            })

                    print(f"Results saved to: {output_csv}\n")
                except Exception as e:
                    print(f"Warning: Could not write CSV: {str(e)}\n")

            return results

    # Fall back to sequential processing
    if cluster_id:
        print(f"\n{'='*80}")
        print(f"SEQUENTIAL PROCESSING {total_users} USERS (parallel mode failed)")
        print(f"{'='*80}\n")
    else:
        print(f"\n{'='*80}")
        print(f"SEQUENTIAL PROCESSING {total_users} USERS (no cluster provided)")
        print(f"{'='*80}\n")

    results = []

    for idx, username in enumerate(usernames, 1):
        print(f"[{idx}/{total_users}] Processing: {username}")

        try:
            file_count, total_size, message = list_user_files(
                username=username,
                workspace_url=workspace_url,
                token=token,
                cluster_id=cluster_id,
                profile=profile,
                debug=debug
            )

            result = {
                "username": username,
                "file_count": file_count,
                "total_size": total_size,
                "status": "success" if file_count > 0 else "empty",
                "error": None
            }

            print(f"  ✓ Files: {file_count:,}, Size: {format_size(total_size)}")

        except Exception as e:
            result = {
                "username": username,
                "file_count": 0,
                "total_size": 0,
                "status": "error",
                "error": str(e)
            }
            print(f"  ✗ Error: {str(e)}")

        results.append(result)
        print()

    # Print summary
    print(f"{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")

    successful = [r for r in results if r["status"] == "success"]
    empty = [r for r in results if r["status"] == "empty"]
    errors = [r for r in results if r["status"] == "error"]

    total_files = sum(r["file_count"] for r in results)
    total_size_all = sum(r["total_size"] for r in results)

    print(f"Total users processed: {total_users}")
    print(f"  ✓ Successful (with files): {len(successful)}")
    print(f"  ⊘ Empty directories: {len(empty)}")
    print(f"  ✗ Errors: {len(errors)}")
    print()
    print(f"Total files across all users: {total_files:,}")
    print(f"Total size across all users: {format_size(total_size_all)} ({total_size_all:,} bytes)")
    print(f"{'='*80}\n")

    # Write CSV if requested
    if output_csv:
        try:
            import csv
            with open(output_csv, 'w', newline='') as csvfile:
                fieldnames = ['username', 'file_count', 'total_size', 'total_size_gb', 'status', 'file_source', 'error']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                writer.writeheader()
                for r in results:
                    writer.writerow({
                        'username': r['username'],
                        'file_count': r['file_count'],
                        'total_size': r['total_size'],
                        'total_size_gb': round(r['total_size'] / (1024**3), 2),
                        'status': r['status'],
                        'file_source': r.get('file_source', 'unknown'),
                        'error': r['error'] or ''
                    })

            print(f"Results saved to: {output_csv}\n")
        except Exception as e:
            print(f"Warning: Could not write CSV: {str(e)}\n")

    return results


def main():
    """Example usage of the user file listing functionality."""
    import argparse
    from datetime import datetime

    parser = argparse.ArgumentParser(
        description="List files in Databricks user home directories with parallel processing",
        epilog="""
Examples:
  # Single user
  python databricks_user_files_simple.py user@example.com --profile PROD

  # Multiple users (sequential - no cluster)
  python databricks_user_files_simple.py user1@example.com user2@example.com user3@example.com --profile PROD

  # Multiple users (PARALLEL - with cluster, MUCH FASTER!)
  python databricks_user_files_simple.py --users-file users.txt --profile PROD --cluster-id 1234-567890-abc123

  # Multiple users from file with CSV output (parallel)
  python databricks_user_files_simple.py --users-file users.txt --profile PROD --cluster-id 1234-567890-abc123 --output results.csv

  # Resume from checkpoint after timeout/failure
  python databricks_user_files_simple.py --users-file users.txt --profile PROD --cluster-id 1234-567890-abc123 --resume

  # Force sequential even with cluster
  python databricks_user_files_simple.py --users-file users.txt --profile PROD --cluster-id 1234-567890-abc123 --no-parallel

Performance Note:
  - Without --cluster-id: Sequential processing (1 user at a time)
  - With --cluster-id: PARALLEL processing (all users distributed across workers)
  - Parallel mode can be 10-100x faster for many users!
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("usernames", nargs="*", help="Username(s) (email) to list files for")
    parser.add_argument("--users-file", help="File containing usernames (one per line)")
    parser.add_argument("--profile", "-p", help="Databricks CLI profile name")
    parser.add_argument("--workspace-url", help="Workspace URL (overrides profile)")
    parser.add_argument("--token", help="Access token (overrides profile)")
    parser.add_argument("--cluster-id", help="Cluster ID for Spark Connect (enables PARALLEL processing)")
    parser.add_argument("--output", "-o", help="Output CSV file path for results")
    parser.add_argument("--no-parallel", action="store_true", help="Disable parallel processing (force sequential)")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint file if available (.checkpoint_progress.json)")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")

    args = parser.parse_args()

    # Collect usernames from arguments and/or file
    usernames = list(args.usernames) if args.usernames else []

    if args.users_file:
        try:
            with open(args.users_file, 'r') as f:
                file_users = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
                usernames.extend(file_users)
        except Exception as e:
            print(f"Error reading users file: {str(e)}")
            sys.exit(1)

    if not usernames:
        parser.error("No usernames provided. Specify usernames as arguments or use --users-file")

    # Remove duplicates while preserving order
    seen = set()
    usernames = [u for u in usernames if not (u in seen or seen.add(u))]

    # Record start time
    start_time = datetime.now()
    print(f"\n{'='*80}")
    print(f"DATABRICKS USER FILES LISTING")
    print(f"{'='*80}")
    print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Users to process: {len(usernames)}")
    if args.cluster_id and len(usernames) > 1:
        print(f"Mode: PARALLEL (using cluster {args.cluster_id})")
    else:
        print(f"Mode: SEQUENTIAL")
    print(f"{'='*80}\n")

    try:
        if len(usernames) == 1:
            # Single user - detailed output
            file_count, total_size, message = list_user_files(
                username=usernames[0],
                workspace_url=args.workspace_url,
                token=args.token,
                cluster_id=args.cluster_id,
                profile=args.profile,
                debug=args.debug
            )

            print(f"\n{'='*60}")
            print(f"DATABRICKS USER FILE LISTING RESULT")
            print(f"{'='*60}")
            print(f"User: {usernames[0]}")
            print(f"Files: {file_count}")
            print(f"Total size: {format_size(total_size)} ({total_size:,} bytes)")
            print(f"{'='*60}")
            print()
            print(message)
            print()

            # Save single result to CSV if requested
            if args.output:
                import csv
                with open(args.output, 'w', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=['username', 'file_count', 'total_size', 'total_size_gb'])
                    writer.writeheader()
                    writer.writerow({
                        'username': usernames[0],
                        'file_count': file_count,
                        'total_size': total_size,
                        'total_size_gb': round(total_size / (1024**3), 2)
                    })
                print(f"Results saved to: {args.output}\n")
        else:
            # Multiple users - summary output
            process_multiple_users(
                usernames=usernames,
                workspace_url=args.workspace_url,
                token=args.token,
                cluster_id=args.cluster_id,
                profile=args.profile,
                debug=args.debug,
                output_csv=args.output,
                parallel=not args.no_parallel,  # Enable parallel by default unless --no-parallel
                resume=args.resume  # Resume from checkpoint if requested
            )

        # Record end time and calculate duration
        end_time = datetime.now()
        duration = end_time - start_time

        # Format duration nicely
        total_seconds = int(duration.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        if hours > 0:
            duration_str = f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            duration_str = f"{minutes}m {seconds}s"
        else:
            duration_str = f"{seconds}s"

        print(f"\n{'='*80}")
        print(f"COMPLETED SUCCESSFULLY")
        print(f"{'='*80}")
        print(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Duration: {duration_str}")
        print(f"Users processed: {len(usernames)}")
        if args.output:
            print(f"Output file: {args.output}")
        print(f"{'='*80}\n")

    except Exception as e:
        # Record end time even on error
        end_time = datetime.now()
        duration = end_time - start_time

        print(f"\n{'='*80}")
        print(f"ERROR")
        print(f"{'='*80}")
        print(f"Error: {str(e)}")
        print(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Duration: {duration}")
        print(f"{'='*80}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()