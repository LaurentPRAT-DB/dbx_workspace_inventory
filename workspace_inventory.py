"""
Databricks Workspace Inventory Application (Optimized Version)

This PySpark application:
1. Lists all users in the workspace
2. Parallelizes the listing of items in each user's home directory using Spark workers
3. Persists the results to a CSV file

This version is optimized for Databricks runtime environment.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, sum as _sum, countDistinct
from pyspark.sql.types import StructType, StructField, StringType, LongType
import argparse
import json
import os
import subprocess
import sys
from typing import List, Dict, Optional
import requests


def get_databricks_users(workspace_url: str, token: str, debug: bool = False, max_users: Optional[int] = None) -> List[Dict]:
    """
    Retrieve users from Databricks workspace using the SCIM API.

    Args:
        workspace_url: Databricks workspace URL (e.g., https://your-workspace.cloud.databricks.com)
        token: Databricks personal access token
        debug: If True, print progress while fetching users (useful when listing is slow)
        max_users: Optional maximum number of users to retrieve (stops early)

    Returns:
        List of user dictionaries containing user information
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    users = []
    start_index = 1
    items_per_page = 100
    
    while True:
        url = f"{workspace_url}/api/2.0/preview/scim/v2/Users"
        params = {
            "startIndex": start_index,
            "count": items_per_page
        }
        
        try:
            if debug:
                print(f"Requesting users: startIndex={start_index}, count={items_per_page}...")

            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            resources = data.get("Resources", [])
            total_results = data.get("totalResults", 0)

            if not resources:
                if debug:
                    print("No more users returned by API.")
                break

            # Append resources one by one so we can stream progress and stop early if requested
            for r in resources:
                users.append(r)
                if debug:
                    name = r.get("userName") or r.get("displayName") or r.get("id") or "unknown"
                    suffix = f"/{total_results}" if total_results else ""
                    print(f"Retrieved user {len(users)}{suffix}: {name}")

                if max_users and len(users) >= max_users:
                    if debug:
                        print(f"Reached max_users={max_users}; stopping early.")
                    break

            # If we reached max_users, break outer loop
            if max_users and len(users) >= max_users:
                break

            # Check if there are more pages
            if total_results and (start_index + items_per_page) > total_results:
                if debug:
                    print(f"Fetched all reported users ({len(users)}/{total_results}).")
                break

            # Brief progress feedback when debug is enabled
            if debug and total_results:
                print(f"Progress: {len(users)}/{total_results} users retrieved so far...")

            start_index += items_per_page
        except Exception as e:
            print(f"Error fetching users: {str(e)}")
            break
    
    return users


def list_directory_recursive(dbutils, base_path: str, user_name: str, max_depth: int = 10, current_depth: int = 0) -> List[Dict]:
    """
    Recursively list all items in a directory.
    
    Args:
        dbutils: Databricks utilities object
        base_path: Base directory path to scan
        user_name: Username for tracking
        max_depth: Maximum recursion depth
        current_depth: Current recursion depth
        
    Returns:
        List of file/directory information dictionaries
    """
    items = []
    
    if current_depth > max_depth:
        return items
    
    try:
        file_list = dbutils.fs.ls(base_path)
        
        for item in file_list:
            item_info = {
                "user_name": user_name,
                "path": item.path.rstrip('/'),
                "name": item.name.rstrip('/'),
                "size": item.size if hasattr(item, 'size') else None,
                "is_directory": item.isDir() if hasattr(item, 'isDir') else False,
                "modification_time": str(item.modificationTime) if hasattr(item, 'modificationTime') else None
            }
            items.append(item_info)
            
            # Recursively list subdirectories
            if item_info["is_directory"]:
                try:
                    sub_items = list_directory_recursive(
                        dbutils, 
                        item.path, 
                        user_name, 
                        max_depth, 
                        current_depth + 1
                    )
                    items.extend(sub_items)
                except Exception as e:
                    # Log error but continue
                    error_item = item_info.copy()
                    error_item["error"] = str(e)
                    items.append(error_item)
                    
    except Exception as e:
        # Return error info if directory can't be accessed
        items.append({
            "user_name": user_name,
            "path": base_path,
            "name": os.path.basename(base_path.rstrip('/')) if base_path else "unknown",
            "size": None,
            "is_directory": None,
            "modification_time": None,
            "error": str(e)
        })
    
    return items


def process_user_directory(user_data: str) -> List[Dict]:
    """
    Process a single user's home directory.
    This function runs on Spark worker nodes.
    
    Args:
        user_data: JSON string containing user information and configuration
        
    Returns:
        List of file/directory information dictionaries
    """
    try:
        data = json.loads(user_data)
        user_info = data["user_info"]
        
        user_name = user_info.get("userName", "unknown")
        user_id = user_info.get("id", "")
        user_display_name = user_info.get("displayName", "")
        user_email = user_info.get("userName", "")
        
        # Construct home directory path
        home_path = f"/Users/{user_name}"
        
        # Get dbutils on worker node
        # In Databricks runtime, dbutils is available on each worker
        try:
            # Try to get dbutils from Spark context
            from pyspark.sql import SparkSession as LocalSparkSession
            local_spark = LocalSparkSession.builder.getOrCreate()
            
            # In Databricks, dbutils should be available
            # Try multiple methods to access it
            try:
                worker_dbutils = local_spark._jvm.com.databricks.service.DBUtils(local_spark._jsc.sc())
            except:
                # Alternative: dbutils might be in the Spark context
                worker_dbutils = getattr(local_spark, 'dbutils', None)
                if worker_dbutils is None:
                    # Last resort: try to import from databricks
                    import sys
                    if 'dbutils' in sys.modules:
                        worker_dbutils = sys.modules['dbutils']
                    else:
                        worker_dbutils = None
            
            if worker_dbutils is None:
                raise Exception("dbutils not available on worker node")
            
            # List all items in the home directory
            items = list_directory_recursive(worker_dbutils, home_path, user_name)
            
            # Add user metadata to each item
            for item in items:
                item["user_id"] = user_id
                item["user_display_name"] = user_display_name
                item["user_email"] = user_email
                
                # Ensure all fields are present
                for field in ["error"]:
                    if field not in item:
                        item[field] = None
            
            return items
            
        except Exception as e:
            # Return error item if dbutils is not available
            return [{
                "user_name": user_name,
                "user_id": user_id,
                "user_display_name": user_display_name,
                "user_email": user_email,
                "path": home_path,
                "name": "home",
                "size": None,
                "is_directory": None,
                "modification_time": None,
                "error": f"dbutils not available: {str(e)}"
            }]
            
    except Exception as e:
        return [{
            "user_name": "unknown",
            "user_id": None,
            "user_display_name": None,
            "user_email": None,
            "path": "unknown",
            "name": "unknown",
            "size": None,
            "is_directory": None,
            "modification_time": None,
            "error": f"Processing error: {str(e)}"
        }]


def get_databricks_cli_config(profile: Optional[str] = None) -> Optional[Dict]:
    """
    Get Databricks configuration from Databricks CLI.
    Supports profile selection from ~/.databrickscfg.
    Tries to use databricks-cli commands first, then falls back to reading config file directly.
    
    Args:
        profile: Optional profile name (e.g., "DEFAULT", "PROD"). 
                 If None, uses DEFAULT or first available profile.
    
    Returns:
        Dictionary with workspace_url, token, and other config if available, None otherwise
    """
    # First, try to use databricks-cli command to get config
    try:
        if profile:
            env = os.environ.copy()
            env["DATABRICKS_CONFIG_PROFILE"] = profile
        else:
            env = os.environ.copy()
            if "DATABRICKS_CONFIG_PROFILE" in env:
                profile = env["DATABRICKS_CONFIG_PROFILE"]
        
        # Try to get config using databricks-cli
        cmd = ["databricks", "configure", "--token", "--show"]
        if profile and profile != "DEFAULT":
            cmd.extend(["--profile", profile])
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=5,
            env=env
        )
        
        if result.returncode == 0 and result.stdout.strip():
            # Parse the output if databricks-cli provides structured output
            # This is a fallback - we'll still read from config file
            pass
    except (FileNotFoundError, subprocess.TimeoutExpired):
        # databricks-cli not available or timed out, continue with file reading
        pass
    except Exception:
        # Other errors, continue with file reading
        pass
    
    # Read configuration from ~/.databrickscfg file
    config_path = os.path.expanduser("~/.databrickscfg")
    
    if not os.path.exists(config_path):
        return None
    
    try:
        config = {}
        profiles = {}
        current_profile = None
        
        with open(config_path, 'r') as f:
            for line in f:
                line = line.strip()
                
                # Skip comments and empty lines
                if not line or line.startswith('#') or line.startswith(';'):
                    continue
                
                # Check for profile section
                if line.startswith('[') and line.endswith(']'):
                    current_profile = line[1:-1].strip()
                    if current_profile not in profiles:
                        profiles[current_profile] = {}
                    continue
                
                # Parse key-value pairs
                if '=' in line and current_profile:
                    key, value = line.split('=', 1)
                    key = key.strip().lower()
                    value = value.strip()
                    profiles[current_profile][key] = value
        
        # Select profile
        target_profile = profile or os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
        
        # Try to find the requested profile, fallback to DEFAULT, then first available
        if target_profile in profiles:
            config = profiles[target_profile]
            used_profile = target_profile
        elif "DEFAULT" in profiles:
            config = profiles["DEFAULT"]
            used_profile = "DEFAULT"
        elif profiles:
            # Use first available profile
            used_profile = list(profiles.keys())[0]
            config = profiles[used_profile]
        else:
            return None
        
        # Convert to standardized format
        result = {}
        if "host" in config:
            host = config["host"]
            result["workspace_url"] = host if host.startswith("https://") else f"https://{host}"
        if "token" in config:
            result["token"] = config["token"]
        if "serverless_compute_id" in config:
            result["serverless_compute_id"] = config["serverless_compute_id"]
        if "cluster_id" in config:
            result["cluster_id"] = config["cluster_id"]
        
        # Store which profile was used
        result["_profile"] = used_profile
        
        # Return if we have at least workspace URL and token
        if "workspace_url" in result and "token" in result:
            return result
            
    except Exception as e:
        print(f"Warning: Could not read Databricks CLI config: {str(e)}")
    
    return None


def get_cluster_info(workspace_url: str, token: str, cluster_id: str) -> Optional[Dict]:
    """
    Get cluster information including capabilities and type (serverless vs traditional).
    
    Args:
        workspace_url: Databricks workspace URL
        token: Databricks personal access token
        cluster_id: Cluster ID to query
        
    Returns:
        Dictionary with cluster information including capabilities
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    try:
        # Get cluster details
        url = f"{workspace_url}/api/2.0/clusters/get"
        response = requests.post(
            url,
            headers=headers,
            json={"cluster_id": cluster_id},
            timeout=30
        )
        response.raise_for_status()
        cluster_info = response.json()
        
        # Extract key information
        result = {
            "cluster_id": cluster_id,
            "cluster_name": cluster_info.get("cluster_name", "unknown"),
            "cluster_source": cluster_info.get("cluster_source", "unknown"),
            "spark_version": cluster_info.get("spark_version", "unknown"),
            "node_type_id": cluster_info.get("node_type_id", "unknown"),
            "driver_node_type_id": cluster_info.get("driver_node_type_id", "unknown"),
            "num_workers": cluster_info.get("num_workers", 0),
            "cluster_memory_mb": cluster_info.get("cluster_memory_mb", 0),
            "cluster_cores": cluster_info.get("cluster_cores", 0),
            "state": cluster_info.get("state", "UNKNOWN"),
            "autotermination_minutes": cluster_info.get("autotermination_minutes", None),
        }
        
        # Detect if serverless
        # Serverless clusters have specific characteristics
        spark_conf = cluster_info.get("spark_conf", {})
        custom_tags = cluster_info.get("custom_tags", {})
        
        is_serverless = (
            "spark.databricks.cluster.profile" in spark_conf and 
            spark_conf.get("spark.databricks.cluster.profile") == "serverless"
        ) or (
            "ResourceClass" in custom_tags and 
            custom_tags.get("ResourceClass") == "Serverless"
        ) or (
            "Serverless" in cluster_info.get("cluster_name", "").lower()
        )
        
        result["is_serverless"] = is_serverless
        
        # Detect capabilities
        runtime_version = cluster_info.get("spark_version", "")
        capabilities = {
            "spark_connect": "spark.connect.enabled" in spark_conf or 
                           any("connect" in k.lower() for k in spark_conf.keys()),
            "delta_lake": True,  # Databricks clusters support Delta by default
            "photon": "photon" in runtime_version.lower() or 
                     "spark.databricks.photon.enabled" in spark_conf,
            "serverless": is_serverless,
        }
        
        result["capabilities"] = capabilities
        
        return result
        
    except Exception as e:
        print(f"Warning: Could not fetch cluster info: {str(e)}")
        return None


def profile_cluster_capabilities(workspace_url: str, token: str, cluster_id: Optional[str] = None) -> Dict:
    """
    Profile cluster capabilities and return a summary.
    
    Args:
        workspace_url: Databricks workspace URL
        token: Databricks personal access token
        cluster_id: Optional cluster ID. If not provided, tries to detect from environment.
        
    Returns:
        Dictionary with cluster capability profile
    """
    if not cluster_id:
        cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID") or os.environ.get("DATABRICKS_SERVERLESS_COMPUTE_ID")
    
    if not cluster_id:
        return {
            "error": "No cluster ID provided",
            "recommended_mode": "spark_connect",
            "serverless_available": False
        }
    
    # Check if serverless compute ID
    is_serverless_id = cluster_id.lower() == "auto" or "serverless" in cluster_id.lower()
    
    if is_serverless_id or cluster_id.lower() == "auto":
        return {
            "cluster_id": "auto (serverless)",
            "is_serverless": True,
            "capabilities": {
                "spark_connect": True,
                "delta_lake": True,
                "serverless": True,
                "auto_scaling": True,
            },
            "recommended_session_type": "databricks_connect_serverless",
            "limitations": [
                "No R language support",
                "Limited Spark RDD APIs",
                "User-defined function (UDF) restrictions",
                "Session timeout after 10 minutes of inactivity"
            ]
        }
    
    # Get detailed cluster info for traditional clusters
    cluster_info = get_cluster_info(workspace_url, token, cluster_id)
    
    if not cluster_info:
        return {
            "cluster_id": cluster_id,
            "error": "Could not fetch cluster information",
            "recommended_mode": "spark_connect",
        }
    
    # Build capability profile
    capabilities = cluster_info.get("capabilities", {})
    profile = {
        "cluster_id": cluster_id,
        "cluster_name": cluster_info.get("cluster_name", "unknown"),
        "is_serverless": cluster_info.get("is_serverless", False),
        "state": cluster_info.get("state", "UNKNOWN"),
        "spark_version": cluster_info.get("spark_version", "unknown"),
        "num_workers": cluster_info.get("num_workers", 0),
        "capabilities": capabilities,
    }
    
    # Determine recommended session type
    if profile["is_serverless"]:
        profile["recommended_session_type"] = "databricks_connect_serverless"
    elif capabilities.get("spark_connect"):
        profile["recommended_session_type"] = "spark_connect"
    else:
        profile["recommended_session_type"] = "native_databricks"
    
    return profile


def create_serverless_session(workspace_url: str, token: str, expected_python: Optional[str] = None, dbc_version: Optional[str] = None) -> SparkSession:
    """
    Create a Spark session for Databricks serverless compute using Databricks Connect SDK.
    
    Args:
        workspace_url: Databricks workspace URL
        token: Databricks personal access token
    
    Returns:
        SparkSession configured for serverless compute
    """
    try:
        # Try to use Databricks Connect SDK for serverless
        from databricks.connect import DatabricksSession
        import os
        
        # Set environment variables for token-based authentication
        # Remove https:// prefix if present for DATABRICKS_HOST
        host = workspace_url.replace("https://", "").rstrip("/")
        
        # Explicitly set token-based authentication environment variables
        # These take precedence over OAuth/CLI authentication
        os.environ["DATABRICKS_HOST"] = host
        os.environ["DATABRICKS_TOKEN"] = token
        os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"
        
        # If the caller provided an expected Python, print a helpful hint
        if expected_python:
            print(f"Requested expected server Python: {expected_python}")

        # Create session - the SDK should use DATABRICKS_TOKEN from environment
        # Note: If this fails with OAuth errors, the user may need to reauthenticate
        # or ensure their ~/.databrickscfg uses token-based auth, not OAuth
        spark = DatabricksSession.builder.getOrCreate()
        
        return spark
        
    except ImportError:
        if dbc_version:
            raise ImportError(
                "Databricks Connect SDK is required for serverless compute.\n"
                f"Install the matching version with: pip install databricks-connect=={dbc_version}\n"
                "Note: Version must match your Databricks Runtime version."
            )
        raise ImportError(
            "Databricks Connect SDK is required for serverless compute.\n"
            "Install it with: pip install databricks-connect\n"
            "Note: Version must match your Databricks Runtime version."
        )
    except Exception as e:
        error_msg = str(e)
        # Provide helpful error message
        if "cannot get access token" in error_msg or "refresh token" in error_msg or "databricks-cli" in error_msg:
            raise RuntimeError(
                f"Authentication failed: The SDK is trying to use OAuth but the token is invalid.\n\n"
                "Solutions:\n"
                "  1. Reauthenticate: databricks auth login --host <workspace-url>\n"
                "  2. Or use explicit token: python workspace_inventory.py --token <your-token> --serverless\n"
                "  3. Or ensure ~/.databrickscfg has a valid token (not OAuth) configuration\n"
                f"\nError details: {error_msg}"
            )
        raise RuntimeError(
            f"Failed to create serverless session: {error_msg}\n"
            "Make sure:\n"
            "  1. DATABRICKS_SERVERLESS_COMPUTE_ID is set to 'auto'\n"
            "  2. Valid token is provided (via --token, environment variable, or ~/.databrickscfg)\n"
            "  3. The databricks-connect version matches your Databricks Runtime version"
        )


def create_spark_connect_session(workspace_url: str, token: str, cluster_id: Optional[str] = None, profile: Optional[Dict] = None, expected_python: Optional[str] = None, dbc_version: Optional[str] = None) -> SparkSession:
    """
    Create a Spark Connect session to a Databricks cluster.
    Supports both traditional clusters and serverless compute.
    
    Args:
        workspace_url: Databricks workspace URL
        token: Databricks personal access token
        cluster_id: Optional cluster ID to connect to. If "auto", uses serverless.
        
    Returns:
        SparkSession configured for Spark Connect or serverless
    """
    # Get cluster ID from environment or parameter
    if not cluster_id:
        cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID") or os.environ.get("DATABRICKS_SERVERLESS_COMPUTE_ID", "")
    
    # Handle serverless compute
    if cluster_id and cluster_id.lower() == "auto" or os.environ.get("DATABRICKS_SERVERLESS_COMPUTE_ID", "").lower() == "auto":
        print("Detected serverless compute configuration, using Databricks Connect SDK...")
        return create_serverless_session(workspace_url, token, expected_python=expected_python, dbc_version=dbc_version)
    
    if not cluster_id:
        raise ValueError(
            "DATABRICKS_CLUSTER_ID or DATABRICKS_SERVERLESS_COMPUTE_ID environment variable must be set.\n"
            "For serverless: set DATABRICKS_SERVERLESS_COMPUTE_ID=auto\n"
            "For traditional cluster: set DATABRICKS_CLUSTER_ID=<cluster-id>"
        )
    
    # Remove https:// if present for connection string
    workspace_host = workspace_url.replace("https://", "").rstrip("/")
    
    # Build Spark Connect URL
    # Format: sc://workspace_host:443/;token=token;x-databricks-cluster-id=cluster_id
    connect_url = f"sc://{workspace_host}:443/;token={token};x-databricks-cluster-id={cluster_id}"
    
    spark = SparkSession.builder \
        .appName("Databricks Workspace Inventory") \
        .remote(connect_url) \
        .getOrCreate()

    # Determine server Python (use explicit expected_python if provided, else try to infer)
    server_python = None
    try:
        if expected_python:
            server_python = expected_python
        elif profile and profile.get('spark_version'):
            server_python = infer_python_from_spark_version(profile.get('spark_version'))

        if server_python:
            local_py = f"{sys.version_info.major}.{sys.version_info.minor}"
            print(f"Detected (or requested) server Python: {server_python}; local Python: {local_py}")
            if server_python != local_py:
                print("Warning: Python minor version differs between client and server.\n"
                      "  - mapInPandas and other Python UDFs may fail when running via Spark Connect.\n"
                      "  - Recommended: match the client's Python minor version to the server, or run with --force-sequential to avoid remote Python execution.")
        else:
            print("Could not infer server Python version from spark_version; consult Databricks release notes if needed.")
    except Exception:
        server_python = None

    # If caller specified a databricks-connect version, print guidance
    if dbc_version:
        print(f"Note: using databricks-connect version suggestion: {dbc_version} (install with pip if needed)")

    return spark


def get_dbutils(spark):
    """
    Get dbutils object from Spark session.
    Works in both Databricks notebooks and jobs, and with Spark Connect.
    """
    try:
        # Try to get from global namespace (notebooks)
        import IPython
        return IPython.get_ipython().user_ns.get('dbutils')
    except:
        try:
            # Try to create from Spark context (jobs and Spark Connect)
            return spark._jvm.com.databricks.service.DBUtils(spark._jsc.sc())
        except:
            # For Spark Connect, dbutils might not be directly available
            # We'll use alternative methods in worker functions
            return None


def infer_python_from_spark_version(spark_version: Optional[str]) -> Optional[str]:
    """
    Heuristically infer Databricks Python minor version from a cluster `spark_version` string.

    This is best-effort and may be inaccurate for some runtimes; consult Databricks
    release notes for authoritative mapping.

    Returns a string like '3.10' or None if unknown.
    """
    if not spark_version:
        return None

    # Typical spark_version examples: '13.1.x-scala2.12', '12.2.x-scala2.12', '11.3.x'
    try:
        prefix = spark_version.split('.')[0]
        # Some runtimes are two-part like '13.1', use first token
        major_token = prefix.strip()
        mapping = {
            # These are approximate mappings — verify against Databricks release notes.
            '10': '3.8',
            '11': '3.9',
            '12': '3.10',
            '13': '3.11',
            '14': '3.11',
        }

        if major_token.isdigit() and major_token in mapping:
            return mapping[major_token]

        # fallback: look for explicit python mention
        low = spark_version.lower()
        if 'python3.10' in low or 'py3.10' in low:
            return '3.10'
        if 'python3.9' in low or 'py3.9' in low:
            return '3.9'
        if 'python3.11' in low or 'py3.11' in low:
            return '3.11'
    except Exception:
        return None

    return None


def parse_arguments():
    """
    Parse command-line arguments.
    
    Returns:
        argparse.Namespace with parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Databricks Workspace Inventory - List all users and their home directory contents",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use default profile from ~/.databrickscfg
  python workspace_inventory.py

  # Use specific profile
  python workspace_inventory.py --profile PROD

  # Override with environment variables
  DATABRICKS_CLUSTER_ID=cluster-123 python workspace_inventory.py --profile DEV

  # Use serverless compute
  python workspace_inventory.py --profile PROD --serverless

  # Specify output location
  python workspace_inventory.py --profile DEFAULT --output /dbfs/inventory --format parquet
        """
    )
    
    parser.add_argument(
        "--profile",
        "-p",
        type=str,
        default=None,
        help="Databricks CLI profile name from ~/.databrickscfg (default: DEFAULT or first available)"
    )
    
    parser.add_argument(
        "--workspace-url",
        type=str,
        default=None,
        help="Databricks workspace URL (overrides profile and environment variable)"
    )
    
    parser.add_argument(
        "--token",
        type=str,
        default=None,
        help="Databricks personal access token (overrides profile and environment variable)"
    )
    
    parser.add_argument(
        "--cluster-id",
        type=str,
        default=None,
        help="Databricks cluster ID for Spark Connect (overrides profile and environment variable)"
    )
    
    parser.add_argument(
        "--serverless",
        action="store_true",
        help="Use serverless compute (sets DATABRICKS_SERVERLESS_COMPUTE_ID=auto)"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output path for results (default: /tmp/workspace_inventory)"
    )
    
    parser.add_argument(
        "--format",
        choices=["csv", "parquet", "delta"],
        default=None,
        help="Output format (default: csv)"
    )

    parser.add_argument(
        "--expected-python",
        type=str,
        default=None,
        help="Expected Python version on the server (e.g. '3.11' or full '3.11.14')."
    )

    parser.add_argument(
        "--databricks-connect-version",
        type=str,
        default=None,
        help="Explicit databricks-connect pip version to use (e.g. '16.1.7')."
    )
    
    parser.add_argument(
        "--no-spark-connect",
        action="store_true",
        help="Disable Spark Connect mode (use native Databricks runtime)"
    )
    
    parser.add_argument(
        "--list-profiles",
        action="store_true",
        help="List available Databricks CLI profiles and exit"
    )
    
    parser.add_argument(
        "--max-user",
        "--max-users",
        dest="max_user",
        type=int,
        default=None,
        help="Maximum number of users to retrieve from the workspace (default: all)"
    )

    parser.add_argument(
        "--force-sequential",
        action="store_true",
        help="Force sequential processing on the driver (skip RDD and mapInPandas paths)"
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output (prints progress while fetching users)"
    )
    
    return parser.parse_args()


def list_available_profiles():
    """List all available profiles from ~/.databrickscfg."""
    config_path = os.path.expanduser("~/.databrickscfg")
    
    if not os.path.exists(config_path):
        print("No ~/.databrickscfg file found.")
        print("Configure it with: databricks configure --token")
        return
    
    try:
        profiles = []
        current_profile = None
        
        with open(config_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('[') and line.endswith(']'):
                    current_profile = line[1:-1].strip()
                    profiles.append(current_profile)
        
        if profiles:
            print("Available Databricks CLI profiles:")
            for profile in profiles:
                marker = " (default)" if profile == "DEFAULT" else ""
                print(f"  - {profile}{marker}")
        else:
            print("No profiles found in ~/.databrickscfg")
    except Exception as e:
        print(f"Error reading profiles: {str(e)}")


def main(args=None):
    """
    Main function to orchestrate the workspace inventory process.
    Supports both native Databricks runtime and local execution via Spark Connect.
    
    Args:
        args: Optional argparse.Namespace with command-line arguments
    """
    # Parse arguments if not provided
    if args is None:
        args = parse_arguments()
    
    # Handle list-profiles option
    if args.list_profiles:
        list_available_profiles()
        sys.exit(0)
    
    # Get profile from arguments or environment
    cli_profile = args.profile or os.environ.get("DATABRICKS_CONFIG_PROFILE")
    
    # Try to get credentials from Databricks CLI first (highest priority for auth)
    cli_config = get_databricks_cli_config(profile=cli_profile)
    
    # Get configuration with priority: command-line args > CLI config > environment variables
    workspace_url = (
        args.workspace_url or 
        (cli_config.get("workspace_url") if cli_config else None) or 
        os.environ.get("DATABRICKS_WORKSPACE_URL", "")
    )
    
    databricks_token = (
        args.token or 
        (cli_config.get("token") if cli_config else None) or 
        os.environ.get("DATABRICKS_TOKEN", "")
    )
    
    output_path = args.output or os.environ.get("OUTPUT_PATH", "/tmp/workspace_inventory")
    output_format = (args.format or os.environ.get("OUTPUT_FORMAT", "csv")).lower()
    
    # Handle cluster/serverless configuration
    if args.serverless:
        os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"
        os.environ.pop("DATABRICKS_CLUSTER_ID", None)  # Remove cluster ID if set
    elif args.cluster_id:
        os.environ["DATABRICKS_CLUSTER_ID"] = args.cluster_id
        os.environ.pop("DATABRICKS_SERVERLESS_COMPUTE_ID", None)
    elif cli_config:
        # Get cluster/serverless ID from CLI config if available
        if "cluster_id" in cli_config and not os.environ.get("DATABRICKS_CLUSTER_ID"):
            os.environ["DATABRICKS_CLUSTER_ID"] = cli_config["cluster_id"]
        if "serverless_compute_id" in cli_config and not os.environ.get("DATABRICKS_SERVERLESS_COMPUTE_ID"):
            os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = cli_config["serverless_compute_id"]
    
    # Show which configuration source is being used
    if cli_config:
        used_profile = cli_config.get("_profile", cli_profile or "DEFAULT")
        print(f"Using Databricks CLI configuration (profile: {used_profile})")
        if args.workspace_url or args.token:
            print("Note: Some settings overridden by command-line arguments")
    
    # Detect execution mode
    use_spark_connect = not args.no_spark_connect
    is_databricks_runtime = os.environ.get("DATABRICKS_RUNTIME_VERSION") is not None
    
    # If not explicitly set and not in Databricks runtime, try Spark Connect
    if use_spark_connect and not is_databricks_runtime:
        # Check if we should use Spark Connect (if cluster ID or serverless is set)
        if os.environ.get("DATABRICKS_CLUSTER_ID") or os.environ.get("DATABRICKS_SERVERLESS_COMPUTE_ID"):
            print("Spark Connect mode enabled")
        elif not is_databricks_runtime:
            # Auto-enable if not in Databricks runtime
            use_spark_connect = True
    
    # Validate configuration
    if not workspace_url or not databricks_token:
        raise ValueError(
            "DATABRICKS_WORKSPACE_URL and DATABRICKS_TOKEN must be set.\n"
            "Options:\n"
            "  1. Set environment variables:\n"
            "     export DATABRICKS_WORKSPACE_URL='https://your-workspace.cloud.databricks.com'\n"
            "     export DATABRICKS_TOKEN='your-token'\n"
            "  2. Use Databricks CLI: databricks configure --token\n"
            "  3. For Spark Connect, also set: export DATABRICKS_CLUSTER_ID='cluster-id'"
        )
    
    # Profile cluster capabilities before connecting
    cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID") or os.environ.get("DATABRICKS_SERVERLESS_COMPUTE_ID", "")
    if use_spark_connect and cluster_id:
        print("\n" + "="*60)
        print("CLUSTER CAPABILITY PROFILING")
        print("="*60)
        try:
            profile = profile_cluster_capabilities(workspace_url, databricks_token, cluster_id)
            
            if "error" not in profile:
                print(f"Cluster ID: {profile.get('cluster_id', 'unknown')}")
                print(f"Cluster Name: {profile.get('cluster_name', 'N/A')}")
                print(f"Type: {'Serverless' if profile.get('is_serverless') else 'Traditional'}")
                
                if profile.get('spark_version'):
                    print(f"Spark Version: {profile.get('spark_version')}")
                
                if profile.get('num_workers') is not None:
                    print(f"Workers: {profile.get('num_workers', 0)}")
                
                print(f"\nCapabilities:")
                capabilities = profile.get('capabilities', {})
                for cap, enabled in capabilities.items():
                    status = "✓" if enabled else "✗"
                    print(f"  {status} {cap.replace('_', ' ').title()}")
                
                if profile.get('is_serverless') and 'limitations' in profile:
                    print(f"\nServerless Limitations:")
                    for limitation in profile['limitations']:
                        print(f"  • {limitation}")
                
                print(f"\nRecommended Session Type: {profile.get('recommended_session_type', 'unknown')}")
            else:
                print(f"Could not profile cluster: {profile.get('error', 'unknown error')}")
        except Exception as e:
            print(f"Warning: Could not profile cluster capabilities: {str(e)}")
        print("="*60 + "\n")
    
    # Initialize Spark session
    if use_spark_connect:
        print("Initializing Spark Connect session...")
        spark = create_spark_connect_session(
            workspace_url,
            databricks_token,
            cluster_id=cluster_id,
            profile=profile if 'profile' in locals() else None,
            expected_python=args.expected_python,
            dbc_version=args.databricks_connect_version
        )
        session_type = "Serverless" if cluster_id and cluster_id.lower() == "auto" else "Spark Connect"
        print(f"✓ Connected to Databricks cluster via {session_type}")
    else:
        print("Initializing native Spark session...")
        spark = SparkSession.builder \
            .appName("Databricks Workspace Inventory") \
            .getOrCreate()
    
    # Get Databricks utilities
    dbutils = get_dbutils(spark)
    
    if dbutils is None and not use_spark_connect:
        print("Warning: dbutils not available. Some features may be limited.")
    
    print(f"\nStarting workspace inventory for: {workspace_url}")
    print(f"Output path: {output_path}")
    print(f"Output format: {output_format}")
    
    # Step 1: Get all users from the workspace
    print("\n[Step 1] Fetching users from workspace...")
    users = get_databricks_users(workspace_url, databricks_token, debug=args.debug, max_users=args.max_user)
    print(f"Found {len(users)} users in the workspace")

    if not users:
        print("No users found in the workspace. Exiting.")
        spark.stop()
        return

    # If requested, limit the number of users to process
    if args.max_user and args.max_user > 0:
        print(f"Limiting to first {args.max_user} users")

    # Step 2: Parallelize user processing using Spark
    # Prepare user data with configuration for worker nodes
    user_data_list = []
    limit = args.max_user if args.max_user and args.max_user > 0 else None
    current = 0
    total_available = len(users)

    for user in users:
        current += 1
        if limit and current > limit:
            break

        user_name = user.get("userName") or user.get("displayName") or user.get("id") or "unknown"
        display_total = limit if limit else total_available
        print(f"Retrieved user {current}/{display_total}: {user_name}")

        user_data = {
            "user_info": user,
            "workspace_url": workspace_url
        }
        user_data_list.append(json.dumps(user_data))

    print(f"\n[Step 2] Processing {len(user_data_list)} user home directories in parallel...")

    # Create RDD and distribute work across Spark workers
    num_slices = max(1, len(user_data_list))

    # Spark Connect sessions do not expose `sparkContext` (JVM attribute). In that case
    # fall back to sequential local processing (slower) so the script can still run.
    items_list = None
    try:
        sc = spark.sparkContext
        users_rdd = sc.parallelize(user_data_list, numSlices=num_slices)
        # Process each user's directory in parallel
        items_rdd = users_rdd.flatMap(process_user_directory)
        # Collect results from RDD
        items_list = items_rdd.collect()
    except Exception as e:
        # Most likely: Spark Connect / Databricks Connect where sparkContext is not supported
        print("Warning: could not use sparkContext (likely Spark Connect). Falling back to sequential processing. This will be slower.")
        if args.debug:
            print(f"Debug: sparkContext error: {str(e)}")

        # First try to use DataFrame + mapInPandas which is supported by Spark Connect
        skip_map = bool(args.force_sequential)
        if skip_map and args.debug:
            print("Skipping mapInPandas and RDD paths due to --force-sequential. Using sequential processing.")

        if not skip_map:
            try:
                # Create a small DataFrame with one column containing JSON strings of user_info
                users_json = [{"user_json": json.dumps(json.loads(ud) if isinstance(ud, str) else ud)} for ud in user_data_list]
                users_df = spark.createDataFrame(users_json)

                # Define the schema for the output rows (same as final schema)
                output_schema = StructType([
                    StructField("user_name", StringType(), True),
                    StructField("user_id", StringType(), True),
                    StructField("user_display_name", StringType(), True),
                    StructField("user_email", StringType(), True),
                    StructField("path", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("size", LongType(), True),
                    StructField("is_directory", StringType(), True),
                    StructField("modification_time", StringType(), True),
                    StructField("error", StringType(), True)
                ])

                def map_process_user(iterator):
                    import pandas as _pd
                    import json as _json

                    for pdf in iterator:
                        rows = []
                        for user_json in pdf['user_json']:
                            try:
                                user = _json.loads(user_json)
                                # Build payload expected by process_user_directory (JSON string)
                                payload = _json.dumps({"user_info": user, "workspace_url": workspace_url})
                                res = process_user_directory(payload)
                                if res:
                                    rows.extend(res)
                            except Exception as _ex:
                                rows.append({
                                    "user_name": "unknown",
                                    "user_id": None,
                                    "user_display_name": None,
                                    "user_email": None,
                                    "path": "unknown",
                                    "name": "unknown",
                                    "size": None,
                                    "is_directory": None,
                                    "modification_time": None,
                                    "error": str(_ex)
                                })

                        if rows:
                            yield _pd.DataFrame(rows)
                        else:
                            # yield empty frame with correct columns to satisfy contract
                            yield _pd.DataFrame(columns=[f.name for f in output_schema.fields])

                # Run in parallel on executors
                result_df = users_df.mapInPandas(map_process_user, schema=output_schema)

                # Collect results back to driver as list of dicts
                items_rows = result_df.collect()
                items_list = [r.asDict() for r in items_rows]
            except Exception as e2:
                if args.debug:
                    print(f"mapInPandas path failed: {str(e2)}")

                # Fall back to sequential local processing
                skip_map = True

        if skip_map:
            # Final fallback: sequential local processing
            items_list = []
            for idx, ud in enumerate(user_data_list, start=1):
                if args.debug:
                    print(f"Sequentially processing user {idx}/{len(user_data_list)}")
                try:
                    res = process_user_directory(ud)
                    if res:
                        items_list.extend(res)
                except Exception as ex:
                    print(f"Error processing user data sequentially: {str(ex)}")
    
    # Step 3: Convert to DataFrame
    print("\n[Step 3] Converting results to DataFrame...")
    
    # Define schema
    schema = StructType([
        StructField("user_name", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_display_name", StringType(), True),
        StructField("user_email", StringType(), True),
        StructField("path", StringType(), True),
        StructField("name", StringType(), True),
        StructField("size", LongType(), True),
        StructField("is_directory", StringType(), True),
        StructField("modification_time", StringType(), True),
        StructField("error", StringType(), True)
    ])
    
    # If we have a distributed DataFrame (result_df from mapInPandas), use it directly
    if 'result_df' in locals():
        df = result_df
    else:
        # Otherwise we should have items_list collected from RDD or sequential path
        if not items_list:
            print("No items found in any user home directory. Exiting.")
            spark.stop()
            return

        df = spark.createDataFrame(items_list, schema=schema)
    
    # Step 4: Generate summary statistics
    print("\n[Step 4] Generating summary statistics...")
    
    # Compute summary statistics in one distributed aggregation to avoid multiple shuffles
    agg = df.agg(
        _sum(lit(1)).alias("total_items"),
        countDistinct(col("user_name")).alias("total_users"),
        _sum(when((col("is_directory") == False) | (col("is_directory") == "false"), 1).otherwise(0)).alias("file_count"),
        _sum(when((col("is_directory") == True) | (col("is_directory") == "true"), 1).otherwise(0)).alias("dir_count"),
        _sum(when(((col("is_directory") == False) | (col("is_directory") == "false")) & col("error").isNull(), col("size")).otherwise(0)).alias("total_size")
    )

    agg_row = agg.collect()[0]
    total_items = int(agg_row["total_items"] or 0)
    total_users = int(agg_row["total_users"] or 0)
    file_count = int(agg_row["file_count"] or 0)
    dir_count = int(agg_row["dir_count"] or 0)
    total_size = int(agg_row["total_size"] or 0)
    
    print(f"\n{'='*60}")
    print(f"INVENTORY SUMMARY")
    print(f"{'='*60}")
    print(f"  Total users processed: {total_users}")
    print(f"  Total items found: {total_items}")
    print(f"    - Files: {file_count}")
    print(f"    - Directories: {dir_count}")
    print(f"  Total size: {total_size:,} bytes ({total_size / (1024**3):.2f} GB)")
    print(f"{'='*60}\n")
    
    # Step 5: Persist results
    print(f"[Step 5] Persisting results to {output_path}...")
    
    if output_format == "csv":
        # Write as single CSV file
        df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .option("delimiter", ",") \
            .csv(output_path)
        
        # In Databricks, CSV files are written as part-*.csv
        # Find the actual file
        if dbutils:
            try:
                files = dbutils.fs.ls(output_path)
                csv_files = [f.path for f in files if f.name.endswith('.csv')]
                if csv_files:
                    print(f"Results saved to: {csv_files[0]}")
            except:
                print(f"Results saved to: {output_path}/part-*.csv")
        else:
            print(f"Results saved to: {output_path}/part-*.csv")
            
    elif output_format == "parquet":
        df.coalesce(1).write \
            .mode("overwrite") \
            .parquet(output_path)
        print(f"Results saved to: {output_path}")
        
    elif output_format == "delta":
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(output_path)
        print(f"Results saved to: {output_path} (Delta format)")
    else:
        raise ValueError(f"Unsupported output format: {output_format}. Use 'csv', 'parquet', or 'delta'")
    
    # Step 6: Display sample results
    print("\n[Step 6] Sample results (first 10 rows):")
    df.show(10, truncate=False)
    
    spark.stop()
    print("\n✓ Workspace inventory completed successfully!")


if __name__ == "__main__":
    main()

