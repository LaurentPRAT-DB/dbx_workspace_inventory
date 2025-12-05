"""
Databricks User Home Directory File Listing Module

Extracted from workspace_inventory.py - provides functionality to list all files in a specific
user's home directory with comprehensive authentication options and multiple access methods.

Features:
- Multiple authentication methods (CLI profiles, environment variables, dbutils)
- Recursive directory traversal with configurable depth
- Multiple file system access methods (dbutils, /dbfs mount, Hadoop FS)
- File count and total size calculation
- Comprehensive error handling and fallbacks

Usage:
    from databricks_user_files import list_user_files, authenticate_databricks
    
    # Using CLI profile
    workspace_url, token = authenticate_databricks(profile="PROD")
    file_count, total_size, files = list_user_files("john.doe@company.com", workspace_url, token)
    
    # Using environment variables
    file_count, total_size, files = list_user_files("jane.smith@company.com")
"""

import json
import os
import subprocess
import sys
from typing import List, Dict, Optional, Tuple
import requests


def get_databricks_cli_config(profile: Optional[str] = None) -> Optional[Dict]:
    """
    Get Databricks configuration from Databricks CLI.
    Supports profile selection from ~/.databrickscfg.
    """
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
        if "cluster_id" in config:
            result["cluster_id"] = config["cluster_id"]
        if "serverless_compute_id" in config:
            result["serverless_compute_id"] = config["serverless_compute_id"]
        
        # Store which profile was used
        result["_profile"] = used_profile
        
        # Return if we have at least workspace URL and token
        if "workspace_url" in result and "token" in result:
            return result
            
    except Exception as e:
        print(f"Warning: Could not read Databricks CLI config: {str(e)}")
    
    return None


def authenticate_databricks(profile: Optional[str] = None, workspace_url: Optional[str] = None, token: Optional[str] = None) -> tuple[str, str]:
    """
    Authenticate with Databricks using multiple methods with priority order.
    
    Priority (highest to lowest):
    1. Explicit parameters (workspace_url, token)
    2. Databricks CLI profile from ~/.databrickscfg
    3. Environment variables
    4. dbutils context (when running in Databricks runtime)
    """
    final_workspace_url = workspace_url
    final_token = token
    
    # Try to get credentials from Databricks CLI profile
    if not final_workspace_url or not final_token:
        cli_config = get_databricks_cli_config(profile=profile)
        
        if cli_config:
            if not final_workspace_url:
                final_workspace_url = cli_config.get("workspace_url")
            if not final_token:
                final_token = cli_config.get("token")
            
            used_profile = cli_config.get("_profile", profile or "DEFAULT")
            print(f"Using Databricks CLI configuration (profile: {used_profile})")
    
    # Fall back to environment variables
    if not final_workspace_url:
        final_workspace_url = os.environ.get("DATABRICKS_WORKSPACE_URL", "")
    if not final_token:
        final_token = os.environ.get("DATABRICKS_TOKEN", "")
    
    # Try to get from dbutils context if running in Databricks runtime
    is_databricks_runtime = os.environ.get("DATABRICKS_RUNTIME_VERSION") is not None
    if (not final_workspace_url or not final_token) and is_databricks_runtime:
        try:
            from pyspark.sql import SparkSession as _SparkSession
            _spark = _SparkSession.builder.getOrCreate()
            _dbutils = None
            
            try:
                import IPython
                _dbutils = IPython.get_ipython().user_ns.get('dbutils')
            except Exception:
                _dbutils = None

            if _dbutils is None:
                try:
                    _dbutils = _spark._jvm.com.databricks.service.DBUtils(_spark._jsc.sc())
                except Exception:
                    _dbutils = None

            if _dbutils is not None:
                try:
                    ctx = _dbutils.notebook().getContext()
                    if not final_workspace_url:
                        try:
                            api_url = ctx.apiUrl().get()
                            if api_url:
                                final_workspace_url = api_url.rstrip('/')
                                print("Using workspace URL from dbutils context")
                        except Exception:
                            pass
                    
                    if not final_token:
                        try:
                            api_token = ctx.apiToken().get()
                            if api_token:
                                final_token = api_token
                                print("Using token from dbutils context")
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception:
            pass
    
    # Validate that we have both required values
    if not final_workspace_url or not final_token:
        if not is_databricks_runtime:
            raise ValueError(
                "DATABRICKS_WORKSPACE_URL and DATABRICKS_TOKEN must be set.\n"
                "Options:\n"
                "  1. Set environment variables\n"
                "  2. Use Databricks CLI: databricks configure --token\n"
                "  3. Pass explicit parameters to authenticate_databricks()"
            )
        else:
            print("Warning: Running inside Databricks runtime but could not obtain full authentication.")
            if not final_workspace_url:
                raise ValueError("Could not determine workspace URL from any source")
            if not final_token:
                raise ValueError("Could not determine token from any source")
    
    return final_workspace_url, final_token


def create_serverless_session(workspace_url: str, token: str) -> object:
    """
    Create a Spark session for Databricks serverless compute using Databricks Connect SDK.
    """
    try:
        from databricks.connect import DatabricksSession
        
        # Set environment variables for token-based authentication
        host = workspace_url.replace("https://", "").rstrip("/")
        os.environ["DATABRICKS_HOST"] = host
        os.environ["DATABRICKS_TOKEN"] = token
        os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"
        
        # Create session
        spark = DatabricksSession.builder.getOrCreate()
        return spark
        
    except ImportError:
        raise ImportError(
            "Databricks Connect SDK is required for serverless compute.\n"
            "Install it with: pip install databricks-connect\n"
            "Note: Version must match your Databricks Runtime version."
        )
    except Exception as e:
        error_msg = str(e)
        if "cannot get access token" in error_msg or "refresh token" in error_msg:
            raise RuntimeError(
                f"Authentication failed: The SDK is trying to use OAuth but the token is invalid.\n\n"
                "Solutions:\n"
                "  1. Reauthenticate: databricks auth login --host <workspace-url>\n"
                "  2. Or use explicit token authentication\n"
                f"\nError details: {error_msg}"
            )
        raise RuntimeError(f"Failed to create serverless session: {error_msg}")


def create_spark_connect_session(workspace_url: str, token: str, cluster_id: str) -> object:
    """
    Create a Spark Connect session to a Databricks cluster.
    """
    # Remove https:// if present for connection string
    workspace_host = workspace_url.replace("https://", "").rstrip("/")
    
    # Build Spark Connect URL
    connect_url = f"sc://{workspace_host}:443/;token={token};x-databricks-cluster-id={cluster_id}"
    
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("Databricks User File Listing") \
            .remote(connect_url) \
            .getOrCreate()
        
        return spark
    except Exception as e:
        raise RuntimeError(f"Failed to create Spark Connect session: {str(e)}")


def get_spark_session(workspace_url: Optional[str] = None, token: Optional[str] = None, 
                     cluster_id: Optional[str] = None, use_serverless: bool = False):
    """
    Get or create a Spark session for accessing dbutils.
    Supports both Databricks runtime and remote connections via Databricks Connect.
    """
    # Check if we're in Databricks runtime
    is_databricks_runtime = os.environ.get("DATABRICKS_RUNTIME_VERSION") is not None
    
    if is_databricks_runtime:
        # In Databricks runtime, use standard SparkSession
        try:
            from pyspark.sql import SparkSession
            
            # Try to get existing session first
            try:
                spark = SparkSession.getActiveSession()
                if spark:
                    return spark
            except:
                pass
            
            # Create new session
            spark = SparkSession.builder \
                .appName("Databricks User File Listing") \
                .getOrCreate()
            
            return spark
        except Exception as e:
            print(f"Warning: Could not create Spark session in Databricks runtime: {str(e)}")
            return None
    
    # For local/remote execution, use Databricks Connect
    if not workspace_url or not token:
        raise ValueError("workspace_url and token are required for remote Spark sessions")
    
    if use_serverless or cluster_id == "auto":
        # Use serverless compute
        return create_serverless_session(workspace_url, token)
    elif cluster_id:
        # Use specific cluster
        return create_spark_connect_session(workspace_url, token, cluster_id)
    else:
        # Try serverless as fallback
        try:
            print("No cluster ID specified, trying serverless compute...")
            return create_serverless_session(workspace_url, token)
        except Exception as e:
            print(f"Serverless failed: {str(e)}")
            raise ValueError(
                "Either cluster_id must be specified or serverless compute must be available.\n"
                "Set cluster_id in your CLI profile or environment variable DATABRICKS_CLUSTER_ID"
            )


def get_dbutils(spark=None):
    """
    Get dbutils object from Spark session.
    Works in both Databricks notebooks, jobs, and Databricks Connect sessions.
    """
    try:
        # Try to get from global namespace (notebooks)
        import IPython
        dbutils = IPython.get_ipython().user_ns.get('dbutils')
        if dbutils:
            return dbutils
    except:
        pass
    
    if spark:
        try:
            # For Databricks Connect sessions, dbutils is available through spark.dbutils
            if hasattr(spark, 'dbutils'):
                return spark.dbutils
        except:
            pass
        
        try:
            # Try to create from Spark context (traditional method)
            return spark._jvm.com.databricks.service.DBUtils(spark._jsc.sc())
        except:
            pass
    
    # For Databricks Connect, return None and let the function use alternative methods
    return None


class SparkConnectDbUtils:
    """
    A wrapper class that provides dbutils-like functionality for Spark Connect sessions.
    Uses Spark SQL and DataFrame operations to replicate dbutils.fs functionality.
    """
    
    def __init__(self, spark):
        self.spark = spark
        self.fs = SparkConnectFileSystem(spark)


class SparkConnectFileSystem:
    """
    A file system interface that uses Python UDFs to replicate dbutils.fs functionality.
    """
    
    def __init__(self, spark):
        self.spark = spark
    
    def ls(self, path: str):
        """
        List files and directories using Spark SQL with Databricks file functions.
        """
        try:
            # Use Databricks SQL function LIST() which should work in serverless/Connect mode
            # Try different approaches that work with Databricks Connect
            
            # Approach 1: Use Spark SQL with file listing capability 
            try:
                df = self.spark.sql(f"""
                    SELECT 
                        path,
                        name,
                        size,
                        is_directory,
                        modification_time
                    FROM 
                        dbfs_list('{path}')
                """)
                rows = df.collect()
                
                result = []
                for row in rows:
                    file_info = MockFileInfo(
                        path=row.path,
                        name=row.name,
                        size=row.size,
                        is_dir=row.is_directory,
                        modification_time=row.modification_time
                    )
                    result.append(file_info)
                
                return result
            except:
                pass
            
            # Approach 2: Try using a Python function that can be executed remotely
            from pyspark.sql import functions as F
            from pyspark.sql.types import ArrayType, StructType, StructField, StringType, LongType, BooleanType
            
            # Define schema for file info
            file_schema = StructType([
                StructField("path", StringType(), True),
                StructField("name", StringType(), True),
                StructField("size", LongType(), True),
                StructField("is_directory", BooleanType(), True),
                StructField("modification_time", LongType(), True),
                StructField("error", StringType(), True)
            ])
            
            def remote_ls(path_str):
                """Function that runs on remote cluster with dbutils access"""
                try:
                    import os
                    # Try to access files via Python os module first (works in some cases)
                    if os.path.exists(f"/dbfs{path_str}"):
                        files = []
                        for item in os.listdir(f"/dbfs{path_str}"):
                            full_path = os.path.join(f"/dbfs{path_str}", item)
                            try:
                                stat = os.stat(full_path)
                                files.append({
                                    "path": f"{path_str.rstrip('/')}/{item}",
                                    "name": item,
                                    "size": stat.st_size if os.path.isfile(full_path) else None,
                                    "is_directory": os.path.isdir(full_path),
                                    "modification_time": int(stat.st_mtime * 1000),
                                    "error": None
                                })
                            except Exception as e:
                                files.append({
                                    "path": f"{path_str.rstrip('/')}/{item}",
                                    "name": item,
                                    "size": None,
                                    "is_directory": None,
                                    "modification_time": None,
                                    "error": str(e)
                                })
                        return files
                    else:
                        return [{"path": path_str, "name": "error", "size": None, 
                                "is_directory": None, "modification_time": None, 
                                "error": f"Path does not exist: {path_str}"}]
                except Exception as e:
                    return [{"path": path_str, "name": "error", "size": None, 
                            "is_directory": None, "modification_time": None, 
                            "error": str(e)}]
            
            # Create UDF
            remote_ls_udf = F.udf(remote_ls, ArrayType(file_schema))
            
            # Execute UDF
            df = self.spark.createDataFrame([(path,)], ["path"])
            result_df = df.select(F.explode(remote_ls_udf(F.col("path"))).alias("file_info"))
            result_df = result_df.select("file_info.*")
            
            rows = result_df.collect()
            
            # Convert to MockFileInfo objects
            result = []
            for row in rows:
                file_info = MockFileInfo(
                    path=row.path,
                    name=row.name,
                    size=row.size,
                    is_dir=row.is_directory,
                    modification_time=row.modification_time
                )
                if row.error:
                    file_info.error = row.error
                result.append(file_info)
            
            return result
            
        except Exception as e:
            raise Exception(f"Failed to list files in {path}: {str(e)}")


class MockFileInfo:
    """
    Mock file info object to replicate the structure returned by dbutils.fs.ls()
    """
    
    def __init__(self, path, name, size, is_dir, modification_time):
        self.path = path
        self.name = name
        self.size = size
        self._is_dir = is_dir
        self.modificationTime = modification_time
        self.error = None
    
    def isDir(self):
        return self._is_dir


def list_directory_recursive_dbutils(dbutils, base_path: str, max_depth: int = 10, current_depth: int = 0) -> List[Dict]:
    """
    Recursively list all items in a directory using dbutils.
    """
    items = []
    
    if current_depth > max_depth:
        return items
    
    try:
        file_list = dbutils.fs.ls(base_path)
        
        for item in file_list:
            item_info = {
                "path": item.path.rstrip('/'),
                "name": item.name.rstrip('/'),
                "size": item.size if hasattr(item, 'size') else None,
                "is_directory": item.isDir() if hasattr(item, 'isDir') else False,
                "modification_time": str(item.modificationTime) if hasattr(item, 'modificationTime') else None,
                "error": None
            }
            items.append(item_info)
            
            # Recursively list subdirectories
            if item_info["is_directory"]:
                try:
                    sub_items = list_directory_recursive_dbutils(
                        dbutils, 
                        item.path, 
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
            "path": base_path,
            "name": os.path.basename(base_path.rstrip('/')) if base_path else "unknown",
            "size": None,
            "is_directory": None,
            "modification_time": None,
            "error": str(e)
        })
    
    return items


def list_directory_recursive_dbfs_mount(base_path: str, max_depth: int = 10, current_depth: int = 0) -> List[Dict]:
    """
    Recursively list all items in a directory using /dbfs mount.
    """
    items = []
    
    if current_depth > max_depth:
        return items
    
    # Convert to local /dbfs path
    if base_path.startswith("/dbfs"):
        dbfs_path = base_path
    elif base_path.startswith('/'):
        dbfs_path = f"/dbfs{base_path}"
    else:
        dbfs_path = f"/dbfs/{base_path}"
    
    try:
        if not os.path.exists(dbfs_path):
            return [{
                "path": base_path,
                "name": os.path.basename(base_path.rstrip('/')),
                "size": None,
                "is_directory": None,
                "modification_time": None,
                "error": f"Path does not exist: {base_path}"
            }]
        
        for root, dirs, files in os.walk(dbfs_path):
            # Process files
            for fname in files:
                full = os.path.join(root, fname)
                try:
                    stat = os.stat(full)
                    rel_path = full.replace('/dbfs', '')
                    items.append({
                        "path": rel_path,
                        "name": fname,
                        "size": stat.st_size,
                        "is_directory": False,
                        "modification_time": str(int(stat.st_mtime * 1000)),
                        "error": None
                    })
                except Exception as e:
                    items.append({
                        "path": full.replace('/dbfs', ''),
                        "name": fname,
                        "size": None,
                        "is_directory": False,
                        "modification_time": None,
                        "error": str(e)
                    })
            
            # Process directories
            for dname in dirs:
                full = os.path.join(root, dname)
                try:
                    stat = os.stat(full)
                    rel_path = full.replace('/dbfs', '')
                    items.append({
                        "path": rel_path,
                        "name": dname,
                        "size": None,
                        "is_directory": True,
                        "modification_time": str(int(stat.st_mtime * 1000)),
                        "error": None
                    })
                except Exception as e:
                    items.append({
                        "path": full.replace('/dbfs', ''),
                        "name": dname,
                        "size": None,
                        "is_directory": True,
                        "modification_time": None,
                        "error": str(e)
                    })
            
            # Limit recursion depth
            if current_depth >= max_depth:
                dirs.clear()  # Don't recurse further
            else:
                current_depth += 1
                
    except Exception as e:
        items.append({
            "path": base_path,
            "name": os.path.basename(base_path.rstrip('/')) if base_path else "unknown",
            "size": None,
            "is_directory": None,
            "modification_time": None,
            "error": str(e)
        })
    
    return items


def list_user_files(username: str, workspace_url: Optional[str] = None, token: Optional[str] = None, 
                   profile: Optional[str] = None, cluster_id: Optional[str] = None, 
                   use_serverless: bool = False, max_depth: int = 10, debug: bool = False) -> Tuple[int, int, List[Dict]]:
    """
    List all files in a user's home directory and return count, total size, and file list.
    
    Args:
        username: Username (email) to list files for
        workspace_url: Databricks workspace URL (optional if using other auth methods)
        token: Databricks token (optional if using other auth methods)
        profile: Databricks CLI profile name (optional)
        cluster_id: Cluster ID for Spark Connect (optional, can be set in profile or env)
        use_serverless: Use serverless compute instead of cluster
        max_depth: Maximum recursion depth for directory traversal
        debug: Enable debug output
        
    Returns:
        Tuple of (file_count, total_size_bytes, files_list)
        
    Raises:
        ValueError: If authentication fails or user directory cannot be accessed
    """
    # Authenticate if not provided
    if not workspace_url or not token:
        if debug:
            print("Authenticating with Databricks...")
        workspace_url, token = authenticate_databricks(profile=profile, workspace_url=workspace_url, token=token)
    
    # Get cluster configuration from CLI profile or environment if not provided
    if not cluster_id and not use_serverless:
        cli_config = get_databricks_cli_config(profile=profile)
        if cli_config:
            if "cluster_id" in cli_config:
                cluster_id = cli_config["cluster_id"]
                if debug:
                    print(f"Using cluster ID from profile: {cluster_id}")
            elif "serverless_compute_id" in cli_config:
                use_serverless = True
                if debug:
                    print("Using serverless compute from profile")
        
        # Check environment variables
        if not cluster_id and not use_serverless:
            cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID")
            if cluster_id and debug:
                print(f"Using cluster ID from environment: {cluster_id}")
            elif os.environ.get("DATABRICKS_SERVERLESS_COMPUTE_ID") == "auto":
                use_serverless = True
                if debug:
                    print("Using serverless compute from environment")
    
    # Construct home directory path
    home_path = f"/Users/{username}"
    
    if debug:
        print(f"Listing files for user: {username}")
        print(f"Home directory: {home_path}")
    
    # Try multiple methods to access the directory
    files = []
    
    # Method 1: Try using Spark Connect with UDF-based file access
    try:
        if debug:
            print("Creating Spark session for remote access...")
        
        spark = get_spark_session(
            workspace_url=workspace_url, 
            token=token, 
            cluster_id=cluster_id, 
            use_serverless=use_serverless
        )
        
        if spark:
            if debug:
                print(f"Spark session created successfully: {type(spark)}")
                
            # Check if this is a Spark Connect session
            is_connect_session = "connect" in str(type(spark)).lower()
            
            if is_connect_session:
                if debug:
                    print("Using Spark Connect session - trying UDF-based file access...")
                
                # Use UDF approach for Spark Connect
                from pyspark.sql import functions as F
                from pyspark.sql.types import ArrayType, StructType, StructField, StringType, LongType, BooleanType
                
                # Define schema for file info
                file_schema = StructType([
                    StructField("path", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("size", LongType(), True),
                    StructField("is_directory", BooleanType(), True),
                    StructField("modification_time", LongType(), True),
                    StructField("error", StringType(), True)
                ])
                
                def remote_ls(path_str):
                    """Function that runs on remote cluster to list files"""
                    try:
                        import os
                        files = []
                        
                        # Convert to dbfs path
                        dbfs_path = f"/dbfs{path_str}" if not path_str.startswith("/dbfs") else path_str
                        
                        if os.path.exists(dbfs_path):
                            for item in os.listdir(dbfs_path):
                                item_path = os.path.join(dbfs_path, item)
                                try:
                                    stat = os.stat(item_path)
                                    is_dir = os.path.isdir(item_path)
                                    files.append({
                                        "path": f"{path_str.rstrip('/')}/{item}",
                                        "name": item,
                                        "size": None if is_dir else stat.st_size,
                                        "is_directory": is_dir,
                                        "modification_time": int(stat.st_mtime * 1000),
                                        "error": None
                                    })
                                except Exception as e:
                                    files.append({
                                        "path": f"{path_str.rstrip('/')}/{item}",
                                        "name": item,
                                        "size": None,
                                        "is_directory": None,
                                        "modification_time": None,
                                        "error": str(e)
                                    })
                        else:
                            files.append({
                                "path": path_str,
                                "name": "error",
                                "size": None,
                                "is_directory": None,
                                "modification_time": None,
                                "error": f"Directory not found: {path_str}"
                            })
                        
                        return files
                    except Exception as e:
                        return [{
                            "path": path_str,
                            "name": "error", 
                            "size": None,
                            "is_directory": None,
                            "modification_time": None,
                            "error": str(e)
                        }]
                
                # Create UDF and execute
                remote_ls_udf = F.udf(remote_ls, ArrayType(file_schema))
                df = spark.createDataFrame([(home_path,)], ["path"])
                result_df = df.select(F.explode(remote_ls_udf(F.col("path"))).alias("file_info"))
                result_df = result_df.select("file_info.*")
                
                rows = result_df.collect()
                
                # Convert to expected format
                files = []
                for row in rows:
                    files.append({
                        "path": row.path,
                        "name": row.name,
                        "size": row.size,
                        "is_directory": row.is_directory,
                        "modification_time": str(row.modification_time) if row.modification_time else None,
                        "error": row.error
                    })
                
                # Filter out errors if we got some valid files
                valid_files = [f for f in files if not f.get('error')]
                if valid_files:
                    files = valid_files
                    if debug:
                        print(f"Successfully listed {len(files)} items using Spark Connect UDF")
                else:
                    if debug:
                        print("Spark Connect UDF returned only errors")
                    files = []
            else:
                # Try traditional dbutils approach for non-Connect sessions
                dbutils = get_dbutils(spark)
                if dbutils:
                    if debug:
                        print("Using traditional dbutils for directory listing...")
                    files = list_directory_recursive_dbutils(dbutils, home_path, max_depth=max_depth)
                    if files and not all(f.get('error') for f in files):
                        if debug:
                            print(f"Successfully listed {len(files)} items using dbutils")
                    else:
                        files = []
                else:
                    if debug:
                        print("Could not get dbutils from Spark session")
                    files = []
        else:
            if debug:
                print("Failed to create Spark session")
            files = []
    except Exception as e:
        if debug:
            print(f"Remote Spark method failed: {str(e)}")
            import traceback
            traceback.print_exc()
        files = []
    
    # Method 2: Try using /dbfs mount (fallback - only works if running on Databricks infrastructure)
    if not files:
        try:
            if debug:
                print("Trying /dbfs mount method...")
            files = list_directory_recursive_dbfs_mount(home_path, max_depth=max_depth)
            if files and not all(f.get('error') for f in files):
                if debug:
                    print(f"Successfully listed {len(files)} items using /dbfs mount")
            else:
                files = []
        except Exception as e:
            if debug:
                print(f"/dbfs mount method failed: {str(e)}")
            files = []
    
    # If all methods failed, return error
    if not files or all(f.get('error') for f in files):
        error_msg = f"Could not access home directory for user {username}"
        if files and files[0].get('error'):
            error_msg += f": {files[0]['error']}"
        
        # Provide helpful error message for common issues
        if not cluster_id and not use_serverless:
            error_msg += "\n\nTip: Specify a cluster ID or enable serverless compute:"
            error_msg += "\n  - Add cluster_id to your ~/.databrickscfg profile"
            error_msg += "\n  - Set DATABRICKS_CLUSTER_ID environment variable"
            error_msg += "\n  - Use --cluster-id parameter"
            error_msg += "\n  - Use --serverless flag"
        
        raise ValueError(error_msg)
    
    # Calculate statistics
    file_count = 0
    total_size = 0
    
    for item in files:
        if not item.get('error') and not item.get('is_directory'):
            file_count += 1
            if item.get('size') is not None:
                total_size += item['size']
    
    if debug:
        dir_count = sum(1 for item in files if not item.get('error') and item.get('is_directory'))
        print(f"\nSummary:")
        print(f"  Files: {file_count}")
        print(f"  Directories: {dir_count}")
        print(f"  Total size: {total_size:,} bytes ({total_size / (1024**2):.2f} MB)")
    
    return file_count, total_size, files


def format_size(size_bytes: int) -> str:
    """Format byte size in human readable format."""
    if size_bytes == 0:
        return "0 B"
    
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"


def main_example():
    """Example usage of the user file listing functionality."""
    import argparse
    
    parser = argparse.ArgumentParser(description="List files in a Databricks user's home directory")
    parser.add_argument("username", help="Username (email) to list files for")
    parser.add_argument("--profile", "-p", help="Databricks CLI profile name")
    parser.add_argument("--workspace-url", help="Workspace URL (overrides profile)")
    parser.add_argument("--token", help="Access token (overrides profile)")
    parser.add_argument("--cluster-id", help="Cluster ID for Spark Connect")
    parser.add_argument("--serverless", action="store_true", help="Use serverless compute")
    parser.add_argument("--max-depth", type=int, default=10, help="Maximum directory recursion depth")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    parser.add_argument("--show-files", action="store_true", help="Show individual file listings")
    
    args = parser.parse_args()
    
    try:
        # Get file listing
        file_count, total_size, files = list_user_files(
            username=args.username,
            workspace_url=args.workspace_url,
            token=args.token,
            profile=args.profile,
            cluster_id=args.cluster_id,
            use_serverless=args.serverless,
            max_depth=args.max_depth,
            debug=args.debug
        )
        
        # Display results
        print(f"\nFile listing for user: {args.username}")
        print(f"Files found: {file_count}")
        print(f"Total size: {format_size(total_size)} ({total_size:,} bytes)")
        
        if args.show_files and files:
            print(f"\nFile details:")
            for item in files:
                if not item.get('error') and not item.get('is_directory'):
                    size_str = format_size(item['size']) if item.get('size') else "unknown"
                    print(f"  {item['path']} ({size_str})")
                
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main_example()