"""
Databricks User Listing Module

Extracted from workspace_inventory.py - contains all user listing and authentication functionality.
This module provides comprehensive user discovery via SCIM API with multiple authentication options.

Features:
- List all users in a Databricks workspace
- Export email addresses to CSV (one per line)
- Multiple authentication methods (CLI profiles, environment variables, dbutils)
- Debug mode with progress tracking
- Pagination support for large workspaces

Usage:
    from databricks_user_list import get_all_users, authenticate_databricks

    # Using CLI profile
    workspace_url, token = authenticate_databricks(profile="PROD")
    users = get_all_users(workspace_url, token, debug=True)

    # Using environment variables
    workspace_url, token = authenticate_databricks()
    users = get_all_users(workspace_url, token, max_users=10)

    # Command-line usage:
    # List all users
    python databricks_user_list.py --profile PROD

    # Export email addresses to CSV (one per line)
    python databricks_user_list.py --profile PROD --output users.csv

    # Limit to first 50 users
    python databricks_user_list.py --profile PROD --max-users 50 --output users.csv
"""

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
    # If running inside Databricks runtime and token/workspace_url are not provided,
    # attempt to obtain them from the DBUtils notebook context (best-effort).
    users = []
    start_index = 1
    items_per_page = 100

    # Try to recover workspace_url and token from dbutils when missing
    if (not workspace_url or not token) and os.environ.get("DATABRICKS_RUNTIME_VERSION"):
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
                    # dbutils.notebook().getContext() exposes API URL and token in Databricks notebooks
                    ctx = _dbutils.notebook().getContext()
                    api_url = None
                    api_token = None
                    try:
                        api_url = ctx.apiUrl().get()
                    except Exception:
                        api_url = None
                    try:
                        api_token = ctx.apiToken().get()
                    except Exception:
                        api_token = None

                    if api_url and not workspace_url:
                        workspace_url = api_url.rstrip('/')
                        if debug:
                            print(f"In-cluster: inferred workspace_url={workspace_url} from dbutils context")
                    if api_token and not token:
                        token = api_token
                        if debug:
                            print("In-cluster: obtained API token from dbutils context")
                except Exception:
                    # Best-effort; continue with whatever we have
                    pass
        except Exception:
            # If any unexpected error occurs, continue to fallback behavior
            pass

    # If still missing workspace_url or token, and not in Databricks runtime, the caller
    # should have raised earlier. Here we try to proceed but will error on requests.
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    headers["Content-Type"] = "application/json"

    while True:
        if not workspace_url:
            raise RuntimeError("Workspace URL is unknown; cannot call SCIM API.")

        url = f"{workspace_url}/api/2.0/preview/scim/v2/Users"
        params = {"startIndex": start_index, "count": items_per_page}

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

            if max_users and len(users) >= max_users:
                break

            if total_results and (start_index + items_per_page) > total_results:
                if debug:
                    print(f"Fetched all reported users ({len(users)}/{total_results}).")
                break

            if debug and total_results:
                print(f"Progress: {len(users)}/{total_results} users retrieved so far...")

            start_index += items_per_page
        except Exception as e:
            print(f"Error fetching users: {str(e)}")
            break

    return users


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


def list_available_profiles():
    """List all available profiles from ~/.databrickscfg."""
    config_path = os.path.expanduser("~/.databrickscfg")
    
    if not os.path.exists(config_path):
        print("No ~/.databrickscfg file found.")
        print("Configure it with: databricks configure --token")
        return []
    
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
            
        return profiles
    except Exception as e:
        print(f"Error reading profiles: {str(e)}")
        return []


def authenticate_databricks(profile: Optional[str] = None, workspace_url: Optional[str] = None, token: Optional[str] = None) -> tuple[str, str]:
    """
    Authenticate with Databricks using multiple methods with priority order.
    
    Priority (highest to lowest):
    1. Explicit parameters (workspace_url, token)
    2. Databricks CLI profile from ~/.databrickscfg
    3. Environment variables
    4. dbutils context (when running in Databricks runtime)
    
    Args:
        profile: Optional profile name from ~/.databrickscfg
        workspace_url: Explicit workspace URL (overrides all other sources)
        token: Explicit token (overrides all other sources)
        
    Returns:
        Tuple of (workspace_url, token)
        
    Raises:
        ValueError: If authentication cannot be established
    """
    final_workspace_url = workspace_url
    final_token = token
    
    # Try to get credentials from Databricks CLI profile
    if not final_workspace_url or not final_token:
        cli_profile = profile or os.environ.get("DATABRICKS_CONFIG_PROFILE")
        cli_config = get_databricks_cli_config(profile=cli_profile)
        
        if cli_config:
            if not final_workspace_url:
                final_workspace_url = cli_config.get("workspace_url")
            if not final_token:
                final_token = cli_config.get("token")
            
            used_profile = cli_config.get("_profile", cli_profile or "DEFAULT")
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
                "  1. Set environment variables:\n"
                "     export DATABRICKS_WORKSPACE_URL='https://your-workspace.cloud.databricks.com'\n"
                "     export DATABRICKS_TOKEN='your-token'\n"
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


def get_all_users(workspace_url: str, token: str, debug: bool = False, max_users: Optional[int] = None) -> List[Dict]:
    """
    Convenience function to get all users from a Databricks workspace.
    
    Args:
        workspace_url: Databricks workspace URL
        token: Databricks personal access token
        debug: Enable debug output
        max_users: Maximum number of users to retrieve
        
    Returns:
        List of user dictionaries
    """
    return get_databricks_users(workspace_url, token, debug=debug, max_users=max_users)


# Example usage functions
def main_example():
    """Example usage of the user listing functionality."""
    import argparse
    from datetime import datetime

    parser = argparse.ArgumentParser(
        description="List Databricks workspace users",
        epilog="""
Examples:
  # List all users
  python databricks_user_list.py --profile PROD

  # Save email addresses to CSV (one per line)
  python databricks_user_list.py --profile PROD --output users.csv

  # Limit to first 50 users
  python databricks_user_list.py --profile PROD --max-users 50 --output users.csv
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--profile", "-p", help="Databricks CLI profile name")
    parser.add_argument("--workspace-url", help="Workspace URL (overrides profile)")
    parser.add_argument("--token", help="Access token (overrides profile)")
    parser.add_argument("--output", "-o", help="Output CSV file path (saves email addresses, one per line)")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    parser.add_argument("--max-users", type=int, help="Maximum number of users to retrieve")
    parser.add_argument("--list-profiles", action="store_true", help="List available profiles")

    args = parser.parse_args()

    if args.list_profiles:
        list_available_profiles()
        return

    # Record start time
    start_time = datetime.now()
    print(f"\n{'='*80}")
    print(f"DATABRICKS USER LISTING")
    print(f"{'='*80}")
    print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    try:
        # Authenticate
        workspace_url, token = authenticate_databricks(
            profile=args.profile,
            workspace_url=args.workspace_url,
            token=args.token
        )

        print(f"Workspace: {workspace_url}")
        print("Fetching users...\n")

        # Get users
        users = get_all_users(workspace_url, token, debug=args.debug, max_users=args.max_users)

        print(f"\nFound {len(users)} users")

        # Extract email addresses
        emails = []
        for user in users:
            email = user.get("userName", "")
            if email:
                emails.append(email)

        # Save to CSV if output file specified
        if args.output:
            try:
                with open(args.output, 'w', newline='') as f:
                    for email in emails:
                        f.write(f"{email}\n")

                print(f"\n✓ Saved {len(emails)} email addresses to: {args.output}")
                print(f"  Format: One email per line")

            except Exception as e:
                print(f"\n✗ Error writing to CSV: {str(e)}")
                # Record end time even on error
                end_time = datetime.now()
                duration = end_time - start_time
                print(f"\n{'='*80}")
                print(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"Duration: {duration}")
                print(f"{'='*80}\n")
                sys.exit(1)
        else:
            # Display users if no output file
            print("\nUsers:")
            for i, user in enumerate(users, 1):
                username = user.get("userName", "unknown")
                display_name = user.get("displayName", "")
                email = user.get("userName", "")  # userName is typically the email

                print(f"{i:3d}. {username}")
                if display_name and display_name != username:
                    print(f"     Display: {display_name}")

            print(f"\nTip: Use --output users.csv to save email addresses to a file")

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
        print(f"Users processed: {len(users)}")
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
    main_example()