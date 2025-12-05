#!/usr/bin/env python3
"""
Databricks Server Version Checker

This utility helps identify the exact Python version running on your Databricks
serverless/cluster environment to help you sync your local databricks-connect version.

Usage:
    python check_databricks_versions.py --profile PROD
    python check_databricks_versions.py --serverless --debug
"""

import os
import sys
import json
from typing import Optional, Dict
from databricks_user_files_simple import authenticate_databricks, get_databricks_cli_config


def get_cluster_runtime_info(workspace_url: str, token: str, cluster_id: Optional[str] = None) -> Dict:
    """Get runtime information from a specific cluster using Databricks REST API."""
    import requests
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    try:
        if not cluster_id:
            # List clusters to find an active one
            url = f"{workspace_url}/api/2.0/clusters/list"
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            clusters = response.json().get("clusters", [])
            running_clusters = [c for c in clusters if c.get("state") == "RUNNING"]
            
            if running_clusters:
                cluster_id = running_clusters[0]["cluster_id"]
                print(f"Found running cluster: {running_clusters[0].get('cluster_name', cluster_id)}")
            else:
                return {"error": "No running clusters found"}
        
        # Get cluster details
        url = f"{workspace_url}/api/2.0/clusters/get"
        response = requests.post(url, headers=headers, json={"cluster_id": cluster_id}, timeout=30)
        response.raise_for_status()
        
        cluster_info = response.json()
        spark_version = cluster_info.get("spark_version", "")
        
        # Parse Databricks Runtime version to infer Python version
        # DBR versions typically follow pattern: "13.3.x-scala2.12" or similar
        runtime_info = {
            "cluster_id": cluster_id,
            "cluster_name": cluster_info.get("cluster_name", "unknown"),
            "spark_version": spark_version,
            "node_type": cluster_info.get("node_type_id", "unknown"),
            "driver_node_type": cluster_info.get("driver_node_type_id", "unknown"),
            "num_workers": cluster_info.get("num_workers", 0),
            "state": cluster_info.get("state", "unknown"),
        }
        
        # Infer Python version from DBR version
        if spark_version:
            dbr_major = spark_version.split('.')[0] if '.' in spark_version else spark_version[:2]
            
            # Common DBR to Python mappings (approximate)
            dbr_python_map = {
                '10': '3.8',
                '11': '3.9', 
                '12': '3.10',
                '13': '3.10',
                '14': '3.11',
                '15': '3.11',
                '16': '3.11',
            }
            
            inferred_python = dbr_python_map.get(dbr_major, 'unknown')
            runtime_info["inferred_python_version"] = inferred_python
            
            # Recommend databricks-connect version
            dbc_version_map = {
                '10': '10.4.*',
                '11': '11.3.*',
                '12': '12.2.*', 
                '13': '13.3.*',
                '14': '14.3.*',
                '15': '15.4.*',
                '16': '16.1.*',
            }
            
            runtime_info["recommended_databricks_connect"] = dbc_version_map.get(dbr_major, 'latest')
        
        return runtime_info
        
    except Exception as e:
        return {"error": f"Failed to get cluster info: {str(e)}"}


def check_local_versions():
    """Check current local environment versions."""
    try:
        import pkg_resources
        dbc_version = pkg_resources.get_distribution("databricks-connect").version
    except:
        dbc_version = "not installed"
    
    try:
        import pyspark
        pyspark_version = pyspark.__version__
    except:
        pyspark_version = "not installed"
    
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    
    return {
        "python_version": python_version,
        "python_full": sys.version,
        "databricks_connect": dbc_version,
        "pyspark": pyspark_version
    }


def main():
    """Main function to check and compare versions."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Check Databricks server and local versions for sync")
    parser.add_argument("--profile", "-p", help="Databricks CLI profile name")
    parser.add_argument("--workspace-url", help="Workspace URL")
    parser.add_argument("--token", help="Access token")
    parser.add_argument("--cluster-id", help="Specific cluster ID to check")
    parser.add_argument("--serverless", action="store_true", help="Check serverless compute info")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    
    args = parser.parse_args()
    
    try:
        # Authenticate
        workspace_url, token = authenticate_databricks(
            profile=args.profile,
            workspace_url=args.workspace_url,
            token=args.token
        )
        
        print(f"{'='*70}")
        print(f"DATABRICKS VERSION COMPATIBILITY CHECK")
        print(f"{'='*70}")
        print(f"Workspace: {workspace_url}")
        print()
        
        # Check local versions
        print("LOCAL ENVIRONMENT:")
        local_info = check_local_versions()
        for key, value in local_info.items():
            print(f"  {key.replace('_', ' ').title()}: {value}")
        print()
        
        # Get cluster/serverless info
        cluster_id = args.cluster_id
        if args.serverless:
            print("SERVERLESS COMPUTE:")
            print("  Note: Serverless compute uses the latest Databricks Runtime")
            print("  Typically Python 3.11 with recent DBR versions")
            print("  Recommended: databricks-connect==16.1.* or latest")
            
            # Try to get more specific info if possible
            try:
                cli_config = get_databricks_cli_config(profile=args.profile)
                if cli_config and "serverless_compute_id" in cli_config:
                    print(f"  Serverless ID: {cli_config['serverless_compute_id']}")
            except:
                pass
        else:
            print("CLUSTER RUNTIME:")
            runtime_info = get_cluster_runtime_info(workspace_url, token, cluster_id)
            
            if "error" in runtime_info:
                print(f"  Error: {runtime_info['error']}")
            else:
                print(f"  Cluster: {runtime_info.get('cluster_name', 'unknown')}")
                print(f"  Spark Version: {runtime_info.get('spark_version', 'unknown')}")
                print(f"  State: {runtime_info.get('state', 'unknown')}")
                print(f"  Inferred Python: {runtime_info.get('inferred_python_version', 'unknown')}")
                print(f"  Recommended databricks-connect: {runtime_info.get('recommended_databricks_connect', 'unknown')}")
        
        print()
        print("COMPATIBILITY ANALYSIS:")
        
        local_python = local_info["python_version"] 
        local_python_minor = ".".join(local_python.split(".")[:2])
        
        if not args.serverless and "inferred_python_version" in runtime_info:
            server_python = runtime_info["inferred_python_version"]
            if local_python_minor == server_python:
                print("  ✅ Python versions appear compatible!")
                print(f"  Both using Python {local_python_minor}")
            else:
                print("  ❌ Python version mismatch detected!")
                print(f"  Local: {local_python_minor}, Server: {server_python}")
                print(f"  Fix: Install Python {server_python} locally or use matching databricks-connect")
        
        # Databricks-connect recommendations
        current_dbc = local_info["databricks_connect"]
        if current_dbc != "not installed":
            print(f"  Current databricks-connect: {current_dbc}")
            
            if not args.serverless and "recommended_databricks_connect" in runtime_info:
                recommended = runtime_info["recommended_databricks_connect"]
                if current_dbc.startswith(recommended.replace('.*', '')):
                    print("  ✅ databricks-connect version looks compatible")
                else:
                    print("  ⚠️  Consider updating databricks-connect")
                    print(f"  Recommended: pip install databricks-connect=={recommended}")
        else:
            print("  ❌ databricks-connect not installed")
            if not args.serverless and "recommended_databricks_connect" in runtime_info:
                recommended = runtime_info["recommended_databricks_connect"]
                print(f"  Install: pip install databricks-connect=={recommended}")
            else:
                print("  Install: pip install databricks-connect")
        
        print()
        print("NEXT STEPS:")
        if args.serverless:
            print("  1. Ensure you have databricks-connect==16.1.* or latest")
            print("  2. Use Python 3.11 locally for best compatibility")
        else:
            if "recommended_databricks_connect" in runtime_info:
                recommended = runtime_info["recommended_databricks_connect"]
                print(f"  1. Install: pip install databricks-connect=={recommended}")
            if "inferred_python_version" in runtime_info:
                server_python = runtime_info["inferred_python_version"]
                if local_python_minor != server_python:
                    print(f"  2. Update local Python to {server_python} (optional but recommended)")
        
        print("  3. Test with: python databricks_user_files.py <username> --debug")
        print()
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()