#!/bin/bash
#
# Helper script to run the workspace inventory locally using Spark Connect
# Supports Databricks CLI profile selection
#

set -e

# Parse command-line arguments
PROFILE=""
SERVERLESS=false
CLUSTER_ID=""
OUTPUT_PATH=""
OUTPUT_FORMAT=""
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--profile)
            PROFILE="$2"
            shift 2
            ;;
        --serverless)
            SERVERLESS=true
            shift
            ;;
        --cluster-id)
            CLUSTER_ID="$2"
            shift 2
            ;;
        --output)
            OUTPUT_PATH="$2"
            shift 2
            ;;
        --format)
            OUTPUT_FORMAT="$2"
            shift 2
            ;;
        --list-profiles)
            python workspace_inventory.py --list-profiles
            exit 0
            ;;
        *)
            EXTRA_ARGS+=("$1")
            shift
            ;;
    esac
done

echo "Databricks Workspace Inventory - Local Execution Helper"
echo "========================================================"
echo ""

# Check if .env file exists
if [ -f .env ]; then
    echo "Loading environment variables from .env file..."
    export $(cat .env | grep -v '^#' | xargs)
fi

# Check if Databricks CLI is configured
if ! command -v databricks &> /dev/null; then
    echo "Warning: Databricks CLI not found."
    echo "For easier authentication, install it:"
    echo "  brew install databricks-cli  # macOS"
    echo "  pip install databricks-cli   # Other platforms"
    echo ""
fi

# Check if ~/.databrickscfg exists
if [ ! -f ~/.databrickscfg ]; then
    echo "Warning: ~/.databrickscfg not found."
    echo "Configure it with: databricks configure --token"
    echo ""
    echo "Or set environment variables:"
    echo "  export DATABRICKS_WORKSPACE_URL='https://your-workspace.cloud.databricks.com'"
    echo "  export DATABRICKS_TOKEN='your-token'"
    echo ""
    
    # Check if environment variables are set as fallback
    if [ -z "$DATABRICKS_WORKSPACE_URL" ] || [ -z "$DATABRICKS_TOKEN" ]; then
        echo "Error: No authentication configured."
        echo "Either configure ~/.databrickscfg or set environment variables."
        exit 1
    fi
fi

# Build command arguments
CMD_ARGS=()

if [ -n "$PROFILE" ]; then
    CMD_ARGS+=("--profile" "$PROFILE")
    echo "Using Databricks CLI profile: $PROFILE"
elif [ -n "$DATABRICKS_CONFIG_PROFILE" ]; then
    CMD_ARGS+=("--profile" "$DATABRICKS_CONFIG_PROFILE")
    echo "Using Databricks CLI profile from environment: $DATABRICKS_CONFIG_PROFILE"
fi

if [ "$SERVERLESS" = true ]; then
    CMD_ARGS+=("--serverless")
    echo "Mode: Serverless compute"
elif [ -n "$CLUSTER_ID" ]; then
    CMD_ARGS+=("--cluster-id" "$CLUSTER_ID")
    echo "Cluster ID: $CLUSTER_ID"
elif [ -n "$DATABRICKS_CLUSTER_ID" ]; then
    CMD_ARGS+=("--cluster-id" "$DATABRICKS_CLUSTER_ID")
    echo "Cluster ID from environment: $DATABRICKS_CLUSTER_ID"
elif [ -n "$DATABRICKS_SERVERLESS_COMPUTE_ID" ]; then
    if [ "$DATABRICKS_SERVERLESS_COMPUTE_ID" = "auto" ]; then
        CMD_ARGS+=("--serverless")
        echo "Mode: Serverless compute (from environment)"
    fi
fi

if [ -n "$OUTPUT_PATH" ]; then
    CMD_ARGS+=("--output" "$OUTPUT_PATH")
elif [ -n "$DATABRICKS_OUTPUT_PATH" ]; then
    CMD_ARGS+=("--output" "$DATABRICKS_OUTPUT_PATH")
fi

if [ -n "$OUTPUT_FORMAT" ]; then
    CMD_ARGS+=("--format" "$OUTPUT_FORMAT")
elif [ -n "$DATABRICKS_OUTPUT_FORMAT" ]; then
    CMD_ARGS+=("--format" "$DATABRICKS_OUTPUT_FORMAT")
fi

# Add any extra arguments
CMD_ARGS+=("${EXTRA_ARGS[@]}")

echo ""

# Check for serverless compute dependencies
if [ "$SERVERLESS" = true ] || [ "$DATABRICKS_SERVERLESS_COMPUTE_ID" = "auto" ]; then
    echo "Checking serverless compute dependencies..."
    if ! python -c "import databricks.connect" 2>/dev/null; then
        echo "Warning: databricks-connect not found."
        echo "Install it with: pip install databricks-connect"
        echo "Note: Version must match your Databricks Runtime"
    fi
fi

# Check if Python dependencies are installed
echo "Checking dependencies..."
if ! python -c "import pyspark" 2>/dev/null; then
    echo "Installing dependencies..."
    pip install -r requirements.txt
fi

if ! python -c "import requests" 2>/dev/null; then
    echo "Installing requests..."
    pip install requests
fi

echo ""
echo "Starting workspace inventory..."
echo ""

# Run the script with all arguments
python workspace_inventory.py "${CMD_ARGS[@]}"

echo ""
echo "Done!"

