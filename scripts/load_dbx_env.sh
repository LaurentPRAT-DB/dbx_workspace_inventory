#!/usr/bin/env bash
# Source this file to export Databricks connection variables from a .env file
# Usage (in zsh/bash):
#   source scripts/load_dbx_env.sh [path-to-env-file]
# Example env file (.dbx_env):
#   DATABRICKS_HOST=your-workspace.cloud.databricks.com
#   DATABRICKS_TOKEN=dapi0123... 
#   DATABRICKS_SERVERLESS_COMPUTE_ID=auto
#   # or for classic cluster:
#   # DATABRICKS_CLUSTER_ID=cluster-123

set -u

# If the script is executed instead of sourced, warn the user and exit.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  echo "This script is intended to be sourced so variables are exported into your shell." >&2
  echo "Use: source scripts/load_dbx_env.sh path/to/.dbx_env" >&2
  exit 2
fi

ENV_FILE="${1:-.dbx_env}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Env file not found: ${ENV_FILE}" >&2
  return 2
fi

echo "Loading environment variables from ${ENV_FILE}..."

# Read the file line-by-line and export KEY=VALUE pairs
while IFS= read -r line || [[ -n "$line" ]]; do
  # Strip leading/trailing whitespace
  line="$(echo "$line" | sed -e 's/^[ \t]*//' -e 's/[ \t]*$//')"
  # Skip empty lines and comments
  if [[ -z "$line" ]] || [[ "$line" == \#* ]]; then
    continue
  fi

  # Only allow KEY=VALUE lines (no command evaluation)
  if [[ "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]]; then
    key="${BASH_REMATCH[1]}"
    val="${BASH_REMATCH[2]}"

    # Remove optional surrounding quotes from value
    if [[ "$val" =~ ^\".*\"$ ]] || [[ "$val" =~ ^\'.*\'$ ]]; then
      val="${val:1:${#val}-2}"
    fi

    export "${key}=${val}"
    echo "exported ${key}" 
  else
    echo "Skipping invalid line in ${ENV_FILE}: $line" >&2
  fi
done < "${ENV_FILE}"

echo "Done. You can now run: python workspace_inventory.py --serverless --max-users 5 --debug" 
