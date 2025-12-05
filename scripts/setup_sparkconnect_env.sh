#!/usr/bin/env bash
# Helper to create a pyenv/python venv and install databricks-connect + deps
# Usage: scripts/setup_sparkconnect_env.sh [python_version] [venv_dir] [databricks_connect_version]
# Example: ./scripts/setup_sparkconnect_env.sh 3.12.3 .venv_sc 16.1.7

set -euo pipefail

PYTHON_VERSION=${1:-3.12.3}
VENV_DIR=${2:-.venv_sc}
DBC_VERSION=${3:-}

echo "Setup Spark Connect local environment"
echo "  python_version = ${PYTHON_VERSION}"
echo "  venv_dir       = ${VENV_DIR}"
if [ -n "${DBC_VERSION}" ]; then
  echo "  databricks-connect version = ${DBC_VERSION}"
fi

# Check for pyenv
if ! command -v pyenv >/dev/null 2>&1; then
  echo "pyenv is not installed or not on PATH."
  echo "Install pyenv and pyenv-virtualenv (Homebrew recommended):"
  echo "  brew install pyenv pyenv-virtualenv"
  echo "Then add to your ~/.zshrc (if not already):"
  echo "  eval \"\$(pyenv init --path)\""
  echo "  eval \"\$(pyenv init -)\""
  echo "  eval \"\$(pyenv virtualenv-init -)\""
  exit 1
fi

# Install the requested Python version if missing
if ! pyenv versions --bare | grep -q "^${PYTHON_VERSION}$"; then
  echo "Installing Python ${PYTHON_VERSION} via pyenv..."
  pyenv install -s "${PYTHON_VERSION}"
fi

# Create a virtualenv using the installed Python
PYENV_PREFIX=$(pyenv prefix "${PYTHON_VERSION}" 2>/dev/null || true)
if [ -n "${PYENV_PREFIX}" ]; then
  PYTHON_EXE="${PYENV_PREFIX}/bin/python"
else
  PYTHON_EXE="python${PYTHON_VERSION}"
fi

echo "Creating venv at ${VENV_DIR} using ${PYTHON_EXE}"

# Use builtin venv to create environment
if [ -d "${VENV_DIR}" ]; then
  echo "Virtualenv ${VENV_DIR} already exists - skipping creation"
else
  "${PYTHON_EXE}" -m venv "${VENV_DIR}"
fi

echo "Activating virtualenv and upgrading pip..."
source "${VENV_DIR}/bin/activate"
python -m pip install --upgrade pip setuptools wheel

echo "Installing packages: databricks-connect (optional), pyspark, pandas, requests"

# Map known databricks-connect versions to compatible pyspark and py4j pins.
# This mapping is best-effort; if your Databricks Runtime requires different pins,
# update the mapping or pass no pins and adjust manually.
declare -A PYSpark_PIN
declare -A PY4J_PIN

# Example mappings (adjust if you have different compatibility requirements)
PYSpark_PIN["16.1.7"]="3.5.2"
PY4J_PIN["16.1.7"]="0.10.9.7"

PYSpark_PIN["16.0.0"]="3.4.1"
PY4J_PIN["16.0.0"]="0.10.9.6"

if [ -n "${DBC_VERSION}" ]; then
  echo "Installing databricks-connect==${DBC_VERSION}"
  pip install "databricks-connect==${DBC_VERSION}"

  # If we have a known mapping for this databricks-connect version, pin pyspark/py4j
  if [ -n "${PYSpark_PIN[${DBC_VERSION}]:-}" ]; then
    PYS_VERSION="${PYSpark_PIN[${DBC_VERSION}]}"
    PY4J_VERSION="${PY4J_PIN[${DBC_VERSION}]}"
    echo "Pinning pyspark==${PYS_VERSION} and py4j==${PY4J_VERSION} for compatibility with databricks-connect ${DBC_VERSION}"
    pip install "pyspark==${PYS_VERSION}" "py4j==${PY4J_VERSION}"
  else
    echo "No pinned pyspark/py4j mapping found for databricks-connect ${DBC_VERSION}."
    echo "Installing latest pyspark and py4j (may require manual pinning if compatibility errors occur)."
    pip install pyspark py4j
  fi
else
  echo "No databricks-connect version provided; installing latest databricks-connect and helpers"
  pip install databricks-connect
  pip install pyspark pandas requests
fi

echo "Setup complete. Activate the environment with: source ${VENV_DIR}/bin/activate"
echo "Then export: DATABRICKS_HOST, DATABRICKS_TOKEN, and DATABRICKS_CLUSTER_ID (or DATABRICKS_SERVERLESS_COMPUTE_ID=auto) before running workspace_inventory.py"

echo "Example run (serverless):"
echo "  export DATABRICKS_HOST='your-workspace.cloud.databricks.com'"
echo "  export DATABRICKS_TOKEN='dapi...'" 
echo "  export DATABRICKS_SERVERLESS_COMPUTE_ID=auto"
echo "  python workspace_inventory.py --serverless --expected-python ${PYTHON_VERSION%.*} --databricks-connect-version ${DBC_VERSION:-latest} --max-user 5 --debug"

deactivate