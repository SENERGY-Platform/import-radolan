#!/usr/bin/env bash

set -euo pipefail

ENV_FILE="conda-env.yml"
LOCK_FILE="conda-lock.yml"
PLATFORMS=("linux-64")

echo "Install conda-lock"
pip install conda-lock

echo "Checking for ${ENV_FILE}..."
if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Error: ${ENV_FILE} not found."
  exit 1
fi

echo "Generating lock file for platforms: ${PLATFORMS[*]}"

# Build platform arguments dynamically
PLATFORM_ARGS=()
for platform in "${PLATFORMS[@]}"; do
  PLATFORM_ARGS+=("-p" "$platform")
done

conda-lock -f "${ENV_FILE}" "${PLATFORM_ARGS[@]}" --log-level DEBUG

echo "Lock file generated: ${LOCK_FILE}"
echo "Done."
