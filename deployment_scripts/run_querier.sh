#!/usr/bin/env bash

# Exit when any command fails
set -e

# Move to the project root (one directory up from the script's location)
cd "$(dirname "$0")"/..

# Ensure Poetry environment is set up
if [ "$SKIP_REQS" != "1" ]; then
    poetry install
fi

# Run querier
poetry run python -m "querier"