#!/usr/bin/env bash

# exit when any command fails
set -e
# cd into the right directory
cd "$(dirname "$0")"

# set up virtual env
[ ! -d "./venv" ] && python3 -m venv ./venv
source venv/bin/activate
if [ "$SKIP_REQS" == "1" ]; then
    pip install -r requirements.txt
fi

# run querier
until python3 -m "querier"; do
    echo "Exited with $?"
    sleep 1
done
