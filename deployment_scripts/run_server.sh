#!/usr/bin/env bash

# Exit when any command fails
set -e

# Move to the project root (one directory up from the script's location)
cd "$(dirname "$0")"/..

# Ensure Poetry environment is set up
if [ "$SKIP_REQS" != "1" ]; then
    poetry install
fi

# Run server
if [ "$1" == "dev" ]; then
    FLASK_APP=server.serve FLASK_ENV=development python3 -m flask run --port 50135
else
    gunicorn -w 5 server.serve:app -b localhost:50135
fi
