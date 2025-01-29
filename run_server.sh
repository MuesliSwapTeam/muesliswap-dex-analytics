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

# run server
if [ "$1" == "dev" ]; then
    FLASK_APP=server.serve FLASK_ENV=development python3 -m flask run --port 50135
else
    gunicorn -w 5 server.serve:app -b localhost:50135
fi
