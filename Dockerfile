FROM python:3.10

WORKDIR /app

COPY requirements.txt requirements.txt
COPY muesliswap-cardano-python-utils muesliswap-cardano-python-utils

RUN pip install -r requirements.txt

COPY querier querier
COPY server server
