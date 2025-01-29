# Unified Open Source DEX Analytics for Cardano

This is a open-source repository for the "MuesliSwap Unified Open Source DEX Analytics" project funded in Fund 12 by Project Catalyst.

## Setup
Ensure you have a Ogmios instance running and set OGMIOS_URL environment variable appropriately (default: ws://localhost:1337).

There are two components: 
- querier, which pulls new blocks and saves data into a SQL database
- server, which reads from SQL database and provides a REST API for clients

## Running locally

Ideally, **DOCKER** should be used to run the server and not nohup.

------

To start the server use `./run-server.sh` or `./run-server.sh dev` to run the development version
To start the querier use `python -m querier` or `./run.sh`

`run.sh` and `run-server.sh` will skip requirements installation, if the `$SKIP_REQS` environment variable is set

**Note:** On chain rollback, querier will automatically exit so that it can be reinitialized from the last valid block. Therefore, if running as a systemd service, it should be set up to restart automatically.

## Docker deployment
Docker config is provided for convenient production deployment. One may start the entire stack like this:
```bash
# export POSTGRES_PASSWORD=...
git submodule update
docker compose build && docker compose up -d
```
Also make sure to set cardano-node ipc path in `docker-compose.yml`. 
Otherwise, you can also connect this to Ogmios running on the host machine (but remove Ogmios service from docker-compose).
**NOTE:** On Mac and Windows, you can use `OGMIOS_URL=host.docker.internal:1337`, but this won't work on Linux. 
You can either use `172.17.0.1` as host IP (or you can put `172.17.0.1 docker.host.internal` in `/etc/hosts`), 
or with Docker >= 20.10 you can do this in docker-compose.yml:
```yaml
extra_hosts:
      - "host.docker.internal:host-gateway"
```

### How it works with Ogmios
Simple: blocks are iterated sequentially from the max. point in querier's database.
Every transaction's outputs are compared to known order addresses - on success, tx is processed as an order.
Every transaction's inputs are compared to previously encountered orders - on success, tx is processed as a match/cancellation.
Then we save all orders and matches to database and e.g. to calculate the volume we only consider the matched orders.
