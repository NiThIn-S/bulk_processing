# Bulk Processing

## Pre-requisites:
 - Docker with docker-compose setup
 
## Add build environment variables

```sh
export REDIS_PASSWORD="my_secure_token_123"
export HOSPITAL_API_BASE_URL="https://hospital-directory.onrender.com"
```

## First-time startup (use `--remove-orphans`)

```sh
docker-compose up --build -d --remove-orphans
```

## Check logs

```sh
docker-compose logs -f
```

## Stop when done

```sh
docker-compose down
```

## Run test script

### Set up `uv` package manager

Installation guide:
[https://docs.astral.sh/uv/getting-started/installation/](https://docs.astral.sh/uv/getting-started/installation/)

```sh
cd test
uv sync
uv run python3 test.py

```
