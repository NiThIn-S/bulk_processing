# bulk_processing

## Add build env args to terminal:

```
export REDIS_PASSWORD=my_secure_token_123
export HOSPITAL_API_BASE_URL=https://hospital-directory.onrender.com
```


## First time ( --remove-orphans )
```
docker-compose up --build -d
```


## Check logs
```
docker-compose logs -f
```

## Stop when done
```
docker-compose down
```

## To run test script:
### setup uv package manager (link)[https://docs.astral.sh/uv/getting-started/installation/]

```
cd test

```