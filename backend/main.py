import asyncio
import uvicorn
from logging.config import dictConfig # before fastapi
from contextlib import asynccontextmanager

from fastapi.responses import JSONResponse
from fastapi import FastAPI, status, Request
from fastapi.middleware.cors import CORSMiddleware

import config
from config.logger import log, log_config

from src import router
from src.router import dependencies as dp
from src.router import schemas
from src.services.redis_service import redis_service as rs
from src.services.exception_handler import register_exception


origins = ["*"]

dictConfig(log_config)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Establishing connection to Redis.
    """
    log.info("Application started")

    await rs.connect_redis()
    await rs.check_status()
    log.info("*****Connection to Redis established.*****")
    yield

    # Closing db connection.
    await rs.disconnect_redis()
    log.warning("Application shutting down")

if config.APP_ENV == "prod":
    openapi_url = None
else:
    openapi_url = "/openapi.json"
    log.warning(
        "Non production environment - OpenAPI docs exposed",
        extra={
            "environment": config.APP_ENV,
            "openapi_url": openapi_url,
        }
    )

app = FastAPI(
    title=config.SERVICE_NAME,
    version="1.0.0",
    lifespan=lifespan,
    openapi_url=openapi_url,
)


# CORS origin config.
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

register_exception(app=app)
log.info("Logger initialized")

# Endpoint for liveness check.
@app.get("/liveness", status_code=status.HTTP_200_OK)
async def liveness():
    res_content = {
        "reason": ""
    }
    try:
        try:
            await rs.check_status()
        except Exception as e:
            msg = "Ping failed for Redis"
            log.error(f"Ping failed for Redis, err: {repr(e)}")
            res_content["reason"] = msg
            return False
    except Exception as e:
        log.error(
            "Unexpected error in liveness check",
            f"err: {repr(e)}"
        )
        return False
    return True

# Endpoint for readiness check.
@app.get("/health", status_code=status.HTTP_200_OK)
async def healthcheck():
    return True


# Initializing all routers with prefix.
router_prefix = "/api/v1"
# app.include_router(router.user_api, prefix=router_prefix)
app.include_router(router.bulk_processing_router, prefix=router_prefix)

if __name__ == '__main__':
    uvicorn.run(
        "main:app",
        port=config.PORT,
        host='0.0.0.0',
        workers=1,
        reload=True,
    )
