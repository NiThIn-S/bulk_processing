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
from src.services.aio_http_service import get_hospital_api_session, close_hospital_api_session

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

    await get_hospital_api_session()
    yield

    # Closing hospital API session.
    await close_hospital_api_session()

    # Closing Redis connection.
    await rs.disconnect_redis()
    log.warning("Application shutting down")


if config.APP_ENV == "prod":
    openapi_url = None
else:
    openapi_url = "/openapi.json"
    log.warning("Non production environment - OpenAPI docs exposed")

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
log.info("*****Logger initialized.*****")

# Endpoint for liveness check.
@app.get("/liveness", status_code=status.HTTP_200_OK)
async def liveness():
    res = {
        "status": "ok",
        "message": "Liveness check passed",
    }
    try:
        await rs.check_status()
    except Exception as e:
        log.error(f"*****Liveness check failed.*****, err: {repr(e)}")
        res["status"] = "error"
        res["message"] = f"Liveness check failed, err: {repr(e)}"
        return JSONResponse(
            content=res,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    return res

# Endpoint for readiness check.
@app.get("/health", status_code=status.HTTP_200_OK)
async def healthcheck():
    return True


# Initializing all routers with prefix.
router_prefix = "/api"

app.include_router(router.hospital_router, prefix=router_prefix)

if __name__ == '__main__':
    uvicorn.run(
        "main:app",
        port=config.PORT,
        host='0.0.0.0',
        workers=1,
        reload=True,
    )
