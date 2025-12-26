import traceback
from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError

from config.logger import log
from config import constants as c


def register_exception(app: FastAPI):
    """Function to log exception in request and response."""

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        exc_err = exc.errors()
        log.error(
            f"Invalid request., "
            f"status_code: {422}, "
            f"url: {request.url}, "
            f"err: {repr(exc_err)}"
        )
        content = {'detail': repr(exc_err)}
        return JSONResponse(
            content=content,
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        )

    @app.exception_handler(Exception)
    async def exception_handle(request: Request, exc: Exception):
        """catch other exception"""
        exc_str = traceback.format_exc()
        log.error(
            f"Unhandled exception., "
            f"status_code: {500}, "
            f"url: {request.url}, "
            f"method: {request.method}, "
            f"err: {repr(exc_str)}"
        )
        content = {'detail': repr(exc)}
        return JSONResponse(
            content=content,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
