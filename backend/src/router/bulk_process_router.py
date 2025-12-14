import pdb
import json
from fastapi import APIRouter
from fastapi import Depends, Header
from fastapi import HTTPException, status
from fastapi.responses import JSONResponse

from config.logger import log
from config import constants as c
from . import schemas as schema
from src.router import dependencies as dp
from src.services.redis_service import redis_service as rs


bulk_processing_router = APIRouter()