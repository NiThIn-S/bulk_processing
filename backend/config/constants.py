import os
import config
from datetime import timezone, timedelta


API_NAME = config.SERVICE_NAME
TZ = timezone(timedelta(hours=5, minutes=30))


PRODUCTION = config.APP_ENV == "prod"

if PRODUCTION:
    LOG_FORMAT = (
        "[%(asctime)s] [%(levelname)s] [%(process)d], name: %(name)s, ",
        "message: %(message)s"
    )
else:
    LOG_FORMAT = (
        "[%(asctime)s] [%(levelname)s] [%(process)d], name: %(name)s, "
        "message: %(message)s, "
        "line: %(lineno)d, path: %(pathname)s"
    )

MAX_HOSPITALS = 20
CSV_TTL = 86400  # 24 hours
STATUS_TTL = 86400  # 24 hours
MAX_CONCURRENT_WORKERS = 4  # Number of parallel tasks to spawn at once