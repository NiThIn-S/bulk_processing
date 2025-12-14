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
