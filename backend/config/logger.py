import sys
import logging
import logging.handlers
import config
from config import constants as c

class GetLogger:
    """Custom logger with handler creation."""
    def __init__(self):
        self.service_name = config.SERVICE_NAME
        self.log_level = config.LOG_LEVEL
        # Logger config for both file and console.
        self.log_format = c.LOG_FORMAT
        self.formatter = logging.Formatter(self.log_format)

    def create_handlers(self):
        # Changing root logger level.
        logging.getLogger().setLevel(self.log_level)

        # Set config for console.
        self.console = logging.StreamHandler(sys.stdout)
        self.console.setLevel(self.log_level)
        self.console.setFormatter(self.formatter)

    def get_logger(self):
        log = logging.getLogger(self.service_name)
        log.propagate = True
        log.setLevel(self.log_level)
        # log.addHandler(self.console)
        log.info(f"Initialized logger., log_name: {self.service_name}")
        return log

# For custom log.
log_obj = GetLogger()
log_obj.create_handlers()
log = log_obj.get_logger()

log_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "()": "uvicorn.logging.DefaultFormatter",
            "fmt": log_obj.log_format,
            # "fmt": "%(levelprefix)s | %(asctime)s | %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",

        },
    },
    "handlers": {
        "default": {
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
        },
    },
    "loggers": {
        log_obj.service_name: {"handlers": ["default"], "level": "INFO"},
    },
}
