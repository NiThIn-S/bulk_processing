import os
import dotenv

dotenv.load_dotenv()

APP_ENV = os.getenv('ENV', 'dev')
PORT = int(os.getenv('PORT', '8000'))
HOST = os.getenv('HOST', '0.0.0.0')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
SERVICE_NAME = os.getenv('SERVICE_NAME', 'bulk-processing')


REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)
if not REDIS_PASSWORD:
    REDIS_PASSWORD = None

REDIS_KEY_PREFIX = "bulk-processing:"
