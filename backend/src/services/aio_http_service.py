import pdb
import aiohttp

import config
from config.logger import log
from config import constants as c


class AIOHttpSessionHandler:
    def __init__(self, base_url, timeout, *args, **kwargs):
        self.base_url = base_url
        self.timeout = timeout
        self.client_timeout = aiohttp.ClientTimeout(
            total=None,
            sock_connect=self.timeout,
            sock_read=self.timeout
        )

    async def init(self):
        self.session = aiohttp.ClientSession(
            base_url=self.base_url,
            timeout=self.client_timeout,
        )

    async def close(self):
        await self.session.close()


hospital_api_session = AIOHttpSessionHandler(
    base_url=config.HOSPITAL_API_BASE_URL,
    timeout=config.HOSPITAL_API_REQUEST_TIMEOUT
)

async def get_hospital_api_session():
    global hospital_api_session
    await hospital_api_session.init()
    log.info(f"*****Hospital API session initialized.*****")

async def close_hospital_api_session():
    global hospital_api_session
    await hospital_api_session.close()
    log.info(f"*****Hospital API session closed.*****")