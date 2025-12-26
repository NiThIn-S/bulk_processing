import pdb
import aiohttp
from uuid import UUID
from typing import Dict, List

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


async def create_hospital(hospital_data: Dict, batch_id: UUID) -> Dict:
    global hospital_api_session

    payload = {
        "name": hospital_data["name"],
        "address": hospital_data["address"],
        "phone": hospital_data.get("phone"),
        "creation_batch_id": str(batch_id)
    }

    try:
        async with hospital_api_session.session.post(
            "/hospitals/",
            json=payload
        ) as response:
            if response.status == 200:
                result = await response.json()
                log.info(f"Created hospital: {result.get('id')}, batch_id: {batch_id}")
                return result
            else:
                error_text = await response.text()
                log.error(f"Failed to create hospital: {response.status}, error: {error_text}")
                response.raise_for_status()
    except aiohttp.ClientError as e:
        log.error(f"Network error creating hospital: {repr(e)}")
        raise


async def activate_batch(batch_id: UUID) -> Dict:
    """
    Activate a batch via PATCH /hospitals/batch/{batch_id}/activate
    """
    global hospital_api_session

    try:
        async with hospital_api_session.session.patch(
            f"/hospitals/batch/{batch_id}/activate"
        ) as response:
            if response.status == 200:
                log.info(f"Activated batch: {batch_id}")
                return {}
            else:
                error_text = await response.text()
                log.error(f"Failed to activate batch: {response.status}, error: {error_text}")
                response.raise_for_status()
    except aiohttp.ClientError as e:
        log.error(f"Network error activating batch: {repr(e)}")
        raise


async def get_batch_hospitals(batch_id: UUID) -> List[Dict]:
    """
    Get all hospitals for a batch via GET /hospitals/batch/{batch_id}
    """
    global hospital_api_session

    try:
        async with hospital_api_session.session.get(
            f"/hospitals/batch/{batch_id}"
        ) as response:
            if response.status == 200:
                result = await response.json()
                log.info(f"Retrieved {len(result)} hospitals for batch: {batch_id}")
                return result
            else:
                error_text = await response.text()
                log.error(f"Failed to get batch hospitals: {response.status}, error: {error_text}")
                response.raise_for_status()
    except aiohttp.ClientError as e:
        log.error(f"Network error getting batch hospitals: {repr(e)}")
        raise