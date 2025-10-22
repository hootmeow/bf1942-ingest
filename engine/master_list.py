import aiohttp
import asyncio
import logging
from typing import List, Tuple, Optional

MASTER_SERVER_URL = "http://master.bf1942.org/json"

logger = logging.getLogger(__name__)


async def fetch_servers() -> Optional[List[Tuple[str, int]]]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(MASTER_SERVER_URL, timeout=10) as response:
                response.raise_for_status()
                data = await response.json()
                servers = [(item[0], int(item[1])) for item in data if isinstance(item, list) and len(item) == 2]
                return servers
    except Exception:
        logger.exception("An unexpected error occurred while fetching master list.")
        return None
