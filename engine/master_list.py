import aiohttp
import logging
from typing import List, Tuple, Optional

MASTER_SERVER_URL = "http://master.bf1942.org/json"

logger = logging.getLogger(__name__)


_session: Optional[aiohttp.ClientSession] = None


async def _get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession()
    return _session


async def fetch_servers() -> Optional[List[Tuple[str, int]]]:
    session = await _get_session()
    try:
        async with session.get(MASTER_SERVER_URL, timeout=10) as response:
            response.raise_for_status()
            data = await response.json()
            servers = [(item[0], int(item[1])) for item in data if isinstance(item, list) and len(item) == 2]
            return servers
    except Exception:
        logger.exception("An unexpected error occurred while fetching master list.")
        return None


async def close_session():
    global _session
    if _session and not _session.closed:
        await _session.close()
    _session = None
