from opengsq.protocols import GameSpy1
from typing import Dict, Optional
import asyncio
import logging
from .config import settings


logger = logging.getLogger(__name__)


async def query_server(ip: str, port: int) -> Optional[Dict]:
    timeout = settings.SERVER_QUERY_TIMEOUT_S / 2
    try:
        gs1 = GameSpy1(host=ip, port=port, timeout=timeout)
        status = await gs1.get_status()
        return status
    except Exception as exc:
        logger.debug("Primary query failed for %s:%s: %s", ip, port, exc)

    standard_query_port = 23000
    if port != standard_query_port:
        try:
            gs1 = GameSpy1(host=ip, port=standard_query_port, timeout=timeout)
            status = await gs1.get_status()
            return status
        except Exception as exc:
            logger.debug("Fallback query failed for %s:%s via %s: %s", ip, port, standard_query_port, exc)
    return None
