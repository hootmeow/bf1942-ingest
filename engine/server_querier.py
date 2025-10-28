from opengsq.protocols import GameSpy1
from typing import Dict, Optional
import logging
from .config import settings


logger = logging.getLogger(__name__)


async def query_server(ip: str, port: int) -> Optional[Dict]:
    timeout = settings.SERVER_QUERY_TIMEOUT_S / 2
    primary_error = None
    try:
        gs1 = GameSpy1(host=ip, port=port, timeout=timeout)
        status = await gs1.get_status()
        return status
    except Exception as exc:
        primary_error = exc

    standard_query_port = 23000
    if port != standard_query_port:
        try:
            gs1 = GameSpy1(host=ip, port=standard_query_port, timeout=timeout)
            status = await gs1.get_status()
            if primary_error:
                logger.info(
                    "Primary query for %s:%s failed with %s; fallback to %s succeeded.",
                    ip,
                    port,
                    type(primary_error).__name__,
                    standard_query_port,
                )
            return status
        except Exception as exc:
            logger.warning(
                "Fallback query failed for %s:%s via %s with %s: %s",
                ip,
                port,
                standard_query_port,
                type(exc).__name__,
                exc,
            )
            if primary_error:
                logger.warning(
                    "Primary query for %s:%s previously failed with %s: %s",
                    ip,
                    port,
                    type(primary_error).__name__,
                    primary_error,
                )
            return None

    if primary_error:
        logger.warning(
            "Query for %s:%s failed with %s: %s",
            ip,
            port,
            type(primary_error).__name__,
            primary_error,
        )
    return None
