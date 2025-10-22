import asyncio
import logging
import time
from .config import settings
from . import master_list, server_querier, data_processor


logger = logging.getLogger(__name__)


def _safe_int(value, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(value)
    except (TypeError, ValueError):
        return default

class Scheduler:
    def __init__(self, db_manager):
        self._queue = asyncio.PriorityQueue()
        self._known_servers = set()
        self._db_manager = db_manager
        self._pool = None
        self._exclusions = {'gametype': set(), 'player_name': set(), 'server_id': set()}
        self._parked_servers = set()

    def _is_server_excluded(self, ip: str, port: int) -> bool:
        excluded_servers = self._exclusions.get('server_id', set())
        server_identifier = f"{ip}:{port}"
        return server_identifier in excluded_servers or (ip, port) in excluded_servers

    def _is_server_excluded(self, ip: str, port: int) -> bool:
        excluded_servers = self._exclusions.get('server_id', set())
        server_identifier = f"{ip}:{port}"
        return server_identifier in excluded_servers or (ip, port) in excluded_servers

    async def _update_exclusions_cache(self):
        while True:
            logger.info("Updating exclusions cache...")
            try:
                async with self._pool.acquire() as conn:
                    rows = await conn.fetch("SELECT type, value, server_ip, server_port FROM exclusions;")
                    temp_exclusions = {'gametype': set(), 'player_name': set(), 'server_id': set()}
                    for row in rows:
                        exclusion_type = row['type']
                        if exclusion_type not in temp_exclusions:
                            continue

                        if exclusion_type == 'server_id':
                            server_ip = row['server_ip']
                            server_port = row['server_port']
                            value = row['value']
                            if server_ip and server_port:
                                temp_exclusions['server_id'].add((server_ip, server_port))
                                temp_exclusions['server_id'].add(f"{server_ip}:{server_port}")
                            elif value:
                                temp_exclusions['server_id'].add(value)
                        else:
                            temp_exclusions[exclusion_type].add(row['value'])
                    self._exclusions = temp_exclusions
                logger.info(
                    "Exclusions cache updated: %d gametypes, %d players, %d servers.",
                    len(self._exclusions['gametype']),
                    len(self._exclusions['player_name']),
                    len(self._exclusions['server_id']),
                )
                # Resume any servers that were previously parked because they were excluded.
                for ip, port in list(self._parked_servers):
                    if not self._is_server_excluded(ip, port):
                        logger.info(
                            "Releasing previously excluded server %s:%s back into the polling queue.",
                            ip,
                            port,
                        )
                        self._parked_servers.discard((ip, port))
                        await self._queue.put((time.time(), ip, port))
            except Exception:
                logger.exception("Error updating exclusions cache.")
            await asyncio.sleep(300)

    async def _refresh_materialized_views(self):
        while True:
            await asyncio.sleep(300) 
            logger.info("Refreshing materialized views for stats...")
            try:
                async with self._pool.acquire() as conn:
                    await conn.execute("REFRESH MATERIALIZED VIEW mv_player_advanced_stats;")
                logger.info("Materialized views refreshed.")
            except Exception:
                logger.exception("Error refreshing materialized views.")

    async def _master_list_poller(self):
        while True:
            logger.info("Fetching master server list...")
            servers = await master_list.fetch_servers()
            if servers:
                logger.info("Found %d servers in master list.", len(servers))
                current_server_ids = {f"{ip}:{port}" for ip, port in servers}

                new_servers = current_server_ids - self._known_servers
                for server_id in new_servers:
                    ip, port_str = server_id.split(":")
                    port = int(port_str)
                    self._known_servers.add(server_id)
                    if self._is_server_excluded(ip, port):
                        logger.info("Discovered server %s:%s but it is currently excluded.", ip, port)
                        self._parked_servers.add((ip, port))
                        continue
                    else:
                        logger.info("Discovered new server: %s:%s", ip, port)
                    await self._queue.put((time.time(), ip, port))
            await asyncio.sleep(settings.MASTER_LIST_POLL_INTERVAL_S)

    async def _worker(self, worker_id: int):
        logger.info("Worker %d started.", worker_id)
        while True:
            next_poll_time, ip, port = await self._queue.get()

            if self._is_server_excluded(ip, port):
                logger.debug("Skipping excluded server %s:%s before polling.", ip, port)
                self._queue.task_done()
                self._parked_servers.add((ip, port))
                continue

            sleep_duration = next_poll_time - time.time()
            if sleep_duration > 0:
                await asyncio.sleep(sleep_duration)

            result = await server_querier.query_server(ip, port)

            now = time.time()
            if result:
                await data_processor.process_server_success(self._pool, ip, port, result, self._exclusions)
                
                info = result.info
                num_players = _safe_int(info.get('numplayers'))
                time_remaining = _safe_int(info.get('roundtimeremain') or info.get('roundtime'))

                delay = settings.POLL_INTERVAL_ACTIVE_S

                if num_players == 0:
                    delay = settings.POLL_INTERVAL_EMPTY_S
                elif 0 < time_remaining < (settings.POLL_INTERVAL_ACTIVE_S + 5):
                    delay = time_remaining + 3
                    logger.info(
                        "Round ending on %s:%s. Scheduling dynamic poll in %.1fs.",
                        ip,
                        port,
                        delay,
                    )
            else:
                await data_processor.process_server_failure(self._pool, ip, port)
                delay = settings.POLL_INTERVAL_OFFLINE_S

            await self._queue.put((now + delay, ip, port))
            self._queue.task_done()

    async def run(self):
        self._pool = await self._db_manager.get_pool()
        tasks = [
            self._master_list_poller(),
            self._refresh_materialized_views(),
            self._update_exclusions_cache()
        ]
        for i in range(200):
            tasks.append(self._worker(i))
        logger.info("Ingestion engine scheduler is running.")
        await asyncio.gather(*tasks)
