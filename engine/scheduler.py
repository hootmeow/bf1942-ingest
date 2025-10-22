import asyncio
import time
from .config import settings
from . import master_list, server_querier, data_processor

class Scheduler:
    def __init__(self, db_manager):
        self._queue = asyncio.PriorityQueue()
        self._known_servers = set()
        self._db_manager = db_manager
        self._pool = None
        self._exclusions = {'gametype': set(), 'player_name': set(), 'server_id': set()}

    async def _update_exclusions_cache(self):
        while True:
            print("Updating exclusions cache...")
            try:
                async with self._pool.acquire() as conn:
                    rows = await conn.fetch("SELECT type, value FROM exclusions;")
                    temp_exclusions = {'gametype': set(), 'player_name': set(), 'server_id': set()}
                    for row in rows:
                        if row['type'] in temp_exclusions:
                            temp_exclusions[row['type']].add(row['value'])
                    self._exclusions = temp_exclusions
                print(f"Exclusions cache updated: {len(self._exclusions['gametype'])} gametypes, {len(self._exclusions['player_name'])} players, {len(self._exclusions['server_id'])} servers.")
            except Exception as e:
                print(f"Error updating exclusions cache: {e}")
            await asyncio.sleep(300)

    async def _refresh_materialized_views(self):
        while True:
            await asyncio.sleep(300) 
            print("Refreshing materialized views for stats...")
            try:
                async with self._pool.acquire() as conn:
                    await conn.execute("REFRESH MATERIALIZED VIEW mv_player_advanced_stats;")
                    await conn.execute("REFRESH MATERIALIZED VIEW mv_server_advanced_stats;")
                print("Materialized views refreshed.")
            except Exception as e:
                print(f"Error refreshing materialized views: {e}")

    async def _master_list_poller(self):
        while True:
            print("Fetching master server list...")
            servers = await master_list.fetch_servers()
            if servers:
                print(f"Found {len(servers)} servers in master list.")
                current_server_ids = {f"{ip}:{port}" for ip, port in servers}
                
                new_servers = current_server_ids - self._known_servers
                for server_id in new_servers:
                    ip, port_str = server_id.split(":")
                    port = int(port_str)
                    print(f"Discovered new server: {ip}:{port}")
                    self._known_servers.add(server_id)
                    await self._queue.put((time.time(), ip, port))
            await asyncio.sleep(settings.MASTER_LIST_POLL_INTERVAL_S)

    async def _worker(self, worker_id: int):
        print(f"Worker {worker_id} started.")
        while True:
            next_poll_time, ip, port = await self._queue.get()

            sleep_duration = next_poll_time - time.time()
            if sleep_duration > 0:
                await asyncio.sleep(sleep_duration)

            result = await server_querier.query_server(ip, port)

            now = time.time()
            if result:
                await data_processor.process_server_success(self._pool, ip, port, result, self._exclusions)
                
                info = result.info
                num_players = int(info.get('numplayers', 0))
                time_remaining_str = info.get('roundtime')
                
                delay = settings.POLL_INTERVAL_ACTIVE_S

                if num_players == 0:
                    delay = settings.POLL_INTERVAL_EMPTY_S
                elif time_remaining_str and time_remaining_str.isdigit():
                    time_remaining = int(time_remaining_str)
                    if 0 < time_remaining < (settings.POLL_INTERVAL_ACTIVE_S + 5):
                        delay = time_remaining + 3 
                        print(f"INFO: Round ending on {ip}:{port}. Scheduling dynamic poll in {delay:.1f}s.")
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
        print("Ingestion engine scheduler is running.")
        await asyncio.gather(*tasks)
