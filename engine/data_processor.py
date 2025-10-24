import json
import logging
from datetime import datetime, timezone
import asyncpg
from typing import Dict, List

logger = logging.getLogger(__name__)


def _coerce_int(value, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


async def process_server_success(pool: asyncpg.Pool, ip: str, port: int, raw_data, exclusions: Dict):
    timestamp = datetime.now(timezone.utc).replace(microsecond=0)
    info = raw_data.info
    players_raw = raw_data.players

    server_identifier = f"{ip}:{port}"
    excluded_servers = exclusions.get('server_id', set())
    if server_identifier in excluded_servers or (ip, port) in excluded_servers:
        logger.info("Skipping server %s:%s because it is excluded by server ID.", ip, port)
        return

    gametype = info.get('gametype', 'N/A')
    if gametype in exclusions.get('gametype', set()):
        logger.info("Skipping server %s:%s due to excluded gametype '%s'.", ip, port, gametype)
        return

    # --- Normalize Player Data ---
    normalized_players = []
    excluded_player_names = exclusions.get('player_name', set())
    for p in players_raw:
        player_name = p.get('player', 'N/A')
        if player_name in excluded_player_names:
            continue
        normalized_players.append({
            "name": player_name,
            "keyhash": p.get('keyhash', None),
            "score": _coerce_int(p.get('score')),
            "ping": _coerce_int(p.get('ping')),
            "team": _coerce_int(p.get('team')),
            "kills": _coerce_int(p.get('kills')),
            "deaths": _coerce_int(p.get('deaths')),
        })

    # --- THIS IS THE FIX ---
    # Create the final info object that will be saved to the 'servers' table.
    # It starts with the raw server info and adds the processed player list.
    info_to_save = info.copy()
    info_to_save['players'] = normalized_players
    info_jsonb = json.dumps(info_to_save)
    # --- END OF FIX ---

    hostname = info.get('hostname', 'N/A')
    mapname = info.get('mapname', 'N/A').lower()
    active_mod = info.get('active_mods', 'N/A')

    normalized_players = []
    excluded_player_names = exclusions.get('player_name', set())
    for p in players_raw:
        player_name = p.get('player', 'N/A')
        if player_name in excluded_player_names:
            continue

        normalized_players.append({
            "name": player_name,
            "keyhash": p.get('keyhash', None),
            "score": _coerce_int(p.get('score')),
            "ping": _coerce_int(p.get('ping')),
            "team": _coerce_int(p.get('team')),
            "kills": _coerce_int(p.get('kills')),
            "deaths": _coerce_int(p.get('deaths')),
        })

    info_to_save = info.copy()
    info_to_save['players'] = normalized_players
    info_jsonb = json.dumps(info_to_save)

    # This INSERT/UPDATE now uses the correct 'info_jsonb' which contains the player list.
    server_id = await pool.fetchval("""
        INSERT INTO servers (ip, port, hostname, status, last_seen, first_seen, consecutive_failures, active_mod, gametype, info)
        VALUES ($1, $2, $3, 'online', $4, $4, 0, $5, $6, $7)
        ON CONFLICT (ip, port) DO UPDATE SET
            hostname = EXCLUDED.hostname, status = 'online', last_seen = EXCLUDED.last_seen,
            consecutive_failures = 0, active_mod = EXCLUDED.active_mod,
            gametype = EXCLUDED.gametype, info = EXCLUDED.info
        RETURNING id;
    """, ip, port, hostname, timestamp, active_mod, gametype, info_jsonb)

    if mapname != 'n/a':
        await pool.execute("INSERT INTO unique_maps (id) VALUES ($1) ON CONFLICT DO NOTHING;", mapname)

    previous_snapshot = await pool.fetchrow(
        "SELECT data, raw FROM server_snapshots WHERE server_id = $1 ORDER BY timestamp DESC LIMIT 1;",
        server_id
    )

    previous_players: List[Dict] = []
    previous_data = None
    previous_raw = None
    if previous_snapshot:
        previous_data = previous_snapshot['data']
        if isinstance(previous_data, str):
            previous_data = json.loads(previous_data)
        previous_raw = previous_snapshot['raw']
        if isinstance(previous_raw, str):
            previous_raw = json.loads(previous_raw)
        previous_players = previous_data.get('players', []) if previous_data else []

    await _update_player_sessions(pool, server_id, previous_players, normalized_players, timestamp)

    normalized_data = {"mapname": mapname, "players": normalized_players}
    raw_payload = {'info': raw_data.info, 'players': raw_data.players}

    if previous_data == normalized_data and previous_raw == raw_payload:
        logger.debug("Skipping snapshot insert for %s:%s; data unchanged.", ip, port)
        return

    await pool.execute("""
        INSERT INTO server_snapshots (server_id, timestamp, data, raw)
        VALUES ($1, $2, $3, $4);
    """, server_id, timestamp, json.dumps(normalized_data), json.dumps(raw_payload))


async def process_server_failure(pool: asyncpg.Pool, ip: str, port: int):
    timestamp = datetime.now(timezone.utc).replace(microsecond=0)
    from .config import settings

    failure_row = await pool.fetchrow(
        """
        INSERT INTO servers (ip, port, status, last_seen, first_seen, consecutive_failures)
        VALUES ($1, $2, 'offline', $3, $3, 1)
        ON CONFLICT (ip, port) DO UPDATE SET
            last_seen = EXCLUDED.last_seen,
            consecutive_failures = servers.consecutive_failures + 1,
            status = CASE
                WHEN servers.consecutive_failures + 1 >= $4 THEN 'offline'
                ELSE servers.status
            END
        RETURNING id, consecutive_failures, status;
        """,
        ip,
        port,
        timestamp,
        settings.OFFLINE_FAILURE_THRESHOLD,
    )

    if failure_row and failure_row['consecutive_failures'] >= settings.OFFLINE_FAILURE_THRESHOLD:
        server_id = failure_row['id']
        previous_snapshot = await pool.fetchrow(
            "SELECT data FROM server_snapshots WHERE server_id = $1 ORDER BY timestamp DESC LIMIT 1;",
            server_id,
        )
        previous_players: List[Dict] = []
        if previous_snapshot:
            snapshot_data = previous_snapshot['data']
            if isinstance(snapshot_data, str):
                snapshot_data = json.loads(snapshot_data)
            if snapshot_data:
                previous_players = snapshot_data.get('players', [])

        await _update_player_sessions(pool, server_id, previous_players, [], timestamp)


async def _update_player_sessions(pool: asyncpg.Pool, server_id: int, prev_players: List[Dict], current_players: List[Dict], timestamp: datetime):
    def _build_index(players: List[Dict]):
        index = {}
        for player in players:
            raw_name = player.get("name")
            if not raw_name:
                continue
            index[raw_name.lower()] = player
        return index

    previous_index = _build_index(prev_players)
    current_index = _build_index(current_players)

    prev_names_norm = set(previous_index.keys())
    current_names_norm = set(current_index.keys())

    joined_players = current_names_norm - prev_names_norm
    left_players = prev_names_norm - current_names_norm

    if left_players:
        await pool.execute("""
            UPDATE player_sessions SET leave_ts = $1
            WHERE server_id = $2 AND player_name_norm = ANY($3::VARCHAR[]) AND leave_ts IS NULL;
        """, timestamp, server_id, list(left_players))

    if joined_players:
        new_sessions_data = []
        for norm_name in joined_players:
            player = current_index.get(norm_name)
            if not player:
                continue
            raw_name = player.get("name")
            if not raw_name:
                continue
            new_sessions_data.append(
                (server_id, raw_name, norm_name, timestamp, player.get('keyhash'))
            )

        if new_sessions_data:
            await pool.executemany("""
                INSERT INTO player_sessions (server_id, player_name, player_name_norm, join_ts, keyhash)
                VALUES ($1, $2, $3, $4, $5);
            """, new_sessions_data)
