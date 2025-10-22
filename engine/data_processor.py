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
    except (ValueError, TypeError):
        return default


async def process_server_success(pool: asyncpg.Pool, ip: str, port: int, raw_data, exclusions: Dict):
    timestamp = datetime.now(timezone.utc).replace(microsecond=0)
    info = raw_data.info
    players_raw = raw_data.players

    # --- Exclusion Checks ---
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
        "SELECT data FROM server_snapshots WHERE server_id = $1 ORDER BY timestamp DESC LIMIT 1;",
        server_id
    )
    previous_players: List[Dict] = []
    if previous_snapshot and previous_snapshot['data']:
        previous_data = json.loads(previous_snapshot['data']) if isinstance(previous_snapshot['data'], str) else previous_snapshot['data']
        previous_players = previous_data.get('players', [])

    await _update_player_sessions(pool, server_id, previous_players, normalized_players, timestamp)

    normalized_data = {"mapname": mapname, "players": normalized_players}
    raw_payload = {'info': raw_data.info, 'players': raw_data.players}

    await pool.execute("""
        INSERT INTO server_snapshots (server_id, timestamp, data, raw)
        VALUES ($1, $2, $3, $4);
    """, server_id, timestamp, json.dumps(normalized_data), json.dumps(raw_payload))


async def process_server_failure(pool: asyncpg.Pool, ip: str, port: int):
    timestamp = datetime.now(timezone.utc).replace(microsecond=0)
    from .config import settings

    await pool.execute(
        "UPDATE servers SET consecutive_failures = consecutive_failures + 1, last_seen = $1 WHERE ip = $2 AND port = $3;",
        timestamp, ip, port,
    )

    server_id = await pool.fetchval("""
        UPDATE servers SET status = 'offline'
        WHERE ip = $1 AND port = $2 AND consecutive_failures >= $3 RETURNING id;
    """, ip, port, settings.OFFLINE_FAILURE_THRESHOLD)

    if server_id:
        previous_snapshot = await pool.fetchrow(
            "SELECT data FROM server_snapshots WHERE server_id = $1 ORDER BY timestamp DESC LIMIT 1;",
            server_id,
        )
        previous_players: List[Dict] = []
        if previous_snapshot and previous_snapshot['data']:
            snapshot_data = json.loads(previous_snapshot['data']) if isinstance(previous_snapshot['data'], str) else previous_snapshot['data']
            if snapshot_data:
                previous_players = snapshot_data.get('players', [])
        await _update_player_sessions(pool, server_id, previous_players, [], timestamp)


async def _update_player_sessions(pool: asyncpg.Pool, server_id: int, prev_players: List[Dict], current_players: List[Dict], timestamp: datetime):
    prev_player_names = {p.get("name") for p in prev_players}
    current_player_names = {p.get("name") for p in current_players}

    joined_players = current_player_names - prev_player_names
    left_players = prev_player_names - current_player_names

    if left_players:
        await pool.execute("""
            UPDATE player_sessions SET leave_ts = $1
            WHERE server_id = $2 AND player_name_norm = ANY($3::VARCHAR[]) AND leave_ts IS NULL;
        """, timestamp, server_id, [name.lower() for name in left_players])

    if joined_players:
        new_sessions_data = [
            (server_id, name, name.lower(), timestamp, p.get('keyhash', None))
            for name in joined_players for p in current_players if p.get('name') == name
        ]
        await pool.executemany("""
            INSERT INTO player_sessions (server_id, player_name, player_name_norm, join_ts, keyhash)
            VALUES ($1, $2, $3, $4, $5);
        """, new_sessions_data)