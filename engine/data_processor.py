import json
from datetime import datetime, timezone
import asyncpg
from typing import Dict, List, Optional

async def process_server_success(pool: asyncpg.Pool, ip: str, port: int, raw_data, exclusions: Dict):
    timestamp = datetime.now(timezone.utc).replace(microsecond=0)
    info = raw_data.info
    players_raw = raw_data.players
    
    gametype = info.get('gametype', 'N/A')
    if gametype in exclusions.get('gametype', set()):
        print(f"Ignoring server {ip}:{port} due to excluded gametype: {gametype}")
        return

    # Normalize player data first
    normalized_players = []
    excluded_player_names = exclusions.get('player_name', set())
    for p in players_raw:
        player_name = p.get('player', 'N/A')
        if player_name in excluded_player_names:
            continue
        normalized_players.append({
            "name": player_name, "keyhash": p.get('keyhash', None),
            "score": int(p.get('score', 0)), "ping": int(p.get('ping', 0)),
            "team": int(p.get('team', 0)), "kills": int(p.get('kills', 0)),
            "deaths": int(p.get('deaths', 0)),
        })

    # --- THIS IS THE FIX ---
    # Create the final info object that will be saved.
    # Start with a copy of all raw server metadata.
    normalized_info = info.copy()
    # Ensure our specific fields are correctly typed and formatted.
    normalized_info['hostname'] = info.get('hostname', 'N/A')
    normalized_info['mapname'] = info.get('mapname', 'N/A').lower()
    normalized_info['num_players'] = int(info.get('numplayers', 0))
    normalized_info['max_players'] = int(info.get('maxplayers', 0))
    normalized_info['password'] = bool(int(info.get('password', 0)))
    # Now, add the normalized player list to this object.
    normalized_info['players'] = normalized_players
    
    if 'language' in normalized_info:
        del normalized_info['language']

    # Convert the complete object (with players) to a JSON string for storage.
    info_jsonb = json.dumps(normalized_info)
    # --- END OF FIX ---

    mapname = normalized_info['mapname']

    server_id = await pool.fetchval("""
        INSERT INTO servers (ip, port, hostname, status, last_seen, first_seen, consecutive_failures, active_mod, gametype, info)
        VALUES ($1, $2, $3, 'online', $4, $4, 0, $5, $6, $7)
        ON CONFLICT (ip, port) DO UPDATE SET
            hostname = EXCLUDED.hostname,
            status = 'online',
            last_seen = EXCLUDED.last_seen,
            consecutive_failures = 0,
            active_mod = EXCLUDED.active_mod,
            gametype = EXCLUDED.gametype,
            info = EXCLUDED.info
        RETURNING id;
    """, ip, port, normalized_info['hostname'], timestamp, info.get('active_mods', 'N/A'), gametype, info_jsonb)
    
    if mapname != 'n/a':
        await pool.execute("INSERT INTO unique_maps (id) VALUES ($1) ON CONFLICT DO NOTHING;", mapname)

    previous_players_result = await pool.fetchval(
        "SELECT data -> 'players' FROM server_snapshots WHERE server_id = $1 ORDER BY timestamp DESC LIMIT 1;",
        server_id
    )
    previous_players = json.loads(previous_players_result) if previous_players_result else []
    
    await _update_player_sessions(pool, server_id, previous_players, normalized_players, timestamp)

    normalized_data_jsonb = json.dumps({"mapname": mapname, "players": normalized_players})
    raw_jsonb = json.dumps({'info': raw_data.info, 'players': raw_data.players})

    await pool.execute("""
        INSERT INTO server_snapshots (server_id, timestamp, data, raw)
        VALUES ($1, $2, $3, $4);
    """, server_id, timestamp, normalized_data_jsonb, raw_jsonb)

async def process_server_failure(pool: asyncpg.Pool, ip: str, port: int):
    timestamp = datetime.now(timezone.utc).replace(microsecond=0)
    from .config import settings
    await pool.execute("UPDATE servers SET consecutive_failures = consecutive_failures + 1, last_seen = $1 WHERE ip = $2 AND port = $3;", timestamp, ip, port)
    
    server_id = await pool.fetchval("""
        UPDATE servers SET status = 'offline'
        WHERE ip = $1 AND port = $2 AND consecutive_failures >= $3 RETURNING id;
    """, ip, port, settings.OFFLINE_FAILURE_THRESHOLD)

    if server_id:
        await _update_player_sessions(pool, server_id, [], [], timestamp)

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