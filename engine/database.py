import asyncpg
import logging
from .config import settings


logger = logging.getLogger(__name__)


class Database:
    _pool: asyncpg.Pool = None

    async def connect(self):
        if not self._pool:
            logger.info("Creating PostgreSQL connection pool...")
            try:
                self._pool = await asyncpg.create_pool(dsn=settings.POSTGRES_DSN)
                await self._setup_schema()
                logger.info("PostgreSQL connection pool established.")
            except Exception:
                logger.exception("FATAL: Could not connect to PostgreSQL.")
                raise

    async def disconnect(self):
        if self._pool:
            await self._pool.close()
            logger.info("PostgreSQL connection pool closed.")

    async def get_pool(self) -> asyncpg.Pool:
        if not self._pool:
            await self.connect()
        return self._pool

    async def _setup_schema(self):
        """Runs all necessary DDL commands to create tables, triggers, and views."""
        logger.info("Ensuring database schema exists...")
        async with self._pool.acquire() as conn:
            await conn.execute("""
                DO $$ BEGIN
                    CREATE TYPE server_status AS ENUM ('online', 'offline');
                EXCEPTION
                    WHEN duplicate_object THEN null;
                END $$;
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS servers (
                    id SERIAL PRIMARY KEY, ip VARCHAR(15) NOT NULL, port INTEGER NOT NULL, hostname VARCHAR(255),
                    status server_status DEFAULT 'offline', last_seen TIMESTAMPTZ, first_seen TIMESTAMPTZ,
                    consecutive_failures INTEGER DEFAULT 0, active_mod VARCHAR(255), gametype VARCHAR(255),
                    info JSONB, UNIQUE(ip, port)
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS server_snapshots (
                    id SERIAL PRIMARY KEY, server_id INTEGER REFERENCES servers(id) ON DELETE CASCADE,
                    timestamp TIMESTAMPTZ NOT NULL, data JSONB, raw JSONB
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS player_sessions (
                    id SERIAL PRIMARY KEY, server_id INTEGER REFERENCES servers(id) ON DELETE CASCADE,
                    player_name VARCHAR(255) NOT NULL, player_name_norm VARCHAR(255) NOT NULL,
                    keyhash VARCHAR(32), join_ts TIMESTAMPTZ NOT NULL, leave_ts TIMESTAMPTZ
                );
            """)
            await conn.execute("CREATE TABLE IF NOT EXISTS unique_maps (id VARCHAR(255) PRIMARY KEY);")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS rounds (
                    id SERIAL PRIMARY KEY, server_id INTEGER NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
                    mapname VARCHAR(255), start_ts TIMESTAMPTZ NOT NULL, end_ts TIMESTAMPTZ,
                    duration_seconds INTEGER, start_snapshot_id INTEGER, end_snapshot_id INTEGER,
                    final_scoreboard JSONB
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS exclusions (
                    id SERIAL PRIMARY KEY, type VARCHAR(50) NOT NULL, value VARCHAR(255) NOT NULL,
                    notes TEXT, created_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(type, value)
                );
            """)
            await conn.execute("""
                ALTER TABLE exclusions
                ADD COLUMN IF NOT EXISTS server_ip VARCHAR(45) GENERATED ALWAYS AS (
                    CASE WHEN type = 'server_id' THEN split_part(value, ':', 1) ELSE NULL END
                ) STORED,
                ADD COLUMN IF NOT EXISTS server_port INTEGER GENERATED ALWAYS AS (
                    CASE
                        WHEN type = 'server_id' AND split_part(value, ':', 2) ~ '^[0-9]+$' THEN split_part(value, ':', 2)::INTEGER
                        ELSE NULL
                    END
                ) STORED;
            """)

            # This is the fully corrected trigger function
            await conn.execute("""
                CREATE OR REPLACE FUNCTION identify_round_change()
                RETURNS TRIGGER AS $$
                DECLARE
                    last_snapshot RECORD;
                    active_round RECORD;
                    map_changed BOOLEAN := FALSE;
                    timer_reset BOOLEAN := FALSE;
                    new_mapname TEXT;
                    last_round_time INTEGER;
                    new_round_time INTEGER;
                BEGIN
                    new_mapname := NEW.data->>'mapname';
                    new_round_time := COALESCE((NEW.raw->'info'->>'roundtimeremain')::integer, 0);

                    SELECT
                        data->>'mapname' as mapname,
                        COALESCE((raw->'info'->>'roundtimeremain')::integer, 0) as last_round_time
                    INTO last_snapshot
                    FROM server_snapshots
                    WHERE server_id = NEW.server_id AND timestamp < NEW.timestamp
                    ORDER BY timestamp DESC
                    LIMIT 1;

                    IF last_snapshot IS NOT NULL AND last_snapshot.mapname != new_mapname THEN
                        map_changed := TRUE;
                    END IF;
                    
                    IF last_snapshot IS NOT NULL AND last_snapshot.mapname = new_mapname AND new_round_time > last_snapshot.last_round_time + 60 THEN
                        timer_reset := TRUE;
                    END IF;

                    IF last_snapshot IS NULL OR map_changed OR timer_reset THEN
                        SELECT * INTO active_round FROM rounds WHERE server_id = NEW.server_id AND end_ts IS NULL;
                        IF active_round IS NOT NULL THEN
                            UPDATE rounds
                            SET end_ts = NEW.timestamp,
                                end_snapshot_id = NEW.id,
                                duration_seconds = EXTRACT(EPOCH FROM (NEW.timestamp - active_round.start_ts)),
                                final_scoreboard = NEW.data->'players'
                            WHERE id = active_round.id;
                        END IF;

                        INSERT INTO rounds (server_id, mapname, start_ts, start_snapshot_id)
                        VALUES (NEW.server_id, new_mapname, NEW.timestamp, NEW.id);
                    END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
            """)

            await conn.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'round_change_trigger') THEN
                        CREATE TRIGGER round_change_trigger
                        AFTER INSERT ON server_snapshots
                        FOR EACH ROW
                        EXECUTE FUNCTION identify_round_change();
                    END IF;
                END;
                $$;
            """)
            await conn.execute("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS mv_player_advanced_stats AS
                WITH session_stats AS (
                    SELECT
                        ps.player_name_norm,
                        MAX(ps.player_name) as player_name,
                        COUNT(*) as total_sessions,
                        SUM(EXTRACT(EPOCH FROM (ps.leave_ts - ps.join_ts))) as total_playtime_seconds,
                        FIRST_VALUE(s.hostname) OVER (PARTITION BY ps.player_name_norm ORDER BY COUNT(*) DESC) as favorite_server
                    FROM player_sessions ps JOIN servers s ON ps.server_id = s.id
                    WHERE ps.leave_ts IS NOT NULL
                        AND (s.ip || ':' || s.port) NOT IN (SELECT value FROM exclusions WHERE type = 'server_id')
                    GROUP BY ps.player_name_norm, s.hostname
                ),
                round_stats AS (
                    SELECT
                        player_data.name AS player_name,
                        SUM((player_data.kills)::integer) as total_kills,
                        SUM((player_data.deaths)::integer) as total_deaths,
                        SUM((player_data.score)::integer) as total_score,
                        FIRST_VALUE(r.mapname) OVER (PARTITION BY player_data.name ORDER BY COUNT(*) DESC) as favorite_map
                    FROM rounds r, jsonb_to_recordset(r.final_scoreboard) as player_data(name TEXT, kills TEXT, deaths TEXT, score TEXT)
                    WHERE r.final_scoreboard IS NOT NULL
                    GROUP BY player_data.name, r.mapname
                )
                SELECT
                    ss.player_name_norm,
                    ss.player_name,
                    ss.total_sessions,
                    ss.total_playtime_seconds,
                    COALESCE(rs.total_kills, 0) as total_kills,
                    COALESCE(rs.total_deaths, 0) as total_deaths,
                    COALESCE(rs.total_score, 0) as total_score,
                    CASE WHEN COALESCE(rs.total_deaths, 0) > 0 THEN ROUND(COALESCE(rs.total_kills, 0)::decimal / rs.total_deaths, 2) ELSE COALESCE(rs.total_kills, 0)::decimal END as kd_ratio,
                    CASE WHEN ss.total_playtime_seconds > 0 THEN ROUND((COALESCE(rs.total_kills, 0) * 60)::decimal / ss.total_playtime_seconds, 2) ELSE 0 END as kills_per_minute,
                    CASE WHEN ss.total_playtime_seconds > 0 THEN ROUND((COALESCE(rs.total_score, 0) * 60)::decimal / ss.total_playtime_seconds, 2) ELSE 0 END as score_per_minute,
                    ss.favorite_server,
                    rs.favorite_map
                FROM session_stats ss
                LEFT JOIN round_stats rs ON ss.player_name = rs.player_name;
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_servers_status ON servers(status);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_servers_last_seen ON servers(last_seen DESC);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_servers_active_mod ON servers(active_mod);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_server_id_ts ON server_snapshots(server_id, timestamp DESC);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_server_player_join ON player_sessions(server_id, player_name_norm, join_ts DESC);")
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_exclusions_server_ip_port
                ON exclusions(server_ip, server_port)
                WHERE type = 'server_id';
            """)
        logger.info("Schema is set up.")
