"""Database management module for VALORANT match tracking using aiosqlite."""

import time
import aiosqlite
from typing import List, Dict, Any, Optional, Set


class Database:
    """Asynchronous SQLite manager for players, matches, and performance stats."""

    def __init__(self, db_path: str = "val_tracker.db") -> None:
        self.db_path = db_path

    async def init_db(self) -> None:
        """Initializes tables and indexes if they do not exist."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA foreign_keys = ON;")
            await db.execute("PRAGMA journal_mode = WAL;")

            # 1. Players table
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS players (
                    puuid TEXT PRIMARY KEY,
                    game_name TEXT NOT NULL,
                    tag_line TEXT NOT NULL,
                    region TEXT NOT NULL,
                    routing_region TEXT NOT NULL,
                    last_synced INTEGER DEFAULT 0
                );
                """
            )

            # 2. Matches table
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS matches (
                    match_id TEXT PRIMARY KEY,
                    season_id TEXT NOT NULL,
                    game_start_time INTEGER NOT NULL,
                    queue_id TEXT NOT NULL
                );
                """
            )

            # 3. Player Match Stats table (Composite PK)
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS player_match_stats (
                    puuid TEXT NOT NULL,
                    match_id TEXT NOT NULL,
                    performance_score REAL NOT NULL,
                    PRIMARY KEY (puuid, match_id),
                    FOREIGN KEY (puuid) REFERENCES players (puuid) ON DELETE CASCADE,
                    FOREIGN KEY (match_id) REFERENCES matches (match_id) ON DELETE CASCADE
                );
                """
            )

            # 4. Scheduled Leaderboards table
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS scheduled_leaderboards (
                    guild_id INTEGER PRIMARY KEY,
                    channel_id INTEGER NOT NULL,
                    hour INTEGER NOT NULL,
                    minute INTEGER NOT NULL,
                    timezone TEXT DEFAULT 'UTC',
                    metric TEXT DEFAULT 'both',
                    last_posted_date TEXT DEFAULT ''
                );
                """
            )

            # 5. Guild Players junction table (Per-Server Isolation)
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS guild_players (
                    guild_id INTEGER NOT NULL,
                    puuid TEXT NOT NULL,
                    PRIMARY KEY (guild_id, puuid),
                    FOREIGN KEY (puuid) REFERENCES players (puuid) ON DELETE CASCADE
                );
                """
            )

            # Performance Indexes
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_matches_season ON matches (season_id);"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_pms_puuid ON player_match_stats (puuid);"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_pms_match_id ON player_match_stats (match_id);"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_guild_players_guild ON guild_players (guild_id);"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_guild_players_puuid ON guild_players (puuid);"
            )

            await db.commit()

    async def upsert_player(
        self,
        puuid: str,
        game_name: str,
        tag_line: str,
        region: str,
        routing_region: str,
        guild_id: Optional[int] = None,
    ) -> None:
        """Inserts or updates a tracked player record, optionally associating with a Discord server."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO players (puuid, game_name, tag_line, region, routing_region, last_synced)
                VALUES (?, ?, ?, ?, ?, 0)
                ON CONFLICT(puuid) DO UPDATE SET
                    game_name = excluded.game_name,
                    tag_line = excluded.tag_line,
                    region = excluded.region,
                    routing_region = excluded.routing_region;
                """,
                (puuid, game_name, tag_line, region, routing_region),
            )
            if guild_id is not None:
                try:
                    g_id = int(guild_id)
                    await db.execute(
                        """
                        INSERT OR IGNORE INTO guild_players (guild_id, puuid)
                        VALUES (?, ?);
                        """,
                        (g_id, puuid),
                    )
                except (ValueError, TypeError):
                    pass
            await db.commit()

    async def untrack_player(self, guild_id: int, puuid: str) -> bool:
        """Removes a player from a specific server's leaderboard tracking."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "DELETE FROM guild_players WHERE guild_id = ? AND puuid = ?;",
                (guild_id, puuid),
            )
            await db.commit()
            return cursor.rowcount > 0

    async def get_players_for_guild(self, guild_id: int) -> List[Dict[str, Any]]:
        """Retrieves all tracked players for a specific server."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            query = """
                SELECT p.* FROM players p
                JOIN guild_players gp ON p.puuid = gp.puuid
                WHERE gp.guild_id = ?
                ORDER BY p.game_name ASC;
            """
            async with db.execute(query, (guild_id,)) as cursor:
                rows = await cursor.fetchall()
                return [dict(r) for r in rows]

    async def update_player_last_synced(
        self, puuid: str, timestamp: Optional[int] = None
    ) -> None:
        """Updates the last_synced timestamp for a player."""
        if timestamp is None:
            timestamp = int(time.time())
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE players SET last_synced = ? WHERE puuid = ?;",
                (timestamp, puuid),
            )
            await db.commit()

    async def get_player_by_puuid(self, puuid: str) -> Optional[Dict[str, Any]]:
        """Retrieves a single player by PUUID."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM players WHERE puuid = ?;", (puuid,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def get_all_players(self) -> List[Dict[str, Any]]:
        """Retrieves all players tracked by at least one server (or all if none bound)."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            query = """
                SELECT DISTINCT p.* FROM players p
                WHERE p.puuid IN (SELECT puuid FROM guild_players)
                   OR NOT EXISTS (SELECT 1 FROM guild_players)
                ORDER BY p.game_name ASC;
            """
            async with db.execute(query) as cursor:
                rows = await cursor.fetchall()
                return [dict(r) for r in rows]

    async def is_match_recorded(self, match_id: str) -> bool:
        """Checks if a match is already stored in the matches table."""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT 1 FROM matches WHERE match_id = ? LIMIT 1;", (match_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return row is not None

    async def get_player_match_ids(self, puuid: str) -> Set[str]:
        """Returns all match IDs already recorded for this player."""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT match_id FROM player_match_stats WHERE puuid = ?;",
                (puuid,),
            ) as cursor:
                rows = await cursor.fetchall()
                return {row[0] for row in rows}

    async def insert_match_and_stats(
        self,
        match_id: str,
        season_id: str,
        game_start_time: int,
        queue_id: str,
        player_stats: List[Dict[str, Any]],
    ) -> None:
        """Atomically inserts match metadata and corresponding player stats."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA foreign_keys = ON;")
            await db.execute(
                """
                INSERT OR IGNORE INTO matches (match_id, season_id, game_start_time, queue_id)
                VALUES (?, ?, ?, ?);
                """,
                (match_id, season_id, game_start_time, queue_id),
            )

            for stat in player_stats:
                await db.execute(
                    """
                    INSERT INTO player_match_stats (puuid, match_id, performance_score)
                    VALUES (?, ?, ?)
                    ON CONFLICT(puuid, match_id) DO UPDATE SET
                        performance_score = excluded.performance_score;
                    """,
                    (stat["puuid"], match_id, stat["performance_score"]),
                )
            await db.commit()

    async def get_summative_leaderboard(
        self, season_id: str, guild_id: Optional[int] = None, limit: int = 25
    ) -> List[Dict[str, Any]]:
        """Retrieves total cumulative Performance Score across matches in the target Act."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            if guild_id is not None:
                query = """
                    SELECT
                        p.game_name,
                        p.tag_line,
                        p.puuid,
                        ROUND(SUM(pms.performance_score), 1) AS total_score,
                        COUNT(pms.match_id) AS match_count
                    FROM players p
                    JOIN guild_players gp ON p.puuid = gp.puuid
                    JOIN player_match_stats pms ON p.puuid = pms.puuid
                    JOIN matches m ON pms.match_id = m.match_id
                    WHERE m.season_id = ? AND gp.guild_id = ?
                    GROUP BY p.puuid
                    ORDER BY total_score DESC, match_count DESC
                    LIMIT ?;
                """
                params = (season_id, guild_id, limit)
            else:
                query = """
                    SELECT
                        p.game_name,
                        p.tag_line,
                        p.puuid,
                        ROUND(SUM(pms.performance_score), 1) AS total_score,
                        COUNT(pms.match_id) AS match_count
                    FROM players p
                    JOIN player_match_stats pms ON p.puuid = pms.puuid
                    JOIN matches m ON pms.match_id = m.match_id
                    WHERE m.season_id = ?
                    GROUP BY p.puuid
                    ORDER BY total_score DESC, match_count DESC
                    LIMIT ?;
                """
                params = (season_id, limit)

            async with db.execute(query, params) as cursor:
                rows = await cursor.fetchall()
                return [dict(r) for r in rows]

    async def get_average_leaderboard(
        self, season_id: str, guild_id: Optional[int] = None, min_matches: int = 1, limit: int = 25
    ) -> List[Dict[str, Any]]:
        """Retrieves average Performance Score (0-500) per match across matches in the target Act."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            if guild_id is not None:
                query = """
                    SELECT
                        p.game_name,
                        p.tag_line,
                        p.puuid,
                        ROUND(AVG(pms.performance_score), 1) AS avg_score,
                        COUNT(pms.match_id) AS match_count
                    FROM players p
                    JOIN guild_players gp ON p.puuid = gp.puuid
                    JOIN player_match_stats pms ON p.puuid = pms.puuid
                    JOIN matches m ON pms.match_id = m.match_id
                    WHERE m.season_id = ? AND gp.guild_id = ?
                    GROUP BY p.puuid
                    HAVING match_count >= ?
                    ORDER BY avg_score DESC, match_count DESC
                    LIMIT ?;
                """
                params = (season_id, guild_id, min_matches, limit)
            else:
                query = """
                    SELECT
                        p.game_name,
                        p.tag_line,
                        p.puuid,
                        ROUND(AVG(pms.performance_score), 1) AS avg_score,
                        COUNT(pms.match_id) AS match_count
                    FROM players p
                    JOIN player_match_stats pms ON p.puuid = pms.puuid
                    JOIN matches m ON pms.match_id = m.match_id
                    WHERE m.season_id = ?
                    GROUP BY p.puuid
                    HAVING match_count >= ?
                    ORDER BY avg_score DESC, match_count DESC
                    LIMIT ?;
                """
                params = (season_id, min_matches, limit)

            async with db.execute(query, params) as cursor:
                rows = await cursor.fetchall()
                return [dict(r) for r in rows]

    async def set_scheduled_leaderboard(
        self,
        guild_id: int,
        channel_id: int,
        hour: int,
        minute: int,
        timezone: str = "UTC",
        metric: str = "both",
    ) -> None:
        """Saves or updates daily leaderboard post configuration for a guild."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO scheduled_leaderboards (guild_id, channel_id, hour, minute, timezone, metric, last_posted_date)
                VALUES (?, ?, ?, ?, ?, ?, '')
                ON CONFLICT(guild_id) DO UPDATE SET
                    channel_id = excluded.channel_id,
                    hour = excluded.hour,
                    minute = excluded.minute,
                    timezone = excluded.timezone,
                    metric = excluded.metric;
                """,
                (guild_id, channel_id, hour, minute, timezone, metric),
            )
            await db.commit()

    async def get_scheduled_leaderboard(self, guild_id: int) -> Optional[Dict[str, Any]]:
        """Retrieves schedule configuration for a single guild."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM scheduled_leaderboards WHERE guild_id = ?;", (guild_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def get_all_scheduled_leaderboards(self) -> List[Dict[str, Any]]:
        """Retrieves all scheduled leaderboard postings across guilds."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM scheduled_leaderboards;") as cursor:
                rows = await cursor.fetchall()
                return [dict(r) for r in rows]

    async def delete_scheduled_leaderboard(self, guild_id: int) -> bool:
        """Removes scheduled leaderboard for a guild. Returns True if one existed."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "DELETE FROM scheduled_leaderboards WHERE guild_id = ?;", (guild_id,)
            )
            await db.commit()
            return cursor.rowcount > 0

    async def update_scheduled_last_posted(self, guild_id: int, date_str: str) -> None:
        """Records date when daily leaderboard was posted."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE scheduled_leaderboards SET last_posted_date = ? WHERE guild_id = ?;",
                (date_str, guild_id),
            )
            await db.commit()

