"""Automated tests for Database operations and leaderboard queries using aiosqlite."""

import os
import pytest
import pytest_asyncio
from database import Database

TEST_DB_PATH = "test_val_tracker.db"
TARGET_ACT = "act-test-uuid-2026"
OTHER_ACT = "act-other-uuid-2025"


@pytest_asyncio.fixture
async def db():
    """Fixture providing a fresh isolated database for each test."""
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)

    database = Database(db_path=TEST_DB_PATH)
    await database.init_db()
    yield database

    # Teardown
    if os.path.exists(TEST_DB_PATH):
        try:
            os.remove(TEST_DB_PATH)
        except PermissionError:
            pass


@pytest.mark.asyncio
async def test_upsert_player(db: Database):
    """Test inserting and updating players by PUUID."""
    # 1. Insert player
    await db.upsert_player(
        puuid="puuid-1",
        game_name="TenZ",
        tag_line="0001",
        region="na",
        routing_region="americas",
    )

    player = await db.get_player_by_puuid("puuid-1")
    assert player is not None
    assert player["game_name"] == "TenZ"
    assert player["tag_line"] == "0001"
    assert player["region"] == "na"

    # 2. Update player's name/tag without changing PUUID
    await db.upsert_player(
        puuid="puuid-1",
        game_name="TenZSEN",
        tag_line="GOAT",
        region="na",
        routing_region="americas",
    )

    updated = await db.get_player_by_puuid("puuid-1")
    assert updated is not None
    assert updated["game_name"] == "TenZSEN"
    assert updated["tag_line"] == "GOAT"


@pytest.mark.asyncio
async def test_match_ingestion_and_stats(db: Database):
    """Test match insertion and player match stats linking."""
    await db.upsert_player("puuid-1", "TenZ", "0001", "na", "americas")

    # Ingest match
    await db.insert_match_and_stats(
        match_id="match-101",
        season_id=TARGET_ACT,
        game_start_time=1700000000,
        queue_id="competitive",
        player_stats=[{"puuid": "puuid-1", "performance_score": 385.5}],
    )

    assert await db.is_match_recorded("match-101") is True
    assert await db.is_match_recorded("match-nonexistent") is False

    match_ids = await db.get_player_match_ids("puuid-1")
    assert "match-101" in match_ids


@pytest.mark.asyncio
async def test_summative_leaderboard(db: Database):
    """Test cumulative score aggregation across target Act matches."""
    # Setup 2 players
    await db.upsert_player("p1", "Alice", "NA1", "na", "americas")
    await db.upsert_player("p2", "Bob", "NA1", "na", "americas")

    # Match 1 (Target Act): Alice = 350, Bob = 250
    await db.insert_match_and_stats(
        match_id="m1",
        season_id=TARGET_ACT,
        game_start_time=1700000000,
        queue_id="competitive",
        player_stats=[
            {"puuid": "p1", "performance_score": 350.0},
            {"puuid": "p2", "performance_score": 250.0},
        ],
    )

    # Match 2 (Target Act): Alice = 300, Bob = 450
    await db.insert_match_and_stats(
        match_id="m2",
        season_id=TARGET_ACT,
        game_start_time=1700003600,
        queue_id="competitive",
        player_stats=[
            {"puuid": "p1", "performance_score": 300.0},
            {"puuid": "p2", "performance_score": 450.0},
        ],
    )

    # Match 3 (DIFFERENT ACT): Alice = 500 (should NOT be counted)
    await db.insert_match_and_stats(
        match_id="m3",
        season_id=OTHER_ACT,
        game_start_time=1690000000,
        queue_id="competitive",
        player_stats=[{"puuid": "p1", "performance_score": 500.0}],
    )

    board = await db.get_summative_leaderboard(season_id=TARGET_ACT)
    assert len(board) == 2

    # Bob total = 250 + 450 = 700.0
    # Alice total = 350 + 300 = 650.0 (m3 excluded)
    assert board[0]["game_name"] == "Bob"
    assert board[0]["total_score"] == 700.0
    assert board[0]["match_count"] == 2

    assert board[1]["game_name"] == "Alice"
    assert board[1]["total_score"] == 650.0
    assert board[1]["match_count"] == 2


@pytest.mark.asyncio
async def test_average_leaderboard_with_min_matches(db: Database):
    """Test average score aggregation with min_matches filtering."""
    await db.upsert_player("p1", "Alice", "NA1", "na", "americas")
    await db.upsert_player("p2", "Bob", "NA1", "na", "americas")
    await db.upsert_player("p3", "Charlie", "NA1", "na", "americas")

    # Charlie plays 1 match with high score 480
    await db.insert_match_and_stats(
        match_id="m1",
        season_id=TARGET_ACT,
        game_start_time=1700000000,
        queue_id="competitive",
        player_stats=[{"puuid": "p3", "performance_score": 480.0}],
    )

    # Alice plays 2 matches: 300 and 400 (avg = 350.0)
    await db.insert_match_and_stats(
        match_id="m2",
        season_id=TARGET_ACT,
        game_start_time=1700001000,
        queue_id="competitive",
        player_stats=[{"puuid": "p1", "performance_score": 300.0}],
    )
    await db.insert_match_and_stats(
        match_id="m3",
        season_id=TARGET_ACT,
        game_start_time=1700002000,
        queue_id="competitive",
        player_stats=[{"puuid": "p1", "performance_score": 400.0}],
    )

    # Bob plays 2 matches: 200 and 250 (avg = 225.0)
    await db.insert_match_and_stats(
        match_id="m4",
        season_id=TARGET_ACT,
        game_start_time=1700003000,
        queue_id="competitive",
        player_stats=[{"puuid": "p2", "performance_score": 200.0}],
    )
    await db.insert_match_and_stats(
        match_id="m5",
        season_id=TARGET_ACT,
        game_start_time=1700004000,
        queue_id="competitive",
        player_stats=[{"puuid": "p2", "performance_score": 250.0}],
    )

    # Query with min_matches = 1 (Charlie included)
    board_min1 = await db.get_average_leaderboard(season_id=TARGET_ACT, min_matches=1)
    assert len(board_min1) == 3
    assert board_min1[0]["game_name"] == "Charlie"
    assert board_min1[0]["avg_score"] == 480.0

    # Query with min_matches = 2 (Charlie excluded because he only played 1 match)
    board_min2 = await db.get_average_leaderboard(season_id=TARGET_ACT, min_matches=2)
    assert len(board_min2) == 2
    assert board_min2[0]["game_name"] == "Alice"
    assert board_min2[0]["avg_score"] == 350.0
    assert board_min2[1]["game_name"] == "Bob"
    assert board_min2[1]["avg_score"] == 225.0


@pytest.mark.asyncio
async def test_scheduled_leaderboards(db: Database):
    """Test saving, retrieving, updating, and deleting scheduled leaderboards."""
    # 1. Set schedule
    await db.set_scheduled_leaderboard(
        guild_id=12345,
        channel_id=67890,
        hour=18,
        minute=30,
        timezone="America/New_York",
        metric="both",
    )

    sched = await db.get_scheduled_leaderboard(12345)
    assert sched is not None
    assert sched["channel_id"] == 67890
    assert sched["hour"] == 18
    assert sched["minute"] == 30
    assert sched["timezone"] == "America/New_York"
    assert sched["metric"] == "both"
    assert sched["last_posted_date"] == ""

    # 2. Update schedule in-place
    await db.set_scheduled_leaderboard(
        guild_id=12345,
        channel_id=99999,
        hour=20,
        minute=0,
        timezone="UTC",
        metric="average",
    )
    updated = await db.get_scheduled_leaderboard(12345)
    assert updated["channel_id"] == 99999
    assert updated["hour"] == 20
    assert updated["metric"] == "average"

    # 3. Mark last posted date
    await db.update_scheduled_last_posted(12345, "2026-10-08")
    posted = await db.get_scheduled_leaderboard(12345)
    assert posted["last_posted_date"] == "2026-10-08"

    # 4. Get all schedules
    all_scheds = await db.get_all_scheduled_leaderboards()
    assert len(all_scheds) == 1

    # 5. Delete schedule
    deleted = await db.delete_scheduled_leaderboard(12345)
    assert deleted is True
    assert await db.get_scheduled_leaderboard(12345) is None


@pytest.mark.asyncio
async def test_per_server_isolation(db: Database):
    """Verify that players and leaderboards are isolated per Discord server."""
    # Server 1 tracks Alice and Bob
    await db.upsert_player("p-alice", "Alice", "0001", "na", "americas", guild_id=101)
    await db.upsert_player("p-bob", "Bob", "0001", "na", "americas", guild_id=101)

    # Server 2 tracks Bob and Charlie
    await db.upsert_player("p-bob", "Bob", "0001", "na", "americas", guild_id=102)
    await db.upsert_player("p-charlie", "Charlie", "0001", "na", "americas", guild_id=102)

    # Ingest match stats
    await db.insert_match_and_stats(
        match_id="m-isolated",
        season_id=TARGET_ACT,
        game_start_time=1700000000,
        queue_id="competitive",
        player_stats=[
            {"puuid": "p-alice", "performance_score": 400.0},
            {"puuid": "p-bob", "performance_score": 350.0},
            {"puuid": "p-charlie", "performance_score": 300.0},
        ],
    )

    # Query Server 1 leaderboard: must only contain Alice and Bob
    board_s1 = await db.get_summative_leaderboard(season_id=TARGET_ACT, guild_id=101)
    names_s1 = {r["game_name"] for r in board_s1}
    assert names_s1 == {"Alice", "Bob"}
    assert "Charlie" not in names_s1

    # Query Server 2 leaderboard: must only contain Bob and Charlie
    board_s2 = await db.get_summative_leaderboard(season_id=TARGET_ACT, guild_id=102)
    names_s2 = {r["game_name"] for r in board_s2}
    assert names_s2 == {"Bob", "Charlie"}
    assert "Alice" not in names_s2

    # Untrack Bob from Server 1
    untracked = await db.untrack_player(guild_id=101, puuid="p-bob")
    assert untracked is True

    # Server 1 now only has Alice
    board_s1_after = await db.get_summative_leaderboard(season_id=TARGET_ACT, guild_id=101)
    assert len(board_s1_after) == 1
    assert board_s1_after[0]["game_name"] == "Alice"

    # Server 2 STILL has Bob and Charlie intact!
    board_s2_after = await db.get_summative_leaderboard(season_id=TARGET_ACT, guild_id=102)
    names_s2_after = {r["game_name"] for r in board_s2_after}
    assert names_s2_after == {"Bob", "Charlie"}


