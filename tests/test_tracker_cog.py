"""Integration test verifying TrackerCog sync execution and concurrency locks."""

import os
from unittest.mock import AsyncMock, MagicMock, patch
import discord
import pytest
from config import Config
from database import Database
from riot_client import RiotClient
from cogs.tracker import TrackerCog

TEST_DB_PATH = "test_cog_tracker.db"
TARGET_ACT = "target-act-test-id"


@pytest.fixture
def config():
    return Config(
        discord_token="fake_token",
        riot_api_key="fake_key",
        act_id=TARGET_ACT,
        val_region="na",
        routing_region="americas",
        sync_interval_minutes=15,
        min_matches=1,
        db_path=TEST_DB_PATH,
    )


@pytest.mark.asyncio
async def test_tracker_sync_ingestion(config: Config):
    """Verify that TrackerCog._run_sync properly filters by ACT_ID, competitive queue, and ingests scores."""
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)

    db = Database(TEST_DB_PATH)
    await db.init_db()

    # Insert a tracked player
    await db.upsert_player("puuid-alice", "Alice", "NA1", "na", "americas")

    # Mock Riot Client
    mock_riot = MagicMock(spec=RiotClient)
    mock_riot.get_matchlist_by_puuid = AsyncMock(return_value={
        "puuid": "puuid-alice",
        "history": [
            {"matchId": "m-comp-act"},      # Matches Act & Competitive
            {"matchId": "m-wrong-act"},     # Wrong Act
            {"matchId": "m-unrated-act"},   # Unrated mode
        ]
    })

    # Return mock details based on matchId
    async def fake_get_match_details(match_id, val_region="na"):
        if match_id == "m-comp-act":
            return {
                "matchInfo": {
                    "matchId": "m-comp-act",
                    "seasonId": TARGET_ACT,
                    "queueId": "competitive",
                    "gameStartTimeMillis": 1700000000000,
                },
                "players": [
                    {
                        "puuid": "puuid-alice",
                        "gameName": "Alice",
                        "tagLine": "NA1",
                        "stats": {"performanceScore": 420.0}
                    }
                ]
            }
        elif match_id == "m-wrong-act":
            return {
                "matchInfo": {
                    "matchId": "m-wrong-act",
                    "seasonId": "old-season-id",
                    "queueId": "competitive",
                    "gameStartTimeMillis": 1690000000000,
                },
                "players": [{"puuid": "puuid-alice", "stats": {"performanceScore": 500.0}}]
            }
        elif match_id == "m-unrated-act":
            return {
                "matchInfo": {
                    "matchId": "m-unrated-act",
                    "seasonId": TARGET_ACT,
                    "queueId": "unrated",
                    "gameStartTimeMillis": 1700001000000,
                },
                "players": [{"puuid": "puuid-alice", "stats": {"performanceScore": 490.0}}]
            }
        return None

    mock_riot.get_match_details = AsyncMock(side_effect=fake_get_match_details)
    mock_riot.extract_performance_score = RiotClient.extract_performance_score

    mock_bot = MagicMock()
    mock_bot.wait_until_ready = AsyncMock()

    cog = TrackerCog(bot=mock_bot, db=db, riot_client=mock_riot, config=config)
    cog.sync_loop.cancel()  # Stop background loop during test

    # Run sync
    stats = await cog._run_sync()

    assert stats["players_synced"] == 1
    assert stats["matches_ingested"] == 1

    # Check database
    board = await db.get_summative_leaderboard(season_id=TARGET_ACT)
    assert len(board) == 1
    assert board[0]["game_name"] == "Alice"
    assert board[0]["total_score"] == 420.0
    assert board[0]["match_count"] == 1

    # Clean up
    if os.path.exists(TEST_DB_PATH):
        try:
            os.remove(TEST_DB_PATH)
        except PermissionError:
            pass


@pytest.mark.asyncio
async def test_bulk_track_players(config: Config):
    """Verify bulk tracking handles delimiters, partial success, and database upserts."""
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)

    db = Database(TEST_DB_PATH)
    await db.init_db()

    mock_riot = MagicMock(spec=RiotClient)

    # Mock get_account_by_riot_id to resolve "TenZ#0001", but fail "Ghost#0000"
    async def fake_get_account(game_name, tag_line, routing_region="americas"):
        if game_name == "TenZ" and tag_line == "0001":
            return {"puuid": "puuid-tenz", "gameName": "TenZ", "tagLine": "0001"}
        elif game_name == "Aspas" and tag_line == "LOUD":
            return {"puuid": "puuid-aspas", "gameName": "Aspas", "tagLine": "LOUD"}
        return None

    mock_riot.get_account_by_riot_id = AsyncMock(side_effect=fake_get_account)

    mock_bot = MagicMock()
    mock_bot.wait_until_ready = AsyncMock()

    cog = TrackerCog(bot=mock_bot, db=db, riot_client=mock_riot, config=config)
    cog.sync_loop.cancel()

    # Mock interaction
    mock_interaction = MagicMock()
    mock_interaction.guild_id = 12345
    mock_interaction.response.defer = AsyncMock()
    mock_interaction.followup.send = AsyncMock()

    input_text = "TenZ#0001, Aspas#LOUD; Ghost#0000\nInvalidTagPlayer"

    await cog.bulk_track_players.callback(cog, mock_interaction, input_text)

    # Verify defer and followup
    mock_interaction.response.defer.assert_called_once()
    mock_interaction.followup.send.assert_called_once()

    # Check embed details
    embed_sent = mock_interaction.followup.send.call_args[1]["embed"]
    assert "Bulk Player Registration Results" in embed_sent.title
    assert "**2** registered, **2** failed" in embed_sent.description

    # Verify players were actually upserted into SQLite
    tenz = await db.get_player_by_puuid("puuid-tenz")
    assert tenz is not None
    assert tenz["game_name"] == "TenZ"

    aspas = await db.get_player_by_puuid("puuid-aspas")
    assert aspas is not None
    assert aspas["game_name"] == "Aspas"

    # Clean up
    if os.path.exists(TEST_DB_PATH):
        try:
            os.remove(TEST_DB_PATH)
        except PermissionError:
            pass


@pytest.mark.asyncio
async def test_daily_leaderboard_schedule(config: Config):
    """Verify setting, posting, and removing daily scheduled leaderboard updates."""
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)

    db = Database(TEST_DB_PATH)
    await db.init_db()

    mock_riot = MagicMock(spec=RiotClient)
    mock_bot = MagicMock()
    mock_bot.wait_until_ready = AsyncMock()

    cog = TrackerCog(bot=mock_bot, db=db, riot_client=mock_riot, config=config)
    cog.sync_loop.cancel()
    cog.daily_post_loop.cancel()

    # Mock text channel
    mock_channel = MagicMock(spec=discord.TextChannel)
    mock_channel.id = 55555
    mock_channel.mention = "<#55555>"
    mock_channel.send = AsyncMock()
    perms = MagicMock()
    perms.send_messages = True
    perms.embed_links = True
    mock_channel.permissions_for.return_value = perms

    # Mock interaction
    mock_interaction = MagicMock()
    mock_interaction.guild_id = 99999
    mock_interaction.guild = MagicMock()
    mock_interaction.guild.me = MagicMock()
    mock_interaction.response.send_message = AsyncMock()

    # 1. Test invalid time format
    await cog.set_daily_leaderboard.callback(
        cog, mock_interaction, time="invalid_time", channel=mock_channel
    )
    assert "Invalid time format" in mock_interaction.response.send_message.call_args[0][0]

    # 2. Test valid time and timezone
    mock_interaction.response.send_message.reset_mock()
    await cog.set_daily_leaderboard.callback(
        cog,
        mock_interaction,
        time="14:45",
        channel=mock_channel,
        timezone="UTC",
        metric=None,  # Defaults to both
    )
    mock_interaction.response.send_message.assert_called_once()
    embed = mock_interaction.response.send_message.call_args[1]["embed"]
    assert "Daily Leaderboard Update Scheduled" in embed.title
    assert "14:45" in embed.description

    # Verify database record
    sched = await db.get_scheduled_leaderboard(99999)
    assert sched is not None
    assert sched["channel_id"] == 55555
    assert sched["hour"] == 14
    assert sched["minute"] == 45
    assert sched["metric"] == "both"

    # 3. Test daily_post_loop sending message
    import datetime
    import zoneinfo
    mock_bot.get_channel.return_value = mock_channel

    # Inject player and match into database so leaderboard has data
    await db.upsert_player("p1", "TenZ", "0001", "na", "americas")
    await db.insert_match_and_stats(
        match_id="m1",
        season_id=TARGET_ACT,
        game_start_time=1700000000,
        queue_id="competitive",
        player_stats=[{"puuid": "p1", "performance_score": 400.0}],
    )

    # Patch datetime.datetime to return 14:45 UTC
    class MockDatetime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.datetime(2026, 10, 8, 14, 45, 0, tzinfo=tz)

    with patch("datetime.datetime", MockDatetime):
        await cog.daily_post_loop.coro(cog)

    # Should have sent 2 embeds (Summative + Average since metric == "both")
    assert mock_channel.send.call_count == 2

    # Verify last_posted_date updated
    sched_after = await db.get_scheduled_leaderboard(99999)
    assert sched_after["last_posted_date"] == "2026-10-08"

    # Running again on the same day should NOT re-post
    mock_channel.send.reset_mock()
    with patch("datetime.datetime", MockDatetime):
        await cog.daily_post_loop.coro(cog)
    mock_channel.send.assert_not_called()

    # 4. Remove schedule
    mock_interaction.response.send_message.reset_mock()
    await cog.remove_daily_leaderboard.callback(cog, mock_interaction)
    mock_interaction.response.send_message.assert_called_once()
    assert "disabled" in mock_interaction.response.send_message.call_args[0][0]
    assert await db.get_scheduled_leaderboard(99999) is None

    # Clean up
    if os.path.exists(TEST_DB_PATH):
        try:
            os.remove(TEST_DB_PATH)
        except PermissionError:
            pass


