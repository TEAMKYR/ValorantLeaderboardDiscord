"""Cog providing slash commands and background match synchronization."""

import asyncio
import datetime
import logging
import re
import time
from typing import Optional, List, Dict, Any
import zoneinfo

import discord
from discord import app_commands
from discord.ext import commands, tasks

from config import Config
from database import Database
from riot_client import RiotClient, RiotAPIError

logger = logging.getLogger("TrackerCog")


class TrackerCog(commands.Cog):
    """Cog managing VALORANT player tracking, background match syncing, and leaderboards."""

    def __init__(self, bot: commands.Bot, db: Database, riot_client: RiotClient, config: Config) -> None:
        self.bot = bot
        self.db = db
        self.riot_client = riot_client
        self.config = config
        self._sync_lock = asyncio.Lock()
        self.last_sync_time: Optional[float] = None
        self.active_act_id: str = self.config.act_id
        self.active_act_name: str = "Active Act"

        # Start the background synchronization loop
        self.sync_loop.change_interval(minutes=self.config.sync_interval_minutes)
        self.sync_loop.start()

        # Start the recurring daily leaderboard announcement loop
        self.daily_post_loop.start()

    def cog_unload(self) -> None:
        """Cancel background tasks on unload."""
        self.sync_loop.cancel()
        self.daily_post_loop.cancel()

    async def _refresh_act_id(self) -> str:
        """Dynamically detects active Act ID if configured to 'auto' or updates display name."""
        if self.config.act_id.lower() != "auto":
            self.active_act_id = self.config.act_id
            return self.active_act_id

        resolved_id, resolved_name = await self.riot_client.get_active_act(self.config.val_region)
        if resolved_id:
            if resolved_id != self.active_act_id and self.active_act_id != "auto":
                logger.info(
                    "🔄 VALORANT Act rollover detected! Switched from %s to %s (%s)",
                    self.active_act_id,
                    resolved_id,
                    resolved_name,
                )
            self.active_act_id = resolved_id
            self.active_act_name = resolved_name or "Active Act"
        elif self.active_act_id == "auto":
            logger.warning("Could not automatically resolve active Act ID. Retrying later.")
        return self.active_act_id

    # ==========================================
    # BACKGROUND SYNC ENGINE
    # ==========================================

    @tasks.loop(minutes=15)
    async def sync_loop(self) -> None:
        """Background worker that periodically syncs matches for all tracked players."""
        logger.info("Starting scheduled match synchronization cycle...")
        try:
            await self._run_sync()
        except Exception as exc:
            logger.exception("Unexpected error during scheduled sync: %s", exc)

    @sync_loop.before_loop
    async def before_sync_loop(self) -> None:
        """Wait until the bot is completely ready before running the loop."""
        await self.bot.wait_until_ready()
        await self._refresh_act_id()

    async def _run_sync(self, target_puuid: Optional[str] = None) -> Dict[str, int]:
        """Core sync procedure. Protected by an asyncio Lock to prevent overlapping runs."""
        if self._sync_lock.locked():
            logger.warning("Sync task is already running. Skipping concurrent request.")
            return {"players_synced": 0, "matches_ingested": 0}

        async with self._sync_lock:
            start_time = time.monotonic()
            stats = {"players_synced": 0, "matches_ingested": 0}
            target_act_id = await self._refresh_act_id()

            if target_puuid:
                player = await self.db.get_player_by_puuid(target_puuid)
                players = [player] if player else []
            else:
                players = await self.db.get_all_players()

            if not players:
                logger.info("No players tracked in database. Sync cycle finished.")
                self.last_sync_time = time.time()
                return stats

            # Pre-load all tracked PUUIDs for lobby matching
            all_tracked_players = await self.db.get_all_players()
            tracked_puuids = {p["puuid"] for p in all_tracked_players}

            for player in players:
                puuid = player["puuid"]
                name_tag = f"{player['game_name']}#{player['tag_line']}"
                val_region = player.get("region") or self.config.val_region

                try:
                    logger.debug("Syncing matchlist for player %s (%s)...", name_tag, puuid)
                    matchlist_data = await self.riot_client.get_matchlist_by_puuid(puuid, val_region)
                    if not matchlist_data or "history" not in matchlist_data:
                        logger.debug("No match history returned for player %s", name_tag)
                        await self.db.update_player_last_synced(puuid)
                        stats["players_synced"] += 1
                        continue

                    existing_player_matches = await self.db.get_player_match_ids(puuid)
                    history_items = matchlist_data.get("history", [])

                    for item in history_items:
                        match_id = item.get("matchId")
                        if not match_id:
                            continue

                        # If already recorded for this player, skip to avoid duplicate work
                        if match_id in existing_player_matches:
                            continue

                        # Check if match is already known globally in matches table
                        match_already_in_db = await self.db.is_match_recorded(match_id)

                        # Fetch full match details
                        match_details = await self.riot_client.get_match_details(match_id, val_region)
                        if not match_details:
                            continue

                        match_info = match_details.get("matchInfo", {})
                        season_id = match_info.get("seasonId", "")
                        queue_id = str(match_info.get("queueId", "")).lower()
                        game_start_millis = match_info.get("gameStartTimeMillis", int(time.time() * 1000))
                        game_start_time = int(game_start_millis / 1000)

                        # Verify Act / Season ID matches active target Act ID
                        if season_id != target_act_id:
                            logger.debug("Match %s seasonId '%s' != target '%s'. Skipping.", match_id, season_id, target_act_id)
                            continue

                        # Filter strictly to competitive queue
                        if queue_id != "competitive":
                            logger.debug("Match %s queueId '%s' is not competitive. Skipping.", match_id, queue_id)
                            continue

                        # Extract performance score for all tracked players present in the match
                        extracted_stats: List[Dict[str, Any]] = []
                        for p_data in match_details.get("players", []):
                            p_puuid = p_data.get("puuid")
                            if p_puuid in tracked_puuids:
                                perf_score = self.riot_client.extract_performance_score(p_data)
                                if perf_score is not None:
                                    extracted_stats.append({
                                        "puuid": p_puuid,
                                        "performance_score": perf_score
                                    })
                                else:
                                    logger.warning("No performance score found for player %s in match %s", p_puuid, match_id)

                        if extracted_stats:
                            await self.db.insert_match_and_stats(
                                match_id=match_id,
                                season_id=season_id,
                                game_start_time=game_start_time,
                                queue_id=queue_id,
                                player_stats=extracted_stats
                            )
                            existing_player_matches.add(match_id)
                            stats["matches_ingested"] += 1

                    await self.db.update_player_last_synced(puuid)
                    stats["players_synced"] += 1

                except Exception as p_err:
                    logger.error("Failed to sync matches for player %s: %s", name_tag, p_err)

            elapsed = time.monotonic() - start_time
            self.last_sync_time = time.time()
            logger.info(
                "Sync cycle finished in %.2fs. Players synced: %d, Matches ingested: %d",
                elapsed,
                stats["players_synced"],
                stats["matches_ingested"],
            )
            return stats

    # ==========================================
    # LEADERBOARD EMBED BUILDER & SCHEDULED POSTER
    # ==========================================

    async def _build_leaderboard_embed(self, is_avg: bool, guild_id: Optional[int] = None) -> discord.Embed:
        """Constructs a formatted Discord embed for summative or average leaderboards, optionally filtered by server."""
        target_act_id = await self._refresh_act_id()

        if is_avg:
            records = await self.db.get_average_leaderboard(
                season_id=target_act_id,
                guild_id=guild_id,
                min_matches=self.config.min_matches,
                limit=25
            )
            metric_title = "Average Performance Score (0–500)"
            score_col_name = "Avg Score"
        else:
            records = await self.db.get_summative_leaderboard(
                season_id=target_act_id,
                guild_id=guild_id,
                limit=25
            )
            metric_title = "Summative Performance Score (Cumulative)"
            score_col_name = "Total Score"

        act_label = (
            f"**Competitive Act:** `{self.active_act_name}` (`{target_act_id[:8]}...`)"
            if target_act_id != "auto"
            else f"**Competitive Act:** `{self.active_act_name}`"
        )

        # Modern aesthetic embed with VALORANT Crimson palette
        embed = discord.Embed(
            title=f"🏆 VALORANT Leaderboard — {metric_title}",
            color=0xFD4556,  # Riot VALORANT Crimson
            description=(
                f"{act_label}\n"
                f"**Queue:** `Competitive`\n"
                + (f"*(Min matches required: {self.config.min_matches})*\n" if is_avg else "")
            ),
        )

        if not records:
            embed.description += (
                "\n\n⚠️ **No match data recorded yet.**\n"
                "• Add players using `/track_player <riot_id>` or `/bulk_track_players`.\n"
                "• Sync matches using `/sync_now` or wait for the automatic 15-minute background worker."
            )
            embed.set_footer(text="VALORANT Performance Tracker • SQLite Indexed Cache")
            return embed

        medal_map = {1: "🥇", 2: "🥈", 3: "🥉"}

        lines = []
        for rank, row in enumerate(records, start=1):
            badge = medal_map.get(rank, f"`#{rank:<2}`")
            name = f"{row['game_name']}#{row['tag_line']}"
            score = row['avg_score'] if is_avg else row['total_score']
            matches = row['match_count']
            match_label = "match" if matches == 1 else "matches"

            lines.append(
                f"{badge} **{name}** — **{score:,.1f}** {score_col_name} `({matches} {match_label})`"
            )

        leaderboard_text = "\n".join(lines)
        if len(leaderboard_text) > 4000:
            leaderboard_text = leaderboard_text[:3990] + "\n..."

        embed.add_field(name="Rankings", value=leaderboard_text, inline=False)

        last_synced_str = f"<t:{int(self.last_sync_time)}:R>" if self.last_sync_time else "Pending"
        embed.set_footer(text=f"Last Background Sync: {last_synced_str} • Fast SQLite Cache")
        return embed

    @tasks.loop(minutes=1)
    async def daily_post_loop(self) -> None:
        """Checks every minute whether any server has a scheduled daily leaderboard to post."""
        try:
            schedules = await self.db.get_all_scheduled_leaderboards()
            if not schedules:
                return

            for sched in schedules:
                guild_id = sched["guild_id"]
                channel_id = sched["channel_id"]
                target_hour = sched["hour"]
                target_minute = sched["minute"]
                tz_name = sched.get("timezone", "UTC")
                metric = sched.get("metric", "both")
                last_posted = sched.get("last_posted_date", "")

                try:
                    tz = zoneinfo.ZoneInfo(tz_name)
                except Exception:
                    tz = zoneinfo.ZoneInfo("UTC")

                now = datetime.datetime.now(tz)
                today_str = now.strftime("%Y-%m-%d")

                if now.hour == target_hour and now.minute == target_minute and last_posted != today_str:
                    channel = self.bot.get_channel(channel_id)
                    if channel is None:
                        try:
                            channel = await self.bot.fetch_channel(channel_id)
                        except Exception as fetch_err:
                            logger.warning("Could not fetch channel %d for daily post: %s", channel_id, fetch_err)
                            continue

                    if isinstance(channel, (discord.TextChannel, discord.Thread)):
                        embeds_to_send = []
                        if metric in ("summative", "both"):
                            embeds_to_send.append(await self._build_leaderboard_embed(is_avg=False, guild_id=guild_id))
                        if metric in ("average", "both"):
                            embeds_to_send.append(await self._build_leaderboard_embed(is_avg=True, guild_id=guild_id))

                        for emb in embeds_to_send:
                            await channel.send(embed=emb)

                        await self.db.update_scheduled_last_posted(guild_id, today_str)
                        logger.info("Sent daily scheduled leaderboard to channel %d in guild %d", channel_id, guild_id)
        except Exception as exc:
            logger.exception("Error in daily_post_loop: %s", exc)

    @daily_post_loop.before_loop
    async def before_daily_post_loop(self) -> None:
        """Wait until bot is ready before running schedule loop."""
        await self.bot.wait_until_ready()

    # ==========================================
    # SLASH COMMANDS
    # ==========================================

    @app_commands.command(
        name="leaderboard",
        description="View the VALORANT Act Performance Score leaderboard for this server."
    )
    @app_commands.describe(
        metric="Choose between Summative Performance Score or Average Performance Score"
    )
    @app_commands.choices(metric=[
        app_commands.Choice(name="Summative Performance Score", value="summative"),
        app_commands.Choice(name="Average Performance Score", value="average"),
    ])
    async def leaderboard(self, interaction: discord.Interaction, metric: app_commands.Choice[str]) -> None:
        """Fast leaderboard display reading directly from indexed SQLite database for this server."""
        # Defer immediately to give Discord instant feedback
        await interaction.response.defer(thinking=False)
        is_avg = metric.value == "average"
        embed = await self._build_leaderboard_embed(is_avg=is_avg, guild_id=interaction.guild_id)
        await interaction.followup.send(embed=embed)

    @app_commands.command(
        name="track_player",
        description="[Admin] Add a player to the tracking pool using their Riot ID (Name#Tag)."
    )
    @app_commands.describe(riot_id="Player's Riot ID in the format GameName#TagLine (e.g. TenZ#0001)")
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def track_player(self, interaction: discord.Interaction, riot_id: str) -> None:
        """Resolves Riot ID to permanent PUUID and adds player to tracked database."""
        if "#" not in riot_id:
            await interaction.response.send_message(
                "❌ **Invalid Riot ID format.** Please provide `GameName#TagLine` (e.g. `TenZ#0001`).",
                ephemeral=True
            )
            return

        game_name, tag_line = riot_id.split("#", 1)
        game_name = game_name.strip()
        tag_line = tag_line.strip()

        if not game_name or not tag_line:
            await interaction.response.send_message(
                "❌ **Invalid Riot ID.** Both the name and tag cannot be empty.",
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=False)

        try:
            account = await self.riot_client.get_account_by_riot_id(
                game_name=game_name,
                tag_line=tag_line,
                routing_region=self.config.routing_region,
            )
        except RiotAPIError as err:
            await interaction.followup.send(
                f"❌ **Riot API Error ({err.status_code}):** Failed to query account. Check API key and Riot ID.",
                ephemeral=True
            )
            return
        except Exception as exc:
            logger.exception("Error querying Riot account: %s", exc)
            await interaction.followup.send(
                f"❌ **Error resolving account:** `{exc}`",
                ephemeral=True
            )
            return

        if not account or "puuid" not in account:
            await interaction.followup.send(
                f"❌ **Player not found:** `{game_name}#{tag_line}`. Check spelling and region.",
                ephemeral=True
            )
            return

        puuid = account["puuid"]
        resolved_name = account.get("gameName", game_name)
        resolved_tag = account.get("tagLine", tag_line)

        # Upsert into database
        await self.db.upsert_player(
            puuid=puuid,
            game_name=resolved_name,
            tag_line=resolved_tag,
            region=self.config.val_region,
            routing_region=self.config.routing_region,
            guild_id=interaction.guild_id,
        )

        embed = discord.Embed(
            title="✅ Player Registered for Tracking",
            color=0x00F5A0,  # Vibrant Emerald
            description=(
                f"**Player:** `{resolved_name}#{resolved_tag}`\n"
                f"**Permanent PUUID:** `{puuid}`\n"
                f"**Match Region:** `{self.config.val_region.upper()}`\n"
                f"**Routing Region:** `{self.config.routing_region.upper()}`\n\n"
                "A background match synchronization has been scheduled for this player."
            )
        )
        embed.set_footer(text="VALORANT Performance Tracker • Permanent PUUID Key")
        await interaction.followup.send(embed=embed)

        # Trigger background sync for this player asynchronously without blocking
        asyncio.create_task(self._run_sync(target_puuid=puuid))

    async def _handle_bulk_tracking(self, interaction: discord.Interaction, riot_ids: str) -> None:
        """Internal helper handling bulk player registration logic."""
        import re
        await interaction.response.defer(ephemeral=False)

        # Split on commas, semicolons, or newlines
        raw_entries = [entry.strip() for entry in re.split(r"[,;\n\r]+", riot_ids) if entry.strip()]

        if not raw_entries:
            await interaction.followup.send("❌ No valid Riot IDs provided in input.", ephemeral=True)
            return

        successes: List[Dict[str, str]] = []
        failures: List[Dict[str, str]] = []

        for raw in raw_entries:
            if "#" not in raw:
                failures.append({"input": raw, "reason": "Missing '#' tag separator"})
                continue

            game_name, tag_line = raw.split("#", 1)
            game_name = game_name.strip()
            tag_line = tag_line.strip()

            if not game_name or not tag_line:
                failures.append({"input": raw, "reason": "Empty name or tag"})
                continue

            try:
                account = await self.riot_client.get_account_by_riot_id(
                    game_name=game_name,
                    tag_line=tag_line,
                    routing_region=self.config.routing_region,
                )
                if not account or "puuid" not in account:
                    failures.append({"input": raw, "reason": "Player not found on Riot servers"})
                    continue

                puuid = account["puuid"]
                resolved_name = account.get("gameName", game_name)
                resolved_tag = account.get("tagLine", tag_line)

                await self.db.upsert_player(
                    puuid=puuid,
                    game_name=resolved_name,
                    tag_line=resolved_tag,
                    region=self.config.val_region,
                    routing_region=self.config.routing_region,
                    guild_id=interaction.guild_id,
                )
                successes.append({
                    "input": raw,
                    "resolved": f"{resolved_name}#{resolved_tag}",
                    "puuid": puuid,
                })
            except RiotAPIError as err:
                failures.append({"input": raw, "reason": f"Riot API Error ({err.status_code})"})
            except Exception as exc:
                failures.append({"input": raw, "reason": str(exc)})

        # Render summary embed
        embed = discord.Embed(
            title="📋 Bulk Player Registration Results",
            color=0x00F5A0 if successes else 0xFD4556,
            description=f"Processed **{len(raw_entries)}** player(s): **{len(successes)}** registered, **{len(failures)}** failed."
        )

        if successes:
            success_lines = [f"✅ **{s['resolved']}** (`{s['puuid'][:12]}...`)" for s in successes]
            val = "\n".join(success_lines)
            if len(val) > 1024:
                val = val[:1000] + f"\n...and {len(successes) - 10} more"
            embed.add_field(name=f"Successfully Tracked ({len(successes)})", value=val, inline=False)

        if failures:
            fail_lines = [f"❌ **{f['input']}** — *{f['reason']}*" for f in failures]
            val = "\n".join(fail_lines)
            if len(val) > 1024:
                val = val[:1000] + f"\n...and {len(failures) - 10} more"
            embed.add_field(name=f"Failed / Not Found ({len(failures)})", value=val, inline=False)

        embed.set_footer(text="VALORANT Performance Tracker • Bulk Onboarding")
        await interaction.followup.send(embed=embed)

        # Trigger background match sync if players were added
        if successes:
            asyncio.create_task(self._run_sync())

    @app_commands.command(
        name="bulk_track_players",
        description="[Admin] Add multiple players to tracking at once (comma/newline separated)."
    )
    @app_commands.describe(
        riot_ids="List of Riot IDs separated by commas, newlines, or semicolons (e.g. TenZ#0001, Aspas#0001)"
    )
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def bulk_track_players(self, interaction: discord.Interaction, riot_ids: str) -> None:
        """Batch-resolves multiple Riot IDs to PUUIDs and saves them to the database."""
        await self._handle_bulk_tracking(interaction, riot_ids)

    @app_commands.command(
        name="bulktrackplayers",
        description="[Admin] Add multiple players to tracking at once (alias for /bulk_track_players)."
    )
    @app_commands.describe(
        riot_ids="List of Riot IDs separated by commas, newlines, or semicolons (e.g. TenZ#0001, Aspas#0001)"
    )
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def bulktrackplayers(self, interaction: discord.Interaction, riot_ids: str) -> None:
        """Alias for bulk_track_players."""
        await self._handle_bulk_tracking(interaction, riot_ids)

    @app_commands.command(
        name="untrack_player",
        description="[Admin] Remove a player from this server's tracked leaderboard."
    )
    @app_commands.describe(riot_id="Player's Riot ID in the format GameName#TagLine (e.g. TenZ#0001)")
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def untrack_player(self, interaction: discord.Interaction, riot_id: str) -> None:
        """Removes a player from this server's leaderboard tracking."""
        if not interaction.guild_id:
            await interaction.response.send_message("❌ This command can only be used in a server.", ephemeral=True)
            return

        if "#" not in riot_id:
            await interaction.response.send_message("❌ Invalid Riot ID format. Use `Name#Tag`.", ephemeral=True)
            return

        game_name, tag_line = riot_id.split("#", 1)
        game_name = game_name.strip()
        tag_line = tag_line.strip()

        # Find in guild's players
        players = await self.db.get_players_for_guild(interaction.guild_id)
        target = next(
            (p for p in players if p["game_name"].lower() == game_name.lower() and p["tag_line"].lower() == tag_line.lower()),
            None
        )

        if not target:
            # Try resolving via Riot API to get PUUID
            try:
                acc = await self.riot_client.get_account_by_riot_id(game_name, tag_line, self.config.routing_region)
                if acc and "puuid" in acc:
                    target = next((p for p in players if p["puuid"] == acc["puuid"]), None)
            except Exception:
                pass

        if not target:
            await interaction.response.send_message(
                f"ℹ️ Player `{game_name}#{tag_line}` is not currently tracked in this server.",
                ephemeral=True
            )
            return

        removed = await self.db.untrack_player(interaction.guild_id, target["puuid"])
        if removed:
            await interaction.response.send_message(
                f"✅ **Player Untracked:** `{target['game_name']}#{target['tag_line']}` has been removed from this server's leaderboard."
            )
        else:
            await interaction.response.send_message("ℹ️ Player was not tracked in this server.", ephemeral=True)

    @app_commands.command(
        name="tracked_players",
        description="View all VALORANT players tracked in this server."
    )
    async def tracked_players(self, interaction: discord.Interaction) -> None:
        """Displays list of players tracked in this server."""
        if not interaction.guild_id:
            await interaction.response.send_message("❌ This command can only be used in a server.", ephemeral=True)
            return

        players = await self.db.get_players_for_guild(interaction.guild_id)
        if not players:
            await interaction.response.send_message(
                "ℹ️ **No players tracked in this server yet.**\nUse `/track_player <riot_id>` or `/bulk_track_players` to add players.",
                ephemeral=True
            )
            return

        lines = [f"• **{p['game_name']}#{p['tag_line']}** (`{p['region'].upper()}`)" for p in players]
        embed = discord.Embed(
            title=f"📋 Tracked Players in this Server ({len(players)})",
            color=0xFD4556,
            description="\n".join(lines[:50]) + ("\n...and more" if len(lines) > 50 else "")
        )
        embed.set_footer(text="VALORANT Performance Tracker • Server Roster")
        await interaction.response.send_message(embed=embed)

    @app_commands.command(
        name="sync_now",
        description="[Admin] Manually trigger the background match synchronization loop."
    )
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def sync_now(self, interaction: discord.Interaction) -> None:
        """Triggers the background sync worker immediately."""
        if self._sync_lock.locked():
            await interaction.response.send_message(
                "⏳ **Sync in progress:** A match synchronization cycle is already running. Please wait a moment.",
                ephemeral=True
            )
            return

        await interaction.response.send_message(
            "🔄 **Starting match sync cycle...** Ingesting recent competitive Act matches in the background.",
            ephemeral=False
        )

        # Run sync in background task
        async def _sync_and_notify():
            res = await self._run_sync()
            logger.info("Manual sync completed: %s", res)

        asyncio.create_task(_sync_and_notify())

    @app_commands.command(
        name="set_daily_leaderboard",
        description="[Admin] Schedule a recurring daily leaderboard update in a channel at a specific time."
    )
    @app_commands.describe(
        time="Time of day to post in 24-hour format HH:MM (e.g. 18:00, 09:30)",
        channel="The text channel where updates should be sent (defaults to current channel)",
        timezone="Timezone name (e.g. UTC, America/New_York, Europe/London - default: UTC)",
        metric="Which leaderboard to post: Summative, Average, or Both"
    )
    @app_commands.choices(metric=[
        app_commands.Choice(name="Both Leaderboards", value="both"),
        app_commands.Choice(name="Summative Performance Score", value="summative"),
        app_commands.Choice(name="Average Performance Score", value="average"),
    ])
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def set_daily_leaderboard(
        self,
        interaction: discord.Interaction,
        time: str,
        channel: Optional[discord.TextChannel] = None,
        timezone: str = "UTC",
        metric: Optional[app_commands.Choice[str]] = None,
    ) -> None:
        """Configures automated daily leaderboard posting for this server."""
        if not interaction.guild_id or not interaction.guild:
            await interaction.response.send_message("❌ This command can only be used in a server.", ephemeral=True)
            return

        target_channel = channel or interaction.channel
        if not isinstance(target_channel, (discord.TextChannel, discord.Thread)):
            await interaction.response.send_message("❌ Target channel must be a text channel.", ephemeral=True)
            return

        # Validate time format HH:MM
        time_match = re.match(r"^\s*([0-1]?[0-9]|2[0-3]):([0-5][0-9])\s*$", time)
        if not time_match:
            await interaction.response.send_message(
                "❌ **Invalid time format.** Please use 24-hour `HH:MM` format (e.g. `18:00` or `09:30`).",
                ephemeral=True
            )
            return

        hour = int(time_match.group(1))
        minute = int(time_match.group(2))

        # Validate timezone
        tz_clean = timezone.strip()
        try:
            zoneinfo.ZoneInfo(tz_clean)
            valid_tz = tz_clean
        except Exception:
            await interaction.response.send_message(
                f"❌ **Invalid timezone:** `{timezone}`. Please use a standard IANA timezone name (e.g. `UTC`, `America/New_York`, `US/Pacific`, `Europe/London`).",
                ephemeral=True
            )
            return

        metric_val = metric.value if metric else "both"

        # Check bot permissions in target channel if guild member available
        if interaction.guild.me:
            perms = target_channel.permissions_for(interaction.guild.me)
            if not perms.send_messages or not perms.embed_links:
                await interaction.response.send_message(
                    f"⚠️ **Missing Permissions:** The bot needs `Send Messages` and `Embed Links` in {target_channel.mention}.",
                    ephemeral=True
                )
                return

        await self.db.set_scheduled_leaderboard(
            guild_id=interaction.guild_id,
            channel_id=target_channel.id,
            hour=hour,
            minute=minute,
            timezone=valid_tz,
            metric=metric_val,
        )

        metric_names = {
            "both": "Both (Summative & Average)",
            "summative": "Summative Performance Score",
            "average": "Average Performance Score",
        }

        embed = discord.Embed(
            title="⏰ Daily Leaderboard Update Scheduled",
            color=0x00F5A0,
            description=(
                f"**Channel:** {target_channel.mention}\n"
                f"**Scheduled Time:** `{hour:02d}:{minute:02d}` ({valid_tz})\n"
                f"**Metric(s):** {metric_names.get(metric_val, metric_val)}\n\n"
                "The bot will automatically generate and post the leaderboard every day at the scheduled time."
            )
        )
        embed.set_footer(text="VALORANT Performance Tracker • Daily Recurring Post")
        await interaction.response.send_message(embed=embed)

    @app_commands.command(
        name="remove_daily_leaderboard",
        description="[Admin] Disable the daily recurring leaderboard updates in this server."
    )
    @app_commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def remove_daily_leaderboard(self, interaction: discord.Interaction) -> None:
        """Removes the scheduled daily post for the current guild."""
        if not interaction.guild_id:
            await interaction.response.send_message("❌ This command can only be used in a server.", ephemeral=True)
            return

        deleted = await self.db.delete_scheduled_leaderboard(interaction.guild_id)
        if deleted:
            await interaction.response.send_message("✅ **Daily leaderboard updates have been disabled** for this server.")
        else:
            await interaction.response.send_message("ℹ️ No scheduled daily leaderboard was found for this server.", ephemeral=True)

    @app_commands.command(
        name="view_daily_leaderboard",
        description="View the current daily leaderboard schedule for this server."
    )
    async def view_daily_leaderboard(self, interaction: discord.Interaction) -> None:
        """Displays configured daily schedule for this server."""
        if not interaction.guild_id:
            await interaction.response.send_message("❌ This command can only be used in a server.", ephemeral=True)
            return

        sched = await self.db.get_scheduled_leaderboard(interaction.guild_id)
        if not sched:
            await interaction.response.send_message(
                "ℹ️ **No daily leaderboard scheduled.** An admin can configure one with `/set_daily_leaderboard`.",
                ephemeral=True
            )
            return

        channel = self.bot.get_channel(sched["channel_id"])
        ch_mention = channel.mention if channel else f"<#{sched['channel_id']}>"
        metric_names = {
            "both": "Both (Summative & Average)",
            "summative": "Summative Performance Score",
            "average": "Average Performance Score",
        }

        embed = discord.Embed(
            title="⏰ Current Daily Leaderboard Schedule",
            color=0xFD4556,
            description=(
                f"**Channel:** {ch_mention}\n"
                f"**Scheduled Time:** `{sched['hour']:02d}:{sched['minute']:02d}` ({sched['timezone']})\n"
                f"**Metric:** {metric_names.get(sched['metric'], sched['metric'])}\n"
                f"**Last Posted:** `{sched['last_posted_date'] or 'Never'}`"
            )
        )
        embed.set_footer(text="VALORANT Performance Tracker • Scheduled Config")
        await interaction.response.send_message(embed=embed)

    # ==========================================
    # ERROR HANDLING
    # ==========================================

    async def cog_app_command_error(
        self, interaction: discord.Interaction, error: app_commands.AppCommandError
    ) -> None:
        """Handles permissions and command errors gracefully."""
        if isinstance(error, app_commands.MissingPermissions):
            msg = "⛔ **Permission Denied:** You must be a server Administrator to use this command."
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        else:
            logger.error("Command error in TrackerCog: %s", error)
            msg = f"⚠️ An error occurred while executing the command: `{error}`"
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)


async def setup(bot: commands.Bot) -> None:
    """Extension setup entry point."""
    db: Database = bot.db  # type: ignore[attr-defined]
    riot_client: RiotClient = bot.riot_client  # type: ignore[attr-defined]
    config: Config = bot.config  # type: ignore[attr-defined]
    await bot.add_cog(TrackerCog(bot, db, riot_client, config))
