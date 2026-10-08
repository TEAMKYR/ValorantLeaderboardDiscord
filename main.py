"""Main entry point for the VALORANT Performance Score Discord Leaderboard Bot."""

import asyncio
import logging
import sys
import discord
from discord.ext import commands

from config import Config
from database import Database
from riot_client import RiotClient

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("ValTrackerBot")


class ValorantBot(commands.Bot):
    """Custom Discord Bot instance managing lifetime of DB, HTTP client, and background tasks."""

    def __init__(self, config: Config) -> None:
        intents = discord.Intents.default()
        # Default intents are sufficient for application slash commands and tasks
        super().__init__(command_prefix="!", intents=intents)
        self.config = config
        self.db = Database(config.db_path)
        self.riot_client = RiotClient(api_key=config.riot_api_key)

    async def setup_hook(self) -> None:
        """Executes asynchronous setup before the bot connects to Discord."""
        logger.info("Initializing SQLite database at: %s", self.config.db_path)
        await self.db.init_db()

        logger.info("Loading Cogs...")
        await self.load_extension("cogs.tracker")

        logger.info("Synchronizing application slash commands globally...")
        try:
            synced = await self.tree.sync()
            logger.info("Successfully synced %d slash commands.", len(synced))
        except Exception as exc:
            logger.error("Failed to sync slash commands: %s", exc)

    async def on_ready(self) -> None:
        """Invoked when Discord bot has established gateway connection."""
        logger.info("Logged in as %s (ID: %s)", self.user, self.user.id if self.user else "Unknown")
        logger.info(
            "Tracking Act ID: %s | Match Region: %s | Routing Region: %s",
            self.config.act_id,
            self.config.val_region,
            self.config.routing_region,
        )

        activity = discord.Activity(
            type=discord.ActivityType.competing,
            name=f"VALORANT Performance Score (Act: {self.config.act_id[:8]}...)",
        )
        await self.change_presence(activity=activity)

    async def close(self) -> None:
        """Clean teardown on shutdown."""
        logger.info("Shutting down bot. Releasing resources...")
        await self.riot_client.close()
        await super().close()


def main() -> None:
    """Entry point to validate environment and run the bot."""
    try:
        config = Config.load_from_env()
        config.validate()
    except ValueError as err:
        logger.critical("Configuration validation error: %s", err)
        logger.critical("Please refer to .env.example to set up your environment variables.")
        sys.exit(1)

    bot = ValorantBot(config)

    try:
        bot.run(config.discord_token, log_handler=None)
    except KeyboardInterrupt:
        logger.info("Process interrupted by user. Exiting.")
    except Exception as exc:
        logger.critical("Fatal error running bot: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
