"""Configuration loader and validator for the VALORANT Leaderboard Discord Bot."""

import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv()


@dataclass(frozen=True)
class Config:
    discord_token: str
    riot_api_key: str
    act_id: str
    val_region: str
    routing_region: str
    sync_interval_minutes: int = 15
    min_matches: int = 1
    db_path: str = "val_tracker.db"

    @classmethod
    def load_from_env(cls, env_path: Optional[str] = None) -> "Config":
        """Loads configuration from environment variables."""
        if env_path:
            load_dotenv(dotenv_path=env_path, override=True)

        token = os.getenv("DISCORD_BOT_TOKEN", "").strip()
        riot_key = os.getenv("RIOT_API_KEY", "").strip()
        act_id = os.getenv("ACT_ID", "auto").strip() or "auto"
        val_region = os.getenv("VAL_REGION", "na").strip().lower()
        routing_region = os.getenv("ROUTING_REGION", "americas").strip().lower()

        sync_minutes_str = os.getenv("SYNC_INTERVAL_MINUTES", "15").strip()
        try:
            sync_minutes = int(sync_minutes_str)
        except ValueError:
            sync_minutes = 15

        min_matches_str = os.getenv("MIN_MATCHES", "1").strip()
        try:
            min_matches = max(1, int(min_matches_str))
        except ValueError:
            min_matches = 1

        db_path = os.getenv("DB_PATH", "val_tracker.db").strip()

        return cls(
            discord_token=token,
            riot_api_key=riot_key,
            act_id=act_id,
            val_region=val_region,
            routing_region=routing_region,
            sync_interval_minutes=sync_minutes,
            min_matches=min_matches,
            db_path=db_path,
        )

    def validate(self) -> None:
        """Validates that all critical configuration variables are present and sane."""
        missing = []
        if not self.discord_token:
            missing.append("DISCORD_BOT_TOKEN")
        if not self.riot_api_key:
            missing.append("RIOT_API_KEY")

        if missing:
            raise ValueError(
                f"Missing required environment variable(s): {', '.join(missing)}. "
                "Please configure them in your .env file or environment."
            )

        valid_routing = {"americas", "europe", "asia", "esports"}
        if self.routing_region not in valid_routing:
            raise ValueError(
                f"Invalid ROUTING_REGION: '{self.routing_region}'. "
                f"Expected one of: {', '.join(sorted(valid_routing))}"
            )

        valid_val_regions = {"na", "eu", "ap", "kr", "latam", "br"}
        if self.val_region not in valid_val_regions:
            raise ValueError(
                f"Invalid VAL_REGION: '{self.val_region}'. "
                f"Expected one of: {', '.join(sorted(valid_val_regions))}"
            )
