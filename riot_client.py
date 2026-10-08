"""Riot Games REST API client with intelligent dual-window rate limiting and exponential backoff."""

import asyncio
import logging
import random
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple
import aiohttp

logger = logging.getLogger("RiotClient")


class RiotRateLimiter:
    """Token bucket / sliding window rate limiter enforcing:

    - 20 requests per 1 second
    - 100 requests per 120 seconds (2 minutes)
    """

    def __init__(self, limit_1s: int = 20, limit_120s: int = 100) -> None:
        self.limit_1s = limit_1s
        self.limit_120s = limit_120s
        self.timestamps_1s: List[float] = []
        self.timestamps_120s: List[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Blocks until a request token is available within both rate limit windows."""
        while True:
            async with self._lock:
                now = time.monotonic()

                # Clean expired timestamps
                self.timestamps_1s = [t for t in self.timestamps_1s if now - t < 1.0]
                self.timestamps_120s = [t for t in self.timestamps_120s if now - t < 120.0]

                # Check 1-second burst window
                delay_1s = 0.0
                if len(self.timestamps_1s) >= self.limit_1s:
                    delay_1s = 1.0 - (now - self.timestamps_1s[0])

                # Check 120-second rolling window
                delay_120s = 0.0
                if len(self.timestamps_120s) >= self.limit_120s:
                    delay_120s = 120.0 - (now - self.timestamps_120s[0])

                delay = max(delay_1s, delay_120s)
                if delay <= 0:
                    # Slot available, record timestamp and proceed
                    self.timestamps_1s.append(now)
                    self.timestamps_120s.append(now)
                    return

            # Wait outside the lock before re-checking
            logger.debug("Rate limit threshold approached. Sleeping for %.2f seconds.", delay)
            await asyncio.sleep(delay + 0.05)


class RiotAPIError(Exception):
    """Base exception for Riot API failures."""

    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        super().__init__(f"Riot API Error ({status_code}): {message}")


class RiotClient:
    """Asynchronous client interacting with Riot Games endpoints."""

    def __init__(
        self,
        api_key: str,
        session: Optional[aiohttp.ClientSession] = None,
        rate_limiter: Optional[RiotRateLimiter] = None,
    ) -> None:
        self.api_key = api_key
        self._session = session
        self._own_session = session is None
        self.rate_limiter = rate_limiter or RiotRateLimiter()

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or bool(getattr(self._session, "closed", False)) is True:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=20.0)
            )
            self._own_session = True
        return self._session

    async def close(self) -> None:
        """Closes the client session if owned."""
        if self._own_session and self._session and not self._session.closed:
            await self._session.close()

    async def _request(
        self, url: str, max_retries: int = 5
    ) -> Optional[Dict[str, Any]]:
        """Executes HTTP GET with rate limiting, retry-after handling, and exponential backoff."""
        session = await self._get_session()
        headers = {
            "X-Riot-Token": self.api_key,
            "Accept": "application/json",
            "User-Agent": "VALORANT-Discord-Leaderboard-Bot/1.0",
        }

        backoff = 1.0

        for attempt in range(1, max_retries + 1):
            await self.rate_limiter.acquire()

            try:
                async with session.get(url, headers=headers) as resp:
                    # Rate limit exceeded (429)
                    if resp.status == 429:
                        retry_after_header = resp.headers.get("Retry-After")
                        if retry_after_header:
                            try:
                                retry_after = float(retry_after_header)
                            except ValueError:
                                retry_after = backoff
                        else:
                            retry_after = backoff

                        # Add jitter
                        jitter = random.uniform(0.1, 0.5)
                        sleep_time = retry_after + jitter
                        logger.warning(
                            "HTTP 429 Rate limited on %s. Retrying in %.2fs (attempt %d/%d)",
                            url,
                            sleep_time,
                            attempt,
                            max_retries,
                        )
                        await asyncio.sleep(sleep_time)
                        backoff = min(backoff * 2, 32.0)
                        continue

                    # Server error (5xx)
                    if 500 <= resp.status < 600:
                        logger.warning(
                            "HTTP %d Server error on %s. Retrying in %.2fs (attempt %d/%d)",
                            resp.status,
                            url,
                            backoff,
                            attempt,
                            max_retries,
                        )
                        await asyncio.sleep(backoff + random.uniform(0.1, 0.3))
                        backoff = min(backoff * 2, 32.0)
                        continue

                    # Resource not found (404)
                    if resp.status == 404:
                        logger.info("HTTP 404 Not Found: %s", url)
                        return None

                    # Client error (400, 401, 403, etc.)
                    if resp.status >= 400:
                        text = await resp.text()
                        logger.error("HTTP %d error on %s: %s", resp.status, url, text)
                        raise RiotAPIError(resp.status, text)

                    return await resp.json()

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt == max_retries:
                    logger.error("Connection failed on %s after %d retries: %s", url, max_retries, exc)
                    raise
                logger.warning(
                    "Network error on %s: %s. Retrying in %.2fs (attempt %d/%d)",
                    url,
                    exc,
                    backoff,
                    attempt,
                    max_retries,
                )
                await asyncio.sleep(backoff + random.uniform(0.1, 0.3))
                backoff = min(backoff * 2, 32.0)

        raise RiotAPIError(429, f"Max retries exceeded for {url}")

    async def get_account_by_riot_id(
        self, game_name: str, tag_line: str, routing_region: str = "americas"
    ) -> Optional[Dict[str, Any]]:
        """Resolves gameName#tagLine to permanent PUUID via /riot/account/v1."""
        encoded_name = urllib.parse.quote(game_name.strip())
        encoded_tag = urllib.parse.quote(tag_line.strip())
        url = (
            f"https://{routing_region}.api.riotgames.com"
            f"/riot/account/v1/accounts/by-riot-id/{encoded_name}/{encoded_tag}"
        )
        return await self._request(url)

    async def get_matchlist_by_puuid(
        self, puuid: str, val_region: str = "na"
    ) -> Optional[Dict[str, Any]]:
        """Fetches recent match history via /val/match/v1/matchlists/by-puuid/{puuid}."""
        url = f"https://{val_region}.api.riotgames.com/val/match/v1/matchlists/by-puuid/{puuid}"
        return await self._request(url)

    async def get_match_details(
        self, match_id: str, val_region: str = "na"
    ) -> Optional[Dict[str, Any]]:
        """Fetches detailed match payload via /val/match/v1/matches/{matchId}."""
        url = f"https://{val_region}.api.riotgames.com/val/match/v1/matches/{match_id}"
        return await self._request(url)

    @staticmethod
    def extract_performance_score(player_data: Dict[str, Any]) -> Optional[float]:
        """Extracts performance rating from player stats.

        Prioritizes `stats.performanceScore` with fallback to `stats.performance_score`.
        """
        stats = player_data.get("stats", {})
        if not isinstance(stats, dict):
            return None

        # 1. Primary key
        if "performanceScore" in stats and stats["performanceScore"] is not None:
            try:
                return float(stats["performanceScore"])
            except (ValueError, TypeError):
                pass

        # 2. Fallback key
        if "performance_score" in stats and stats["performance_score"] is not None:
            try:
                return float(stats["performance_score"])
            except (ValueError, TypeError):
                pass

        return None

    async def get_active_act(
        self, val_region: str = "na"
    ) -> Tuple[Optional[str], Optional[str]]:
        """Resolves the current active VALORANT Act UUID and display name.

        First attempts Riot's official /val/content/v1/contents endpoint.
        Falls back to valorant-api.com seasons database if needed.
        Returns a tuple of (act_uuid, display_name).
        """
        import datetime

        # 1. Official Riot endpoint
        try:
            content_url = f"https://{val_region}.api.riotgames.com/val/content/v1/contents"
            content = await self._request(content_url)
            if content and "seasons" in content:
                for season in content["seasons"]:
                    is_active = season.get("isActive", False)
                    s_type = str(season.get("type", "")).lower()
                    if is_active and s_type == "act":
                        act_id = season.get("id")
                        act_name = season.get("name", "Current Act")
                        if act_id:
                            logger.info("Auto-detected active Act from Riot API: %s (%s)", act_name, act_id)
                            return act_id, act_name
        except Exception as exc:
            logger.warning("Could not fetch active season from Riot content API: %s. Trying fallback.", exc)

        # 2. Public valorant-api.com fallback
        try:
            session = await self._get_session()
            async with session.get("https://valorant-api.com/v1/seasons") as resp:
                if resp.status == 200:
                    payload = await resp.json()
                    seasons = payload.get("data", [])
                    now = datetime.datetime.now(datetime.timezone.utc)
                    for s in seasons:
                        if s.get("type") == "EAresSeasonType::Act" and s.get("startTime") and s.get("endTime"):
                            try:
                                s_start = datetime.datetime.fromisoformat(s["startTime"].replace("Z", "+00:00"))
                                s_end = datetime.datetime.fromisoformat(s["endTime"].replace("Z", "+00:00"))
                                if s_start <= now <= s_end:
                                    act_id = s.get("uuid")
                                    act_name = s.get("displayName", "Current Act")
                                    if act_id:
                                        logger.info("Auto-detected active Act from valorant-api.com: %s (%s)", act_name, act_id)
                                        return act_id, act_name
                            except (ValueError, TypeError):
                                continue
        except Exception as fallback_exc:
            logger.error("Failed to fetch active Act from fallback endpoint: %s", fallback_exc)

        return None, None
