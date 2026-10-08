"""Automated tests for Riot API client rate limiting, retry-after backoff, and data parsing."""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock
import pytest
from riot_client import RiotClient, RiotRateLimiter, RiotAPIError


def test_extract_performance_score():
    """Verify performance rating extraction with primary and fallback keys."""
    # 1. Primary key
    data1 = {"stats": {"performanceScore": 425.5, "score": 300}}
    assert RiotClient.extract_performance_score(data1) == 425.5

    # 2. Fallback key
    data2 = {"stats": {"performance_score": 380, "score": 250}}
    assert RiotClient.extract_performance_score(data2) == 380.0

    # 3. Primary takes precedence over fallback
    data3 = {"stats": {"performanceScore": 400.0, "performance_score": 350.0}}
    assert RiotClient.extract_performance_score(data3) == 400.0

    # 4. Missing / None
    assert RiotClient.extract_performance_score({}) is None
    assert RiotClient.extract_performance_score({"stats": {}}) is None
    assert RiotClient.extract_performance_score({"stats": {"performanceScore": None}}) is None
    assert RiotClient.extract_performance_score({"stats": "invalid_type"}) is None


@pytest.mark.asyncio
async def test_rate_limiter_burst_enforcement():
    """Verify rate limiter delays requests when exceeding the burst limit."""
    limiter = RiotRateLimiter(limit_1s=3, limit_120s=100)

    start = time.monotonic()
    # Acquire 3 tokens immediately
    await limiter.acquire()
    await limiter.acquire()
    await limiter.acquire()
    elapsed_immediate = time.monotonic() - start
    assert elapsed_immediate < 0.2

    # 4th acquire must wait until the 1-second window clears
    await limiter.acquire()
    elapsed_total = time.monotonic() - start
    assert elapsed_total >= 0.95


@pytest.mark.asyncio
async def test_get_account_by_riot_id():
    """Verify account resolution URL construction and response handling."""
    mock_session = MagicMock()
    mock_session.closed = False

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={
        "puuid": "test-puuid-123",
        "gameName": "TenZ",
        "tagLine": "0001"
    })

    # Async context manager for session.get
    cm = AsyncMock()
    cm.__aenter__.return_value = mock_response
    mock_session.get.return_value = cm

    client = RiotClient(api_key="RGAPI-test", session=mock_session)
    result = await client.get_account_by_riot_id("TenZ", "0001", routing_region="americas")

    assert result is not None
    assert result["puuid"] == "test-puuid-123"
    assert result["gameName"] == "TenZ"
    mock_session.get.assert_called_once()
    called_url = mock_session.get.call_args[0][0]
    assert "https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/TenZ/0001" == called_url


@pytest.mark.asyncio
async def test_429_retry_after_handling():
    """Verify client observes Retry-After header on HTTP 429."""
    mock_session = MagicMock()
    mock_session.closed = False

    # Response 1: 429 with Retry-After: 0.1
    resp_429 = AsyncMock()
    resp_429.status = 429
    resp_429.headers = {"Retry-After": "0.1"}
    cm_429 = AsyncMock()
    cm_429.__aenter__.return_value = resp_429

    # Response 2: 200 Success
    resp_200 = AsyncMock()
    resp_200.status = 200
    resp_200.json = AsyncMock(return_value={"history": []})
    cm_200 = AsyncMock()
    cm_200.__aenter__.return_value = resp_200

    mock_session.get.side_effect = [cm_429, cm_200]

    client = RiotClient(api_key="RGAPI-test", session=mock_session)

    start = time.monotonic()
    result = await client.get_matchlist_by_puuid("test-puuid-123", val_region="na")
    elapsed = time.monotonic() - start

    assert result == {"history": []}
    assert elapsed >= 0.1  # Verified delay was honored
    assert mock_session.get.call_count == 2
