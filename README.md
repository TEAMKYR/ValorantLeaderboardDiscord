# 🎮 VALORANT Performance Score Discord Leaderboard Bot

A production-ready Discord bot written in Python (`discord.py`, `aiosqlite`) that tracks a pool of VALORANT players and renders fast, beautiful leaderboards for the active competitive Act based on Riot's new **Performance Score** (the 0–500 rating replacing ACS).

---

## ⚡ Architectural Highlights

1. **Decoupled Architecture**: Discord slash commands (`/leaderboard`) execute in **milliseconds** by querying a local, indexed SQLite database (`val_tracker.db`). They **never** make blocking on-demand requests to Riot's API during command execution.
2. **Asynchronous Background Ingestion**: A dedicated worker (`discord.ext.tasks.loop`) runs every 15 minutes to ingest new competitive matches into SQLite without blocking Discord gateway interactions.
3. **Dual-Window Rate Limiter & Backoff**: Built-in token bucket and sliding-window rate limiting respecting Riot's personal API keys (20 req/1s, 100 req/120s), with automatic `Retry-After` header parsing and exponential backoff on HTTP 429.
4. **Persistent Player Tracking**: Players are resolved and keyed by their immutable Riot `puuid`. Player name or tag changes never break historical statistics.
5. **Act & Mode Validation**: Strictly filters match history against the configured `ACT_ID` and competitive queue (`queueId == "competitive"`).

---

## 📂 Project Structure

```
DiscordLeaderboard/
├── .env.example              # Environment variable configuration template
├── requirements.txt          # Python dependencies
├── README.md                 # Setup and user documentation
├── config.py                 # Configuration loader and environment validator
├── database.py               # Asynchronous SQLite database layer (aiosqlite)
├── riot_client.py            # Riot Games REST client with dual-window rate limiting
├── bot.py                    # Runner alias
├── main.py                   # Discord client initialization, setup hook, error handlers
├── cogs/
│   └── tracker.py            # Slash commands (/leaderboard, /track_player, /sync_now) & background loop
└── tests/
    ├── test_database.py      # Automated tests for DB schema, upserts, aggregations
    ├── test_riot_client.py   # Automated tests for rate limiting, retry backoff, score parser
    └── test_tracker_cog.py   # Integration tests for match filtering and ingestion
```

---

## 🛠️ Requirements & Installation

### 1. Prerequisites
- **Python 3.10+** (Tested on Python 3.10 through 3.14)
- **Discord Bot Application** with `applications.commands` and `bot` scopes
- **Riot Games Developer API Key** from [developer.riotgames.com](https://developer.riotgames.com/)

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

---

## ⚙️ Environment Configuration

Copy `.env.example` to `.env`:

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```dotenv
# Discord Bot Credentials (from Discord Developer Portal)
DISCORD_BOT_TOKEN=your_discord_bot_token_here

# Riot Games API Key (from developer.riotgames.com)
RIOT_API_KEY=RGAPI-your-api-key-here

# Target VALORANT Act / Season UUID
# Set to 'auto' (default & recommended) for zero-maintenance auto-detection and rollover.
# Or specify an explicit Act UUID to lock to a specific season.
ACT_ID=auto

# Regional Match Endpoint: na, eu, ap, kr, latam, br
VAL_REGION=na

# Regional Account Routing: americas, europe, asia, esports
# (Use 'americas' for na/latam/br, 'europe' for eu, 'asia' for ap/kr)
ROUTING_REGION=americas

# Background Sync Frequency in Minutes (Default: 15)
SYNC_INTERVAL_MINUTES=15

# Minimum matches required to qualify for Average Leaderboard (Default: 1)
MIN_MATCHES=1

# SQLite Database File Path
DB_PATH=val_tracker.db
```

---

## 🚀 Running the Bot

Start the bot using either:

```bash
python main.py
```
or
```bash
python bot.py
```

On startup, the bot will:
1. Initialize the SQLite schema and indexes (`players`, `matches`, `player_match_stats`).
2. Load the `cogs.tracker` extension.
3. Automatically register and synchronize application slash commands globally with Discord.
4. Start the 15-minute background synchronization worker.

---

## 💬 Slash Commands

| Command | Permission | Description |
|---|---|---|
| `/leaderboard <metric>` | Everyone | Displays the current Act leaderboard for this server. Choices: `Summative Performance Score` (total cumulative score) or `Average Performance Score` (average 0–500 rating per match). Reads instantly from SQLite. |
| `/track_player <riot_id>` | Administrator | Resolves a player's Riot ID (`Name#Tag`) to their permanent PUUID, saves them to this server's tracking roster, and schedules an immediate match sync. |
| `/bulk_track_players <riot_ids>` *(or `/bulktrackplayers`)* | Administrator | Batch-adds multiple players to this server's tracking roster. Accepts comma, newline, or semicolon delimited lists (e.g. `TenZ#0001, Aspas#0001`). Provides a breakdown of successes vs. failed IDs and kicks off background sync. |
| `/untrack_player <riot_id>` | Administrator | Removes a player from this server's leaderboard tracking without deleting global match history. |
| `/tracked_players` | Everyone | Displays a list of all players currently tracked in this server. |
| `/set_daily_leaderboard <time> [channel] [timezone] [metric]` | Administrator | Schedules a recurring daily leaderboard update in a channel at a specific time (24-hr `HH:MM` format, e.g. `18:00`, `09:30`). Supports any IANA timezone (e.g. `UTC`, `America/New_York`, `US/Pacific`, `Europe/London`) and metric selection (`Both`, `Summative`, or `Average`). |
| `/remove_daily_leaderboard` | Administrator | Disables and deletes the scheduled daily leaderboard update for the current server. |
| `/view_daily_leaderboard` | Everyone | Displays the configured daily leaderboard schedule, channel, time, and last posted date for the current server. |
| `/sync_now` | Administrator | Triggers an immediate background match synchronization cycle without waiting for the 15-minute timer. Protected against concurrent runs. |

---

## 🗄️ Database Schema (`val_tracker.db`)

- **`players`**:
  - `puuid` (TEXT, PK): Permanent Riot unique identifier.
  - `game_name` (TEXT): Latest resolved Riot game name.
  - `tag_line` (TEXT): Latest resolved tagline.
  - `region` (TEXT): Player's match region (e.g. `na`).
  - `routing_region` (TEXT): Regional routing platform (e.g. `americas`).
  - `last_synced` (INTEGER): Epoch timestamp of last successful sync.

- **`guild_players`** *(Multi-Server Isolation)*:
  - `guild_id` (INTEGER, Indexed): Discord server ID.
  - `puuid` (TEXT, Indexed, FK -> `players.puuid`): Tracked player in this server.
  - Composite Primary Key on `(guild_id, puuid)`. Enables distinct leaderboards per server while deduplicating Riot API calls.

- **`matches`**:
  - `match_id` (TEXT, PK): Unique match UUID.
  - `season_id` (TEXT, Indexed): Act / Season UUID.
  - `game_start_time` (INTEGER): Epoch timestamp.
  - `queue_id` (TEXT): Game mode queue (e.g. `competitive`).

- **`player_match_stats`**:
  - `puuid` (TEXT, Indexed, FK -> `players.puuid`): Tracked player.
  - `match_id` (TEXT, FK -> `matches.match_id`): Match ID.
  - `performance_score` (REAL): Extracted performance rating (0–500 scale).
  - Composite Primary Key on `(puuid, match_id)`.

- **`scheduled_leaderboards`**:
  - `guild_id` (INTEGER, PK): Discord server identifier.
  - `channel_id` (INTEGER): Channel where daily updates are posted.
  - `hour` (INTEGER): Hour of day (0–23).
  - `minute` (INTEGER): Minute of day (0–59).
  - `timezone` (TEXT): IANA timezone name (default: `UTC`).
  - `metric` (TEXT): Leaderboard to post (`both`, `summative`, `average`).
  - `last_posted_date` (TEXT): Date string (`YYYY-MM-DD`) preventing duplicate posts on the same day.

---

## 🧪 Running Automated Tests

Run the full automated test suite using `pytest`:

```bash
python -m pytest tests/ -v
```

All 12 unit and integration tests validate:
- Player upserts and PUUID persistence
- Summative & Average SQL aggregations and `MIN_MATCHES` filtering
- Scheduled leaderboard persistence, timezone evaluation, and daily deduplication
- Match filtering against target `ACT_ID` and competitive queue
- Dual-window rate limit throttling and burst enforcement
- HTTP 429 backoff observing `Retry-After` header
- Performance score extraction logic (`stats.performanceScore` with fallback)
- Delimiter parsing and partial-success reporting for bulk tracking
