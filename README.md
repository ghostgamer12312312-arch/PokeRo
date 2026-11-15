# PokeRo Telegram Bot

Python Telegram Pokémon bot (`PokeRo.py`) with a Turso cloud database for
backups, so it can run on any host (Termux, NexCloud/Pterodactyl, VPS).

## Host settings (NexCloud / Pterodactyl)

- **Language / Egg:** Python
- **Docker image:** Python 3.12
- **Startup command:** `python PokeRo.py`
- **Branch:** `main`
- **Additional packages (only if your egg doesn't auto-read requirements.txt):**
  `python-telegram-bot aiohttp httpx python-dotenv Pillow`

## Install dependencies

```
pip install -r requirements.txt
```

## Environment (.env)

Copy `.env.example` to `.env` and fill in the values, or paste them into the
panel's startup variables:

| Key | Required | Meaning |
| --- | --- | --- |
| `BOT_TOKEN` | yes | Telegram bot token from @BotFather |
| `TURSO_DATABASE_URL` | yes | e.g. `libsql://your-db-your-org.aws-ap-northeast-1.turso.io` |
| `TURSO_AUTH_TOKEN` | yes | Turso database auth token |
| `TURSO_BACKUP_INTERVAL` | no | Seconds between cloud backups (default 300) |
| `OWNER_ID` | yes | Your Telegram numeric ID (owner) |
| `ADMIN_IDS` | no | Extra admins, comma/space separated |
| `HEAD_ADMIN_IDS` | no | Head admins (defaults to admins) |
| `QUERY_ADMIN_GC` | no | Where /query reports are sent (defaults to owner) |
| `RENAME_ADMIN_GC` | no | Where rename requests are sent (defaults to owner) |
| `CAPTCHA_POLICE_CHAT_ID` | no | CAPTCHA report target (defaults to owner) |

## How the database works

The bot uses local SQLite for speed (`ZNX.db`, `t1727372828728stereolodu8.db`).
`turso_sync.py` automatically:

- on first boot of a fresh machine: **restores** your data from Turso;
- on boot when local data exists: **pushes** it to Turso;
- every `TURSO_BACKUP_INTERVAL` seconds: pushes a fresh backup;
- on clean shutdown: pushes one final backup.

`pokeapi_cache.db` is a rebuildable cache and stays local only.

Manual backup / restore helpers:

```
python turso_sync.py push
python turso_sync.py pull
python turso_sync.py status
```

## Run

```
python PokeRo.py
```
