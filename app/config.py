import os
from pathlib import Path


def _load_dotenv() -> None:
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Environment variable {name} is not set")
    return value


def _admin_ids_from_env(default_ids):
    raw = os.getenv("ADMIN_USER_IDS", "").strip()
    if not raw:
        return set(default_ids)
    result = set()
    for part in raw.split(","):
        part = part.strip()
        if part:
            result.add(int(part))
    return result


_load_dotenv()

TELEGRAM_BOT_TOKEN = _required_env("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = _required_env("GEMINI_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash").strip() or "gemini-2.5-flash"

DB_PATH_HELP = os.getenv("DB_PATH_HELP", "help_feature.sqlite3").strip() or "help_feature.sqlite3"

ADMIN_USER_IDS = _admin_ids_from_env(
    {
        1966417024,
        593855503,
    }
)

ADMIN_IDS = list(ADMIN_USER_IDS)
