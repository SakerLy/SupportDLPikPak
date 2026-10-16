import hashlib
import json
from datetime import datetime, timedelta
from config.settings import BASE_DIR


class CacheManager:
    CACHE_DIR = BASE_DIR / ".cache"
    CACHE_DURATION = 3600

    @classmethod
    def init(cls):
        try:
            cls.CACHE_DIR.mkdir(exist_ok=True)
        except Exception:
            pass

    @classmethod
    def _get_cache_key(cls, *args):
        return hashlib.md5("_".join(str(arg) for arg in args).encode()).hexdigest()

    @classmethod
    def get(cls, cache_type, *args):
        cache_file = cls.CACHE_DIR / f"{cls._get_cache_key(cache_type, *args)}.json"
        if not cache_file.exists():
            return None
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            if datetime.now() > datetime.fromisoformat(
                data.get("expire_time", "2000-01-01")
            ):
                cache_file.unlink()
                return None
            return data.get("data")
        except Exception:
            return None

    @classmethod
    def set(cls, cache_type, data, *args, duration=None):
        cache_file = cls.CACHE_DIR / f"{cls._get_cache_key(cache_type, *args)}.json"
        if duration is None:
            duration = cls.CACHE_DURATION
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "data": data,
                        "expire_time": (
                            datetime.now() + timedelta(seconds=duration)
                        ).isoformat(),
                        "created_at": datetime.now().isoformat(),
                    },
                    f,
                )
            return True
        except Exception:
            return False

    @classmethod
    def clear_all(cls):
        try:
            for f in cls.CACHE_DIR.glob("*.json"):
                f.unlink()
            return True
        except Exception:
            return False

    @classmethod
    def get_cache_size(cls):
        t = 0
        c = 0
        try:
            for f in cls.CACHE_DIR.glob("*.json"):
                t += f.stat().st_size
                c += 1
            return t, c
        except Exception:
            return 0, 0
