import sys
import json
import uuid
import os
import shutil
import urllib3
from pathlib import Path
from rich.console import Console
from core.logger import normalize_level, set_log_level
from config.i18n import STRINGS

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

APP_VERSION = "0.0.9"
APP_AUTHOR = "SakerLy"
GITHUB_ZIP_URL = "https://github.com/SakerLy/SupportDLPikPak/archive/refs/heads/main.zip"
GITHUB_VERSION_URL = "https://raw.githubusercontent.com/SakerLy/SupportDLPikPak/refs/heads/main/config/settings.py"
GITHUB_RELEASE_URL = "https://github.com/SakerLy/SupportDLPikPak/releases"

IS_FROZEN = getattr(sys, "frozen", False)
BASE_DIR = Path(sys.executable).parent if IS_FROZEN else Path(__file__).resolve().parent.parent

console = Console()


class Language:
    STRINGS = STRINGS

    @classmethod
    def get(cls, key, **kwargs):
        lang = Config.LANGUAGE
        text = cls.STRINGS.get(lang, cls.STRINGS["en"]).get(
            key, cls.STRINGS["en"].get(key, key)
        )
        if kwargs:
            try:
                return text.format(**kwargs)
            except (KeyError, IndexError, ValueError):
                return text
        return text


class Config:
    BASE_DIR = BASE_DIR
    CONFIG_FILE = BASE_DIR / "config.json"
    LOG_DIR = BASE_DIR / "logs"

    REFRESH_TOKEN = ""
    DEVICE_ID = ""
    MAIN_USER_ID = ""
    CAPTCHA_TOKEN = ""
    LANGUAGE = "en"
    USE_PROXY = False
    PROXY_TYPE = "http"
    PROXY_HOST = ""
    PROXY_PORT = ""
    PROXY_USERNAME = ""
    PROXY_PASSWORD = ""
    FORCE_PREMIUM_MODE = False
    MAX_WORKERS = 3
    DOWNLOAD_PATH_STR = "downloads"
    TIMEOUT = 30
    USE_CACHE = True
    CONCURRENT_THREADS = 8
    EXTRA_ACCOUNTS: list = []
    LOG_LEVEL = "INFO"

    # True nếu config.json không đọc được: khi đó KHÔNG ghi đè để khỏi mất token
    _LOAD_FAILED = False

    @classmethod
    def get_download_dir(cls):
        p = Path(os.path.expanduser(cls.DOWNLOAD_PATH_STR or "downloads"))
        return p if p.is_absolute() else cls.BASE_DIR / p

    @staticmethod
    def _int(value, default, lo=None, hi=None):
        try:
            v = int(value)
        except (TypeError, ValueError):
            return default
        if lo is not None:
            v = max(lo, v)
        if hi is not None:
            v = min(hi, v)
        return v

    @classmethod
    def load_config(cls):
        if cls.CONFIG_FILE.exists():
            try:
                with open(cls.CONFIG_FILE, "r", encoding="utf-8") as f:
                    d = json.load(f)
                if not isinstance(d, dict):
                    raise ValueError("config root is not an object")
            except Exception:
                # File hỏng: sao lưu lại một lần rồi dùng giá trị mặc định
                if not cls._LOAD_FAILED:
                    try:
                        shutil.copy2(
                            cls.CONFIG_FILE, cls.CONFIG_FILE.with_suffix(".json.bak")
                        )
                    except Exception:
                        pass
                cls._LOAD_FAILED = True
                d = {}
            else:
                cls._LOAD_FAILED = False

            cls.REFRESH_TOKEN = d.get("refresh_token", "") or ""
            cls.DEVICE_ID = d.get("device_id", "") or ""
            cls.MAIN_USER_ID = d.get("main_user_id", "") or ""
            cls.CAPTCHA_TOKEN = d.get("captcha_token", "") or ""
            cls.MAX_WORKERS = cls._int(d.get("max_workers"), 3, lo=1, hi=32)
            cls.DOWNLOAD_PATH_STR = d.get("download_path", "downloads") or "downloads"
            cls.TIMEOUT = cls._int(d.get("timeout"), 30, lo=5, hi=600)
            cls.USE_CACHE = bool(d.get("use_cache", True))
            lang = d.get("language", "en")
            cls.LANGUAGE = lang if lang in Language.STRINGS else "en"
            cls.USE_PROXY = bool(d.get("use_proxy", False))
            ptype = str(d.get("proxy_type", "http") or "http").lower()
            cls.PROXY_TYPE = (
                ptype if ptype in ("http", "https", "socks4", "socks5") else "http"
            )
            cls.PROXY_HOST = str(d.get("proxy_host", "") or "")
            cls.PROXY_PORT = str(d.get("proxy_port", "") or "")
            cls.PROXY_USERNAME = str(d.get("proxy_username", "") or "")
            cls.PROXY_PASSWORD = str(d.get("proxy_password", "") or "")
            cls.CONCURRENT_THREADS = cls._int(
                d.get("concurrent_threads"), 8, lo=1, hi=64
            )
            cls.FORCE_PREMIUM_MODE = bool(d.get("force_premium_mode", False))
            accs = d.get("extra_accounts", [])
            cls.EXTRA_ACCOUNTS = [a for a in accs if isinstance(a, dict)] if isinstance(accs, list) else []
            cls.LOG_LEVEL = normalize_level(d.get("log_level"))
            set_log_level(cls.LOG_LEVEL)
        if not cls.DEVICE_ID:
            cls.DEVICE_ID = uuid.uuid4().hex

    @classmethod
    def save_config(cls):
        try:
            data = {
                "refresh_token": cls.REFRESH_TOKEN,
                "device_id": cls.DEVICE_ID,
                "main_user_id": cls.MAIN_USER_ID,
                "captcha_token": cls.CAPTCHA_TOKEN,
                "max_workers": cls.MAX_WORKERS,
                "download_path": cls.DOWNLOAD_PATH_STR,
                "timeout": cls.TIMEOUT,
                "use_cache": cls.USE_CACHE,
                "language": cls.LANGUAGE,
                "use_proxy": cls.USE_PROXY,
                "proxy_type": cls.PROXY_TYPE,
                "proxy_host": cls.PROXY_HOST,
                "proxy_port": cls.PROXY_PORT,
                "proxy_username": cls.PROXY_USERNAME,
                "proxy_password": cls.PROXY_PASSWORD,
                "concurrent_threads": cls.CONCURRENT_THREADS,
                "force_premium_mode": cls.FORCE_PREMIUM_MODE,
                "extra_accounts": cls.EXTRA_ACCOUNTS,
                "log_level": cls.LOG_LEVEL,
            }
            # Ghi ra file tạm rồi thay thế, tránh config.json bị cụt khi tắt ngang
            tmp = cls.CONFIG_FILE.with_suffix(".json.tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            os.replace(tmp, cls.CONFIG_FILE)
            cls._LOAD_FAILED = False
            return True
        except Exception:
            return False

    @classmethod
    def migrate_config(cls):
        if not cls.CONFIG_FILE.exists():
            return
        cls.load_config()
        if not cls._LOAD_FAILED:
            cls.save_config()

    @classmethod
    def setup_dirs(cls):
        try:
            cls.get_download_dir().mkdir(parents=True, exist_ok=True)
            return True
        except Exception:
            return False
