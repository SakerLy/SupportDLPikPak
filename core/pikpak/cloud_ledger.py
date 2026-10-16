import json
import os
import threading

from config.settings import BASE_DIR
from core.logger import logger

# Sổ ghi các file mà TOOL đã restore lên cloud, theo từng account (device_id).
# Garbage Collector chỉ được xóa file có trong sổ này, để không bao giờ đụng
# tới file riêng của người dùng nằm trong thư mục gốc PikPak.
LEDGER_FILE = BASE_DIR / "cloud_temp.json"

_lock = threading.Lock()


def _load() -> dict:
    try:
        with open(LEDGER_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return {k: list(v) for k, v in data.items() if isinstance(v, list)}
    except FileNotFoundError:
        pass
    except Exception:
        logger.exception("Không đọc được cloud ledger")
    return {}


def _save(data: dict):
    data = {k: v for k, v in data.items() if v}
    try:
        tmp = LEDGER_FILE.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, LEDGER_FILE)
    except Exception:
        logger.exception("Không ghi được cloud ledger")


def add(account_key: str, file_id: str):
    if not account_key or not file_id:
        return
    with _lock:
        data = _load()
        ids = data.setdefault(account_key, [])
        if file_id not in ids:
            ids.append(file_id)
            _save(data)


def remove(account_key: str, file_ids):
    if not account_key:
        return
    if isinstance(file_ids, str):
        file_ids = [file_ids]
    drop = set(file_ids)
    with _lock:
        data = _load()
        if account_key not in data:
            return
        data[account_key] = [i for i in data[account_key] if i not in drop]
        _save(data)


def get(account_key: str) -> list:
    with _lock:
        return list(_load().get(account_key, []))


def contains(account_key: str, file_id: str) -> bool:
    return file_id in get(account_key)
