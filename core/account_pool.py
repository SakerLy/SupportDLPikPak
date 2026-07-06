import threading
import uuid
from typing import Optional, List
from config.settings import Config, console
from core.api import PikPakAPI
from core.logger import logger


class _AccountSlot:
    def __init__(self, refresh_token: str, device_id: str, index: int):
        self.index = index
        self.api = PikPakAPI(
            refresh_token=refresh_token,
            device_id=device_id or uuid.uuid4().hex,
            on_token_update=self._persist_token,
        )
        self.ready = False
        self.error = ""

    def _persist_token(self, new_token: str):
        # Token xoay vòng: ghi lại vào config để lần chạy sau không bị chết token
        try:
            if self.index == 0:
                Config.REFRESH_TOKEN = new_token
            else:
                Config.EXTRA_ACCOUNTS[self.index - 1]["refresh_token"] = new_token
            Config.save_config()
        except (IndexError, KeyError):
            pass

    async def authenticate(self) -> bool:
        try:
            self.ready = await self.api.refresh_token()
            self.error = "" if self.ready else "Token refresh failed"
        except Exception as e:
            self.ready = False
            self.error = str(e)
            logger.exception("Account auth failed for slot %s", self.index)
        return self.ready


class AccountPool:
    def __init__(self):
        self._slots: List[_AccountSlot] = []
        self._lock = threading.Lock()
        self._rr_idx = 0

    async def load(self, verbose: bool = False) -> int:
        Config.load_config()
        slots = []

        if Config.REFRESH_TOKEN:
            slots.append(_AccountSlot(Config.REFRESH_TOKEN, Config.DEVICE_ID, 0))
        for i, acc in enumerate(Config.EXTRA_ACCOUNTS, start=1):
            rt = acc.get("refresh_token", "")
            if rt:
                slots.append(_AccountSlot(rt, acc.get("device_id", ""), i))

        for slot in slots:
            ok = await slot.authenticate()
            if verbose:
                tag = "(main)" if slot.index == 0 else ""
                console.print(
                    f"  [{'green' if ok else 'red'}]{'✓' if ok else '✖'} Account #{slot.index} {tag}[/]"
                )

        with self._lock:
            self._slots = slots
            self._rr_idx = 0

        ready = sum(1 for s in slots if s.ready)
        logger.info("Account pool loaded: %s ready", ready)
        return ready

    def size(self) -> int:
        with self._lock:
            return sum(1 for s in self._slots if s.ready)

    def all_apis(self) -> List[PikPakAPI]:
        with self._lock:
            return [s.api for s in self._slots if s.ready]

    def acquire(self) -> Optional[PikPakAPI]:
        with self._lock:
            ready = [s for s in self._slots if s.ready]
            if not ready:
                return None
            idx = self._rr_idx % len(ready)
            self._rr_idx = (self._rr_idx + 1) % len(ready)
            return ready[idx].api

    def status_lines(self) -> List[str]:
        lines = []
        with self._lock:
            for s in self._slots:
                tag = "MAIN" if s.index == 0 else f"#{s.index}"
                icon = "✓" if s.ready else "✖"
                col = "green" if s.ready else "red"
                err = f" — {s.error}" if s.error else ""
                lines.append(f"[{col}]{icon} Account {tag}{err}[/]")
        return lines


_pool_instance: Optional[AccountPool] = None
_pool_lock = threading.Lock()


def get_pool() -> AccountPool:
    global _pool_instance
    with _pool_lock:
        if _pool_instance is None:
            _pool_instance = AccountPool()
    return _pool_instance


async def reload_pool(verbose: bool = False) -> int:
    return await get_pool().load(verbose=verbose)
