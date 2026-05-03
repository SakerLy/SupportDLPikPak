import threading
import asyncio
from typing import Optional, List
from config.settings import Config, console
from core.api import PikPakAPI
from core.logger import logger

class _AccountSlot:
    def __init__(self, refresh_token: str, device_id: str, index: int):
        self.index         = index
        self.refresh_token = refresh_token
        self.device_id     = device_id
        self.api: Optional[PikPakAPI] = None
        self.ready         = False
        self.error         = ""

    async def authenticate(self) -> bool:
        try:
            api = PikPakAPI()
            orig_token  = Config.REFRESH_TOKEN
            orig_device = Config.DEVICE_ID
            Config.REFRESH_TOKEN = self.refresh_token
            Config.DEVICE_ID     = self.device_id
            ok = await api.refresh_token()
            Config.REFRESH_TOKEN = orig_token
            Config.DEVICE_ID     = orig_device
            if ok:
                self.api   = api
                self.ready = True
                self.error = ""
            else:
                self.ready = False
                self.error = "Token refresh failed"
            return ok
        except Exception as e:
            self.ready = False
            self.error = str(e)
            logger.exception("Account auth failed for slot %s", self.index)
            return False

class AccountPool:
    def __init__(self):
        self._slots:  List[_AccountSlot] = []
        self._lock    = threading.Lock()
        self._rr_idx  = 0

    async def load(self, verbose: bool = False) -> int:
        slots = []
        if Config.REFRESH_TOKEN:
            slot = _AccountSlot(Config.REFRESH_TOKEN, Config.DEVICE_ID, 0)
            ok   = await slot.authenticate()
            if verbose:
                console.print(f"  [{'green' if ok else 'red'}]{'✓' if ok else '✖'} Account #0 (main)[/]")
            slots.append(slot)

        for i, acc in enumerate(Config.EXTRA_ACCOUNTS, start=1):
            rt  = acc.get("refresh_token", "")
            did = acc.get("device_id", "")
            if not rt: continue
            slot = _AccountSlot(rt, did, i)
            ok   = await slot.authenticate()
            if verbose:
                console.print(f"  [{'green' if ok else 'red'}]{'✓' if ok else '✖'} Account #{i}[/]")
            slots.append(slot)

        with self._lock:
            self._slots  = slots
            self._rr_idx = 0

        ready = sum(1 for s in slots if s.ready)
        logger.info("Account pool loaded: %s ready", ready)
        return ready

    def size(self) -> int:
        with self._lock: return sum(1 for s in self._slots if s.ready)

    def all_apis(self) -> List[PikPakAPI]:
        with self._lock: return [s.api for s in self._slots if s.ready and s.api]

    def acquire(self) -> Optional[PikPakAPI]:
        with self._lock:
            ready = [s for s in self._slots if s.ready]
            if not ready: return None
            idx = self._rr_idx % len(ready)
            self._rr_idx = (self._rr_idx + 1) % len(ready)
            return ready[idx].api

    async def get_stripe_urls_async(self, get_url_fn_per_api) -> List[Optional[str]]:
        apis = self.all_apis()
        if not apis: return []

        async def _fetch(api: PikPakAPI):
            try: return await get_url_fn_per_api(api)
            except Exception: return None

        results = await asyncio.gather(*[asyncio.create_task(_fetch(a)) for a in apis], return_exceptions=True)
        return [r if not isinstance(r, Exception) else None for r in results]

    def status_lines(self) -> List[str]:
        lines = []
        with self._lock:
            for s in self._slots:
                tag  = "MAIN" if s.index == 0 else f"#{s.index}"
                icon = "✓" if s.ready else "✖"
                col  = "green" if s.ready else "red"
                err  = f" — {s.error}" if s.error else ""
                lines.append(f"[{col}]{icon} Account {tag}{err}[/]")
        return lines

_pool_instance: Optional[AccountPool] = None
_pool_lock     = threading.Lock()

def get_pool() -> AccountPool:
    global _pool_instance
    with _pool_lock:
        if _pool_instance is None:
            _pool_instance = AccountPool()
    return _pool_instance

async def reload_pool(verbose: bool = False) -> int:
    global _pool_instance
    with _pool_lock:
        _pool_instance = AccountPool()
    return await _pool_instance.load(verbose=verbose)