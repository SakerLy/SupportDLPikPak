import threading
import time
from typing import Optional, List
from config.settings import Config, console, Language
from core.api import PikPakAPI


class _AccountSlot:
    """1 slot = 1 account PikPak đã refresh token."""
    def __init__(self, refresh_token: str, device_id: str, index: int):
        self.index         = index
        self.refresh_token = refresh_token
        self.device_id     = device_id
        self.api: Optional[PikPakAPI] = None
        self.ready         = False
        self.in_use        = False
        self.error         = ""

    def authenticate(self) -> bool:
        """Tạo PikPakAPI mới và refresh token."""
        try:
            api = PikPakAPI()
            orig_token    = Config.REFRESH_TOKEN
            orig_device   = Config.DEVICE_ID
            Config.REFRESH_TOKEN = self.refresh_token
            Config.DEVICE_ID     = self.device_id
            ok = api.refresh_token()
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
            return False


class AccountPool:
    """
    Thread-safe pool quản lý nhiều PikPak account.
    Cấp phát account theo round-robin, tự fallback về account chính nếu pool trống.
    """

    def __init__(self):
        self._slots:  List[_AccountSlot] = []
        self._lock    = threading.Lock()
        self._rr_idx  = 0   # round-robin index

    # ── Load / reload ─────────────────────────────────────────────────────────

    def load(self, verbose: bool = False) -> int:
        """
        Đọc danh sách account từ Config.EXTRA_ACCOUNTS + account chính.
        Trả về số slot khả dụng.
        """
        slots = []

        # Account chính luôn ở slot 0
        if Config.REFRESH_TOKEN:
            slot = _AccountSlot(Config.REFRESH_TOKEN, Config.DEVICE_ID, 0)
            ok   = slot.authenticate()
            if verbose:
                icon = "✓" if ok else "✖"
                console.print(f"  [{('green' if ok else 'red')}]{icon} Account #0 (main)[/]")
            slots.append(slot)

        # Extra accounts
        for i, acc in enumerate(Config.EXTRA_ACCOUNTS, start=1):
            rt = acc.get("refresh_token", "")
            did = acc.get("device_id", "")
            if not rt:
                continue
            slot = _AccountSlot(rt, did, i)
            ok   = slot.authenticate()
            if verbose:
                icon = "✓" if ok else "✖"
                console.print(f"  [{('green' if ok else 'red')}]{icon} Account #{i}[/]")
            slots.append(slot)

        with self._lock:
            self._slots  = slots
            self._rr_idx = 0

        ready = sum(1 for s in slots if s.ready)
        return ready

    # ── Acquire / release ─────────────────────────────────────────────────────

    def acquire(self) -> Optional[PikPakAPI]:
        """
        Trả về PikPakAPI của slot tiếp theo sẵn sàng (round-robin).
        Nếu tất cả đều bận hoặc lỗi, trả về API của slot đầu tiên sẵn (blocking wait).
        """
        with self._lock:
            if not self._slots:
                return None

            ready = [s for s in self._slots if s.ready]
            if not ready:
                return None

            # Round-robin qua các slot sẵn sàng
            idx        = self._rr_idx % len(ready)
            slot       = ready[idx]
            self._rr_idx = (self._rr_idx + 1) % len(ready)
            return slot.api

    def size(self) -> int:
        """Số slot khả dụng."""
        with self._lock:
            return sum(1 for s in self._slots if s.ready)

    def all_apis(self) -> List[PikPakAPI]:
        """Tất cả API sẵn sàng (dùng để Restore song song)."""
        with self._lock:
            return [s.api for s in self._slots if s.ready and s.api]

    def reauth_all(self) -> None:
        """Re-authenticate tất cả slot (gọi khi token hết hạn)."""
        with self._lock:
            slots = list(self._slots)
        for slot in slots:
            slot.authenticate()

    def status_lines(self) -> List[str]:
        """Trả về list string mô tả trạng thái từng slot."""
        lines = []
        with self._lock:
            for s in self._slots:
                tag  = "MAIN" if s.index == 0 else f"#{s.index}"
                icon = "✓" if s.ready else "✖"
                col  = "green" if s.ready else "red"
                err  = f" — {s.error}" if s.error else ""
                lines.append(f"[{col}]{icon} Account {tag}{err}[/]")
        return lines


# ── Module-level singleton ────────────────────────────────────────────────────

_pool_instance: Optional[AccountPool] = None
_pool_lock     = threading.Lock()


def get_pool() -> AccountPool:
    """Singleton AccountPool — tạo 1 lần, dùng suốt session."""
    global _pool_instance
    with _pool_lock:
        if _pool_instance is None:
            _pool_instance = AccountPool()
    return _pool_instance


def reload_pool(verbose: bool = False) -> int:
    """Re-load pool từ Config hiện tại. Trả về số account khả dụng."""
    global _pool_instance
    with _pool_lock:
        _pool_instance = AccountPool()
    return _pool_instance.load(verbose=verbose)
