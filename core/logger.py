import atexit
import json
import logging
import logging.handlers
import platform
import queue
import contextvars
import os
import sys
import tarfile
from datetime import datetime
from pathlib import Path

# Giống config.settings.BASE_DIR (không import trực tiếp vì module này phải chạy
# được trước khi các thư viện bên thứ ba như rich được cài)
if getattr(sys, "frozen", False):
    BASE_DIR = Path(sys.executable).parent
else:
    BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
LOG_PREFIX = "pikpak_tool_"
# Số lần chạy gần nhất được giữ log; cũ hơn sẽ bị xóa khi khởi động
MAX_LOG_RUNS = 30
# File log của lần chạy hiện tại, được gán trong init_logger()
LOG_FILE = None

# Cấp độ log, đặt bằng khóa "log_level" trong config.json (không phân biệt hoa thường).
# Mỗi cấp ghi chính nó và mọi cấp NẶNG hơn:
#   DEBUG   - mọi request HTTP, từng đoạn (range) tải, lần thử lại. File log rất lớn,
#             chỉ bật khi cần gửi log báo lỗi.
#   INFO    - (mặc định) các mốc chính: đăng nhập, tài khoản, bắt đầu/xong từng file,
#             dọn cloud, kiểm tra proxy.
#   WARNING - sự cố tự khắc phục được: thử lại, đổi account, proxy/mạng chập chờn.
#   ERROR   - chỉ những lỗi làm hỏng việc: file tải thất bại, ổ đầy, token hết hạn...
LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")
DEFAULT_LOG_LEVEL = "INFO"

# =========================================================================
# 1. KHỞI TẠO BIẾN NGỮ CẢNH (CONTEXT VARS) ĐỂ TRACING
# =========================================================================
# ContextVars là công cụ hoàn hảo trong Asyncio để "nhớ" trạng thái của từng
# luồng độc lập (không bị đụng chéo dữ liệu giữa các tiến trình tải).
ctx_file = contextvars.ContextVar("ctx_file", default="")
ctx_acc = contextvars.ContextVar("ctx_acc", default="")
ctx_range = contextvars.ContextVar("ctx_range", default="")


class ContextFilter(logging.Filter):
    """Custom Filter tự động lấy các biến ngữ cảnh đính kèm vào mỗi record"""

    def filter(self, record):
        f = ctx_file.get()
        a = ctx_acc.get()
        r = ctx_range.get()

        parts = []
        if a:
            parts.append(f"[Acc: {a}]")
        if f:
            parts.append(f"[File: {f}]")
        if r:
            parts.append(f"[Range: {r}]")

        prefix = " ".join(parts)
        # Tạo thêm tham số %(ctx)s để Formatter sử dụng nội suy chuỗi tự động
        record.ctx = f"{prefix} - " if prefix else ""
        return True


# =========================================================================
# 2. HÀM TỰ ĐỘNG NÉN FILE THÀNH .TAR.GZ KHI ROTATE ĐẠT NGƯỠNG
# =========================================================================
def namer_tar_gz(default_name):
    # Đổi tên file khi chuyển file log cũ (VD: pikpak_tool.log.1 -> .tar.gz)
    return default_name + ".tar.gz"


def rotator_tar_gz(source, dest):
    """Nén file cũ thành tar.gz và xóa file raw để tiết kiệm đĩa"""
    try:
        with open(source, "rb") as f_in:
            with tarfile.open(dest, "w:gz") as tar:
                # Bên trong file nén giữ đúng tên log của lần chạy
                tarinfo = tarfile.TarInfo(name=Path(source).name)
                tarinfo.size = os.path.getsize(source)
                tar.addfile(tarinfo, f_in)
        os.remove(source)
    except Exception:
        pass


# =========================================================================
# 3. CẤU HÌNH QUEUE HANDLER (NON-BLOCKING) VÀ ROTATING
# =========================================================================
logger = logging.getLogger("PikPakTool")
_log_queue = queue.Queue(-1)  # Hàng đợi không giới hạn
_queue_listener = None


def _new_log_path() -> Path:
    """Mỗi lần chạy tool một file riêng: logs/pikpak_tool_2026-09-24_14-05-33.log"""
    stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    path = LOG_DIR / f"{LOG_PREFIX}{stamp}.log"
    n = 1
    while path.exists():
        # Nhiều lần chạy trong cùng một giây: thêm số thứ tự cho khỏi ghi chung file
        path = LOG_DIR / f"{LOG_PREFIX}{stamp}_{n}.log"
        n += 1
    return path


def _cleanup_old_logs(keep: int = MAX_LOG_RUNS):
    """Chỉ giữ log của `keep` lần chạy gần nhất (kể cả các phần .tar.gz đã rotate)."""
    try:
        runs = {}
        for f in LOG_DIR.glob(f"{LOG_PREFIX}*.log*"):
            # pikpak_tool_<stamp>.log, .log.1.tar.gz ... thuộc cùng một lần chạy
            run_id = f.name.split(".log", 1)[0]
            runs.setdefault(run_id, []).append(f)
        # Tên chứa ngày giờ dạng YYYY-MM-DD_HH-MM-SS nên sort theo tên là theo thời gian
        for run_id in sorted(runs)[:-keep] if len(runs) > keep else []:
            for f in runs[run_id]:
                try:
                    f.unlink()
                except OSError:
                    pass
    except Exception:
        pass


def _stop_listener():
    # Đẩy hết log còn trong hàng đợi xuống đĩa trước khi thoát
    global _queue_listener
    if _queue_listener:
        try:
            _queue_listener.stop()
        except Exception:
            pass
        _queue_listener = None


def normalize_level(name) -> str:
    name = str(name or "").strip().upper()
    if name == "WARN":
        name = "WARNING"
    return name if name in LOG_LEVELS else DEFAULT_LOG_LEVEL


def _level_from_config() -> str:
    # Đọc thẳng config.json: lúc này config.settings có thể chưa import được
    try:
        with open(BASE_DIR / "config.json", "r", encoding="utf-8") as f:
            return normalize_level(json.load(f).get("log_level"))
    except Exception:
        return DEFAULT_LOG_LEVEL


def set_log_level(name) -> str:
    """Đổi cấp độ log ngay khi đang chạy. Trả về tên cấp độ đã áp dụng."""
    level = normalize_level(name)
    if logging.getLevelName(logger.level) != level:
        logger.setLevel(level)
        logger.info("Log level: %s", level)
    return level


def init_logger(level=None):
    global _queue_listener, LOG_FILE
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    LOG_FILE = _new_log_path()

    # Custom Formatter sử dụng biến %(ctx)s đã được Inject bởi ContextFilter
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s - %(ctx)s%(message)s",
        "%Y-%m-%d %H:%M:%S",
    )

    # Trong một lần chạy, file vẫn được cắt lúc 20MB (tối đa 10 phần nén)
    file_handler = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=20 * 1024 * 1024, backupCount=10, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    file_handler.namer = namer_tar_gz
    file_handler.rotator = rotator_tar_gz

    # Queue Handler đẩy Log vào bộ đệm RAM để không làm Block Asyncio (Async Safe)
    queue_handler = logging.handlers.QueueHandler(_log_queue)
    queue_handler.addFilter(ContextFilter())

    logger.handlers.clear()
    logger.setLevel(normalize_level(level) if level else _level_from_config())
    logger.addHandler(queue_handler)
    logger.propagate = False

    # Listener chạy ngầm trên một Background Thread riêng biệt để ghi ổ cứng
    _stop_listener()
    _queue_listener = logging.handlers.QueueListener(
        _log_queue, file_handler, respect_handler_level=True
    )
    _queue_listener.start()
    atexit.unregister(_stop_listener)
    atexit.register(_stop_listener)

    _cleanup_old_logs()

    logger.info(
        "Log: %s | Level: %s | Python %s | %s %s",
        LOG_FILE.name,
        logging.getLevelName(logger.level),
        platform.python_version(),
        platform.system(),
        platform.release(),
    )
    return LOG_FILE
