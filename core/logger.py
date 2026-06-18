import logging
import logging.handlers
import queue
import contextvars
import os
import tarfile
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / "pikpak_tool.log"

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
                # Đặt tên file bên trong file nén cho gọn gàng
                tarinfo = tarfile.TarInfo(name="pikpak_tool.log")
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


def init_logger():
    global _queue_listener
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    # Custom Formatter sử dụng biến %(ctx)s đã được Inject bởi ContextFilter
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s - %(ctx)s%(message)s",
        "%Y-%m-%d %H:%M:%S",
    )

    # File Handler cắt đĩa lúc 20MB, giữ lại 10 file gần nhất (Retention = 10)
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
    logger.setLevel(logging.DEBUG)
    logger.addHandler(queue_handler)
    logger.propagate = False

    # Listener chạy ngầm trên một Background Thread riêng biệt để ghi ổ cứng
    if _queue_listener:
        _queue_listener.stop()
    _queue_listener = logging.handlers.QueueListener(
        _log_queue, file_handler, respect_handler_level=True
    )
    _queue_listener.start()

    logger.debug("Hệ thống Async Logger (Non-Blocking) đã khởi chạy thành công")
    return LOG_FILE
