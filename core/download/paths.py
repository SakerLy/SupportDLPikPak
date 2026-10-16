import re
from pathlib import Path


_INVALID_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_RESERVED_NAMES = {
    "CON", "PRN", "AUX", "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}


def safe_name(name: str) -> str:
    """Tên file/thư mục hợp lệ trên Windows/Android/iOS, không thoát ra ngoài thư mục tải."""
    name = _INVALID_CHARS.sub("_", name or "").strip().rstrip(". ")
    if name in ("", ".", ".."):
        name = "_"
    if name.split(".")[0].upper() in _RESERVED_NAMES:
        name = "_" + name
    # Giới hạn 200 byte để còn chỗ cho hậu tố .tmp/.ckpt
    while len(name.encode("utf-8")) > 200:
        stem, dot, ext = name.rpartition(".")
        if dot and stem and len(ext) <= 10:
            name = stem[:-1] + "." + ext
        else:
            name = name[:-1]
    return name


def safe_rel_dir(path: str) -> Path:
    # file_data["path"] dạng "Thư mục/Con/file.ext" (luôn dùng "/")
    parts = [safe_name(p) for p in (path or "").split("/")[:-1] if p]
    return Path(*parts) if parts else Path()
