import asyncio
import re
import socket
import ssl
import time
from urllib.parse import quote, unquote, urlsplit

import aiohttp
from aiohttp_socks import ProxyConnector, ProxyType

from config.settings import Config, Language

PROXY_TYPES = ("http", "https", "socks4", "socks5")

_TYPE_MAP = {
    "http": ProxyType.HTTP,
    "https": ProxyType.HTTP,
    "socks4": ProxyType.SOCKS4,
    "socks5": ProxyType.SOCKS5,
}


def parse_proxy_string(text: str) -> dict:
    """Nhận các dạng proxy phổ biến, trả về dict type/host/port/username/password.

    Hỗ trợ:
      socks5://user:pass@host:port   http://host:port
      user:pass@host:port            host:port
      host:port:user:pass
    """
    text = (text or "").strip()
    if not text:
        raise ValueError("empty")

    ptype = None
    m = re.match(r"^([a-z0-9]+)://", text, re.I)
    if m:
        ptype = m.group(1).lower()
        if ptype == "socks5h":
            ptype = "socks5"
        if ptype == "socks4a":
            ptype = "socks4"
        if ptype not in PROXY_TYPES:
            raise ValueError(f"unsupported type: {ptype}")
        parts = urlsplit(text)
        host, port = parts.hostname, parts.port
        user = unquote(parts.username or "")
        pwd = unquote(parts.password or "")
    elif "@" in text:
        cred, _, addr = text.rpartition("@")
        user, _, pwd = cred.partition(":")
        host, _, port = addr.rpartition(":")
    else:
        pieces = text.split(":")
        if len(pieces) == 2:
            (host, port), user, pwd = pieces, "", ""
        elif len(pieces) == 4:
            host, port, user, pwd = pieces
        else:
            raise ValueError("unrecognized format")

    host = (host or "").strip().strip("[]")
    try:
        port = int(str(port).strip())
    except (TypeError, ValueError):
        raise ValueError("invalid port")
    if not host or not (1 <= port <= 65535):
        raise ValueError("invalid host/port")

    return {
        "type": ptype,
        "host": host,
        "port": str(port),
        "username": user,
        "password": pwd,
    }


def is_enabled() -> bool:
    return bool(
        Config.USE_PROXY
        and (Config.PROXY_HOST or "").strip()
        and str(Config.PROXY_PORT or "").strip()
    )


def display_url(mask_password: bool = True) -> str:
    ptype = (Config.PROXY_TYPE or "http").lower()
    auth = ""
    if Config.PROXY_USERNAME:
        pwd = "****" if mask_password else quote(Config.PROXY_PASSWORD or "", safe="")
        auth = f"{quote(Config.PROXY_USERNAME, safe='')}:{pwd}@"
    return f"{ptype}://{auth}{Config.PROXY_HOST}:{Config.PROXY_PORT}"


def make_connector(**kwargs) -> aiohttp.TCPConnector:
    """TCPConnector thường, hoặc ProxyConnector nếu đang bật proxy.

    Proxy nằm ở tầng connector nên mọi request (HTTP lẫn HTTPS) đều đi qua proxy,
    kể cả SOCKS — thứ mà aiohttp gốc không hỗ trợ.
    """
    if not is_enabled():
        return aiohttp.TCPConnector(**kwargs)

    ptype = (Config.PROXY_TYPE or "http").lower()
    # DNS do proxy phân giải (tránh lộ DNS và tránh DNS nội bộ bị chặn)
    kwargs.pop("resolver", None)
    kwargs.pop("family", None)
    proxy_ssl = None
    if ptype == "https":
        proxy_ssl = ssl.create_default_context()
        proxy_ssl.check_hostname = False
        proxy_ssl.verify_mode = ssl.CERT_NONE
    return ProxyConnector(
        proxy_type=_TYPE_MAP.get(ptype, ProxyType.HTTP),
        host=Config.PROXY_HOST.strip(),
        port=int(str(Config.PROXY_PORT).strip()),
        username=Config.PROXY_USERNAME or None,
        password=Config.PROXY_PASSWORD or None,
        rdns=True,
        proxy_ssl=proxy_ssl,
        **kwargs,
    )


def _explain(e: Exception) -> str:
    err = f"{type(e).__name__}: {e}"
    low = err.lower()
    if "407" in err or "authentication" in low or ("auth" in low and "fail" in low):
        return Language.get("px_err_auth")
    if isinstance(e, (asyncio.TimeoutError, TimeoutError)) or "timeout" in low:
        return Language.get("px_err_timeout")
    if "refused" in low or "cannot connect" in low or isinstance(e, socket.gaierror):
        return Language.get("px_err_refused")
    if "ssl" in low or "tunnel" in low or "connect" in low:
        return Language.get("px_err_tunnel")
    return err[:160]


async def test_proxy() -> list:
    """Chạy lần lượt các bước kiểm tra, trả về list (tên bước, ok, chi tiết)."""
    steps = [
        ("HTTP", "http://api.ipify.org?format=json"),
        ("HTTPS", "https://api.ipify.org?format=json"),
        ("PikPak", "https://api-drive.mypikpak.com/drive/v1/about"),
    ]
    results = []
    if not is_enabled():
        return [("Proxy", False, Language.get("px_not_configured"))]

    timeout = aiohttp.ClientTimeout(total=15, sock_connect=10)
    async with aiohttp.ClientSession(
        connector=make_connector(ssl=False), timeout=timeout
    ) as s:
        for name, url in steps:
            t0 = time.perf_counter()
            try:
                async with s.get(url) as r:
                    body = await r.text()
                    ms = int((time.perf_counter() - t0) * 1000)
                    if name == "PikPak":
                        # Chưa gửi token nên 401 vẫn là kết nối thành công
                        ok = r.status < 500
                        detail = f"HTTP {r.status} · {ms} ms"
                    else:
                        ok = r.status == 200
                        ip = ""
                        try:
                            ip = (await r.json(content_type=None)).get("ip", "")
                        except Exception:
                            ip = body[:40]
                        detail = f"IP {ip} · {ms} ms" if ok else f"HTTP {r.status}"
                    results.append((name, ok, detail))
            except Exception as e:
                results.append((name, False, _explain(e)))
                # HTTP đã hỏng thì các bước sau chắc chắn hỏng, dừng sớm
                if name == "HTTP":
                    break
    return results
