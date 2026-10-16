import aiohttp
import asyncio
import json
import logging
from config.settings import Config
from core.logger import logger
from core.net import proxy as proxy_mod


class HttpClient:
    # Một ClientSession dùng chung cho toàn bộ API call (theo từng event loop),
    # tránh mở connector + TLS handshake mới cho mỗi request.
    _session: aiohttp.ClientSession = None
    _loop_id: int = None
    _proxy_sig: str = None

    @classmethod
    def _get_session(cls) -> aiohttp.ClientSession:
        loop_id = id(asyncio.get_running_loop())
        proxy_sig = proxy_mod.display_url(mask_password=False) if proxy_mod.is_enabled() else ""
        if (
            cls._session is None
            or cls._session.closed
            or cls._loop_id != loop_id
            or cls._proxy_sig != proxy_sig
        ):
            # Session cũ thuộc cấu hình proxy trước: đóng ngầm, không chờ
            if cls._session and not cls._session.closed and cls._loop_id == loop_id:
                asyncio.ensure_future(cls._session.close())
            cls._session = aiohttp.ClientSession(
                connector=proxy_mod.make_connector(ssl=False, limit=64, ttl_dns_cache=300)
            )
            cls._loop_id = loop_id
            cls._proxy_sig = proxy_sig
        return cls._session

    @classmethod
    async def close(cls):
        if (
            cls._session
            and not cls._session.closed
            and cls._loop_id == id(asyncio.get_running_loop())
        ):
            await cls._session.close()
        cls._session = None
        cls._loop_id = None
        cls._proxy_sig = None

    @staticmethod
    async def request(
        method,
        url,
        headers=None,
        params=None,
        json_data=None,
        form_data=None,
        timeout: float = None,
    ):
        logger.debug("HTTP request %s %s", method, url)
        try:
            session = HttpClient._get_session()
            async with session.request(
                method,
                url,
                headers=headers,
                params=params,
                json=json_data,
                data=form_data,
                timeout=aiohttp.ClientTimeout(
                    total=timeout or Config.TIMEOUT, connect=10
                ),
            ) as response:
                text = await response.text()
                try:
                    data = json.loads(text) if text else None
                except ValueError:
                    data = None
                if response.status >= 400:
                    logger.debug(
                        "HTTP %s %s -> %s: %s", method, url, response.status, text[:300]
                    )
                return response.status, data, text
        except Exception as e:
            # Lỗi mạng thường chỉ là tạm thời và được gọi lại, nên là WARNING;
            # traceback đầy đủ chỉ ghi khi bật DEBUG
            logger.warning(
                "HTTP request failed %s %s: %s: %s",
                method,
                url,
                type(e).__name__,
                e,
                exc_info=logger.isEnabledFor(logging.DEBUG),
            )
            return 0, None, str(e)
