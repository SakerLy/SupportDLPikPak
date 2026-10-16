import asyncio
import errno
import json
import mmap
import random
import socket
import time
from pathlib import Path

import aiofiles
import aiohttp
from aiohttp import ClientTimeout

from config.settings import Config, console, Language
from core.net import proxy as proxy_mod
from core.logger import logger, ctx_acc, ctx_range


class DiskFullError(Exception):
    pass


class RangeNotSupported(Exception):
    pass


# TINH CHỈNH ĐỂ CHỐNG RỚT TỐC ĐỘ VỀ CUỐI
MIN_CHUNK_SIZE = 1 * 1024 * 1024  # 1MB
MAX_CHUNK_SIZE = (
    16 * 1024 * 1024
)  # Capped 16MB để đảm bảo luồng tải được chia đều đến giây cuối cùng
START_CHUNK_SIZE = 4 * 1024 * 1024  # 4MB

_503_BASE_DELAY = 3.0
_503_MAX_DELAY = 30.0
_503_MAX_RETRY = 6


def _jitter(base: float) -> float:
    return base + random.uniform(0, base * 0.5)


def _make_connector() -> aiohttp.TCPConnector:
    # ThreadedResolver (DNS của hệ điều hành) + cache 5 phút là đủ vì chỉ có vài host.
    # Không dùng aiodns: c-ares hay lỗi "Could not contact DNS servers" trên
    # Windows/VPN, khi đó mọi kết nối đều hỏng.
    resolver = aiohttp.ThreadedResolver()

    # Khi bật proxy, connector sẽ là ProxyConnector (hỗ trợ http/https/socks4/socks5)
    return proxy_mod.make_connector(
        resolver=resolver,
        limit=0,
        limit_per_host=0,
        ssl=False,
        keepalive_timeout=60,
        ttl_dns_cache=300,
        family=socket.AF_INET,
    )


def _stream_timeout() -> ClientTimeout:
    # Không giới hạn tổng thời gian (file lớn tải lâu là bình thường),
    # chỉ cắt khi kết nối/đọc bị treo
    return ClientTimeout(
        total=None, sock_connect=15, sock_read=max(60, Config.TIMEOUT)
    )


class SegmentedDownloadMixin:
    _dl_sess: aiohttp.ClientSession = None
    _dl_sess_key = None

    def _dl_session(self) -> aiohttp.ClientSession:
        # Dùng chung cho mọi file trong một lượt tải: giữ kết nối keep-alive,
        # khỏi bắt tay lại TCP/TLS (và proxy) cho từng file
        key = (
            id(asyncio.get_running_loop()),
            proxy_mod.display_url(mask_password=False) if proxy_mod.is_enabled() else "",
        )
        if self._dl_sess is None or self._dl_sess.closed or self._dl_sess_key != key:
            self._dl_sess = aiohttp.ClientSession(
                connector=_make_connector(),
                timeout=_stream_timeout(),
                read_bufsize=1024 * 1024,
            )
            self._dl_sess_key = key
        return self._dl_sess

    async def close_session(self):
        if self._dl_sess and not self._dl_sess.closed:
            await self._dl_sess.close()
        self._dl_sess = None
        self._dl_sess_key = None

    @staticmethod
    def _merge_ranges(ranges):
        if not ranges:
            return []
        ranges = sorted(ranges, key=lambda x: x[0])
        merged = [ranges[0].copy()]
        for r in ranges[1:]:
            last = merged[-1]
            if r[0] <= last[1] + 1:
                last[1] = max(last[1], r[1])
            else:
                merged.append(r.copy())
        return merged

    @staticmethod
    def _get_gaps(total_size, completed_ranges):
        merged = SegmentedDownloadMixin._merge_ranges(completed_ranges)
        gaps = []
        current = 0
        for r in merged:
            if r[0] > current:
                gaps.append([current, r[0] - 1])
            current = r[1] + 1
        if current < total_size:
            gaps.append([current, total_size - 1])
        return gaps

    # -------------------------------------------------------------------------
    # HÀM TẢI 1 CHUNK - KHÔNG UI CẬP NHẬT Ở ĐÂY ĐỂ TRÁNH NHẢY LOẠN
    # -------------------------------------------------------------------------
    async def _fetch_segment(
        self,
        session: aiohttp.ClientSession,
        url: str,
        headers: dict,
        mm: mmap.mmap,
        worker_id: int,
        seg_start: int,
        seg_end: int,
        seg_progress: dict,
        url_holders: list,
        url_lock: asyncio.Lock,
    ) -> bool:
        h = headers.copy()
        h["Range"] = f"bytes={seg_start}-{seg_end}"

        for attempt in range(_503_MAX_RETRY):
            if self.cancel_event.is_set():
                return False
            if attempt > 0:
                async with url_lock:
                    n = len(url_holders)
                    url = url_holders[attempt % n] if n > 1 else url_holders[0]

            attempt_bytes = 0
            try:
                async with session.get(url, headers=h) as r:
                    if r.status in (503, 429):
                        logger.debug(f"HTTP {r.status}, chờ rồi thử lại (lần {attempt + 1})")
                        await asyncio.sleep(
                            min(_jitter(_503_BASE_DELAY * (2**attempt)), _503_MAX_DELAY)
                        )
                        continue
                    if r.status in (401, 403, 500, 502, 504):
                        logger.debug(f"HTTP {r.status}, thử lại (lần {attempt + 1})")
                        await asyncio.sleep(_jitter(1.5**attempt))
                        continue

                    r.raise_for_status()
                    # Server bỏ qua Range và trả cả file: chỉ dùng được khi đoạn bắt đầu từ 0
                    if r.status != 206 and seg_start != 0:
                        raise RangeNotSupported(f"HTTP {r.status} cho Range {seg_start}-{seg_end}")

                    offset = seg_start
                    end = seg_end + 1
                    async for chunk in r.content.iter_any():
                        if self.cancel_event.is_set():
                            return False
                        if not chunk:
                            continue

                        chunk = chunk[: end - offset]
                        chunk_len = len(chunk)
                        try:
                            mm[offset : offset + chunk_len] = chunk
                        except ValueError:
                            return False
                        except OSError as e:
                            if e.errno == errno.ENOSPC:
                                self.cancel_event.set()
                                logger.error("Lỗi: Ổ cứng đầy! Dừng nạp chunk.")
                                raise DiskFullError("No space left on device")
                            logger.exception("Lỗi I/O Mmap")
                            raise

                        offset += chunk_len
                        attempt_bytes += chunk_len
                        seg_progress[worker_id] = attempt_bytes
                        if offset >= end:
                            break

                if offset >= end:
                    return True
                # Kết nối đóng sớm: đoạn chưa đủ byte, không được đánh dấu là xong
                logger.debug(
                    f"Range {seg_start}-{seg_end} thiếu {end - offset} byte, thử lại (lần {attempt + 1})"
                )
                await asyncio.sleep(_jitter(1))

            except (DiskFullError, RangeNotSupported):
                raise
            except Exception as e:
                logger.debug(
                    f"Fetch lỗi (lần {attempt + 1}/{_503_MAX_RETRY}): {type(e).__name__}: {e}"
                )
                await asyncio.sleep(min(_jitter(2**attempt), 16))

            seg_progress[worker_id] = 0

        return False

    # -------------------------------------------------------------------------
    # HÀM MULTI DOWNLOAD
    # -------------------------------------------------------------------------
    async def _multi_conn_download(
        self,
        urls: list,
        headers: dict,
        file_path: Path,
        file_size: int,
        thread_id: int,
        num_conn: int,
        get_fresh_urls_coro,
    ) -> bool:
        if isinstance(urls, str):
            urls = [urls]
        valid_urls = [u for u in urls if u]
        if not valid_urls:
            return False

        ckpt_file = file_path.with_name(file_path.name + ".ckpt")
        completed_ranges = []

        if ckpt_file.exists():
            try:
                with open(ckpt_file, "r", encoding="utf-8") as f:
                    ckpt_data = json.load(f)
                    if ckpt_data.get("total") == file_size:
                        if "completed_ranges" in ckpt_data:
                            completed_ranges = ckpt_data["completed_ranges"]
                        elif "completed" in ckpt_data and "segment_size" in ckpt_data:
                            old_seg_sz = ckpt_data["segment_size"]
                            for si in ckpt_data["completed"]:
                                completed_ranges.append(
                                    [
                                        si * old_seg_sz,
                                        min((si + 1) * old_seg_sz - 1, file_size - 1),
                                    ]
                                )
                    else:
                        ckpt_file.unlink()
            except Exception:
                pass

        if not file_path.exists() or file_path.stat().st_size != file_size:
            completed_ranges = []
            try:
                # Checkpoint rỗng phải có TRƯỚC file đã cấp phát đủ dung lượng, nếu không
                # lần chạy sau sẽ tưởng file tạm toàn byte 0 là đã tải xong
                with open(ckpt_file, "w", encoding="utf-8") as f:
                    json.dump({"total": file_size, "completed_ranges": []}, f)
                with open(file_path, "wb") as f:
                    f.truncate(file_size)
            except OSError as e:
                if e.errno == errno.ENOSPC:
                    console.print(f"\n[bold red]{Language.get('err_disk_full')}[/]")
                    self.progress_data[thread_id]["status"] = "Disk Full"
                    self.cancel_event.set()
                    return False
                self.progress_data[thread_id]["status"] = "Disk Error"
                return False

        pending_gaps = self._get_gaps(file_size, completed_ranges)
        base_done = sum(r[1] - r[0] + 1 for r in self._merge_ranges(completed_ranges))

        if not pending_gaps:
            return True

        seg_progress = {i: 0 for i in range(num_conn)}
        shared = {
            "base_done": base_done,
            "fail_streak": 0,
            "aborted": False,
            "disk_full": False,
        }
        shared_lock = asyncio.Lock()

        url_holders = list(urls)
        url_lock = asyncio.Lock()
        ckpt_lock = asyncio.Lock()

        cond = asyncio.Condition()
        active_downloads = 0
        max_fail_streak = max(num_conn * 2, 10)

        async def save_checkpoint():
            async with ckpt_lock:
                try:
                    ckpt_data = {
                        "total": file_size,
                        "completed_ranges": self._merge_ranges(completed_ranges),
                    }
                    async with aiofiles.open(ckpt_file, "w", encoding="utf-8") as f:
                        await f.write(json.dumps(ckpt_data))
                except Exception:
                    pass

        self.progress_data[thread_id]["status"] = (
            "DL x1 acc" if len(valid_urls) == 1 else f"DL x{len(valid_urls)} acc"
        )

        try:
            with open(file_path, "r+b") as fd:
                with mmap.mmap(fd.fileno(), file_size, access=mmap.ACCESS_WRITE) as mm:

                    async def auto_flush():
                        loop = asyncio.get_running_loop()
                        while not self.cancel_event.is_set():
                            await asyncio.sleep(4)
                            try:
                                await loop.run_in_executor(None, mm.flush)
                            except Exception:
                                pass

                    async def speed_monitor():
                        last_time = time.time()
                        last_done = base_done
                        current_speed = 0.0
                        while not self.cancel_event.is_set():
                            await asyncio.sleep(0.5)
                            now = time.time()
                            done = shared["base_done"] + sum(seg_progress.values())

                            delta_done = max(0, done - last_done)
                            delta_time = now - last_time

                            if delta_time > 0:
                                inst_speed = delta_done / delta_time
                                if current_speed == 0:
                                    current_speed = inst_speed
                                else:
                                    current_speed = (current_speed * 0.7) + (
                                        inst_speed * 0.3
                                    )

                            last_done = done
                            last_time = now

                            percent = (
                                min((done / file_size) * 100, 100) if file_size else 0
                            )
                            eta = (
                                (file_size - done) / current_speed
                                if current_speed > 0
                                else 0
                            )

                            self.progress_data[thread_id].update(
                                {
                                    "done_bytes": done,
                                    "speed": current_speed,
                                    "percent": percent,
                                    "eta": eta,
                                }
                            )
                            if done >= file_size:
                                break

                    async def worker(worker_id):
                        nonlocal active_downloads
                        current_chunk_size = START_CHUNK_SIZE

                        while True:
                            if self.cancel_event.is_set() or shared["aborted"]:
                                break

                            async with cond:
                                while (
                                    not pending_gaps
                                    and active_downloads > 0
                                    and not self.cancel_event.is_set()
                                    and not shared["aborted"]
                                ):
                                    await cond.wait()

                                if (
                                    self.cancel_event.is_set()
                                    or shared["aborted"]
                                    or not pending_gaps
                                ):
                                    break

                                gap_start, gap_end = pending_gaps.pop(0)
                                chunk_end = min(
                                    gap_start + current_chunk_size - 1, gap_end
                                )

                                if chunk_end < gap_end:
                                    pending_gaps.insert(0, [chunk_end + 1, gap_end])

                                ss, se = gap_start, chunk_end
                                active_downloads += 1

                            async def _requeue():
                                nonlocal active_downloads
                                async with cond:
                                    active_downloads -= 1
                                    seg_progress[worker_id] = 0
                                    pending_gaps.append([ss, se])
                                    pending_gaps.sort(key=lambda x: x[0])
                                    cond.notify_all()

                            url_idx = worker_id % len(url_holders)
                            async with url_lock:
                                cur_url = url_holders[url_idx] or next(
                                    (u for u in url_holders if u), None
                                )

                            ctx_acc.set(
                                f"#{url_idx}" if len(url_holders) > 1 else "MAIN"
                            )
                            ctx_range.set(f"{ss}-{se}")

                            if not cur_url:
                                await _requeue()
                                continue

                            start_time = time.time()
                            logger.debug(
                                f"Worker {worker_id} bắt đầu tải Range {ss}-{se} (Size: {self.format_size(se - ss + 1)})"
                            )
                            ok = False
                            try:
                                ok = await self._fetch_segment(
                                    session,
                                    cur_url,
                                    headers,
                                    mm,
                                    worker_id,
                                    ss,
                                    se,
                                    seg_progress,
                                    url_holders,
                                    url_lock,
                                )

                                if not ok and not self.cancel_event.is_set():
                                    logger.warning(
                                        "Tải Range thất bại. Đang refresh URL..."
                                    )
                                    fresh = await get_fresh_urls_coro()
                                    if fresh:
                                        async with url_lock:
                                            for i, u in enumerate(fresh):
                                                if u and i < len(url_holders):
                                                    url_holders[i] = u
                                    ok = await self._fetch_segment(
                                        session,
                                        url_holders[url_idx]
                                        or next((u for u in url_holders if u), cur_url),
                                        headers,
                                        mm,
                                        worker_id,
                                        ss,
                                        se,
                                        seg_progress,
                                        url_holders,
                                        url_lock,
                                    )

                            except DiskFullError:
                                shared["disk_full"] = True
                                await _requeue()
                                break
                            except RangeNotSupported as e:
                                if not shared["aborted"]:
                                    logger.warning(f"Server không hỗ trợ tải theo đoạn ({e})")
                                shared["aborted"] = True
                                await _requeue()
                                break
                            except Exception as e:
                                logger.warning(
                                    f"Worker lỗi, trả Range về hàng đợi: {type(e).__name__}: {e}"
                                )
                                await _requeue()
                                await asyncio.sleep(_jitter(2))
                                continue

                            time_taken = time.time() - start_time
                            chunk_bytes = se - ss + 1

                            if ok:
                                speed_str = self.format_size(
                                    chunk_bytes / max(time_taken, 0.001)
                                )
                                logger.debug(
                                    f"Worker {worker_id} hoàn tất Range {ss}-{se} trong {time_taken:.2f}s | Tốc độ: {speed_str}/s"
                                )

                            async with cond:
                                active_downloads -= 1
                                seg_progress[worker_id] = 0
                                if ok:
                                    completed_ranges.append([ss, se])
                                    shared["fail_streak"] = 0
                                    async with shared_lock:
                                        shared["base_done"] += chunk_bytes
                                else:
                                    pending_gaps.append([ss, se])
                                    pending_gaps.sort(key=lambda x: x[0])
                                    shared["fail_streak"] += 1
                                    if shared["fail_streak"] >= max_fail_streak:
                                        shared["aborted"] = True
                                        logger.error(
                                            f"Hủy tải: {shared['fail_streak']} range thất bại liên tiếp, link có thể đã chết."
                                        )
                                cond.notify_all()

                            if ok:
                                await save_checkpoint()
                                speed = chunk_bytes / max(time_taken, 0.001)
                                if speed > 5 * 1024 * 1024:
                                    current_chunk_size = min(
                                        int(current_chunk_size * 1.5), MAX_CHUNK_SIZE
                                    )
                                elif speed < 1 * 1024 * 1024:
                                    current_chunk_size = max(
                                        current_chunk_size // 2, MIN_CHUNK_SIZE
                                    )
                            else:
                                current_chunk_size = max(
                                    current_chunk_size // 2, MIN_CHUNK_SIZE
                                )

                    flusher = asyncio.create_task(auto_flush())
                    ui_monitor = asyncio.create_task(speed_monitor())
                    session = self._dl_session()
                    try:
                        await asyncio.gather(
                            *[asyncio.create_task(worker(i)) for i in range(num_conn)]
                        )
                    finally:
                        flusher.cancel()
                        ui_monitor.cancel()

                    if shared["disk_full"]:
                        raise DiskFullError("No space left on device")
                    if self.cancel_event.is_set() or shared["aborted"]:
                        return False

                    try:
                        mm.flush()
                    except Exception:
                        pass
        except DiskFullError:
            raise
        except OSError as e:
            if e.errno == errno.ENOSPC:
                console.print(f"\n[bold red]{Language.get('err_disk_full')}[/]")
                self.cancel_event.set()
            logger.error(f"Lỗi File / Mmap: {e}")
            return False

        return file_path.exists() and file_path.stat().st_size == file_size
