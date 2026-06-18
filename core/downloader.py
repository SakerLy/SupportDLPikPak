import threading
import time
import asyncio
import random
import re
import socket
import aiohttp
import aiofiles
import json
import errno
import mmap
from pathlib import Path
from aiohttp import ClientTimeout
from rich.console import Group
from rich.table import Table
from rich.panel import Panel
from rich import box
from config.settings import Config, console, Language
from core.api import PikPakAPI, TreeBuilder
from core.account_pool import get_pool
from core.logger import logger, ctx_file, ctx_acc, ctx_range

class DiskFullError(Exception):
    pass

DONE_STATUS   = "Done"
SKIP_STATUS   = "Skipped"
GOOD_STATUSES = {DONE_STATUS, SKIP_STATUS}

# TINH CHỈNH ĐỂ CHỐNG RỚT TỐC ĐỘ VỀ CUỐI
MIN_CHUNK_SIZE   = 1 * 1024 * 1024     # 1MB
MAX_CHUNK_SIZE   = 16 * 1024 * 1024    # Capped 16MB để đảm bảo luồng tải được chia đều đến giây cuối cùng
START_CHUNK_SIZE = 4 * 1024 * 1024     # 4MB

_AUTO_CONN = [
    (2 * 1024 * 1024 * 1024, 32), # > 2GB: 32 Connections
    (1024 * 1024 * 1024, 24),     # > 1GB: 24 Connections
    ( 500 * 1024 * 1024, 16),     # > 500MB: 16 Connections
    (  50 * 1024 * 1024,  8),     # > 50MB: 8 Connections
]

TOKEN_TTL = 20 * 60
_503_BASE_DELAY = 10.0
_503_MAX_DELAY  = 30.0
_503_MAX_RETRY  = 6

def _jitter(base: float) -> float:
    return base + random.uniform(0, base * 0.5)

def _make_connector() -> aiohttp.TCPConnector:
    try:
        import aiodns
        resolver = aiohttp.AsyncResolver()
    except ImportError:
        resolver = aiohttp.ThreadedResolver()

    return aiohttp.TCPConnector(
        resolver=resolver,
        limit=0,
        limit_per_host=0,
        verify_ssl=False,
        keepalive_timeout=60,
        ttl_dns_cache=300,
        family=socket.AF_INET
    )

class Downloader:
    def __init__(self):
        self.api               = PikPakAPI()
        self.tree_builder      = TreeBuilder(self.api)
        self.progress_data     = {}
        self.monitor_active    = False
        self.total_files_count = 0
        self.total_batch_size  = 0
        self.cancel_event      = threading.Event()
        self._last_refresh     = 0.0
        self._token_lock       = asyncio.Lock()
        self.bg_tasks          = set()

    def reset_progress(self):
        self.progress_data     = {}
        self.monitor_active    = False
        self.total_files_count = 0
        self.total_batch_size  = 0
        self.cancel_event      = threading.Event()

    async def _ensure_token(self, api) -> bool:
        async with self._token_lock:
            now = time.time()
            if now - self._last_refresh < TOKEN_TTL: return True
            ok = await api.refresh_token()
            if ok: self._last_refresh = time.time()
            return ok

    async def _bg_delete(self, api_client, file_id):
        for _ in range(3):
            try:
                await asyncio.wait_for(api_client.delete_file(file_id), timeout=10.0)
                return
            except Exception:
                await asyncio.sleep(2)

    @staticmethod
    def format_size(size):
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size < 1024: return f"{size:.2f} {unit}"
            size /= 1024
        return f"{size:.2f} PB"

    @staticmethod
    def format_time(seconds):
        if seconds < 0 or seconds > 86400 * 3: return "--:--"
        m, s = divmod(int(seconds), 60)
        h, m = divmod(m, 60)
        return f"{h:02d}:{m:02d}:{s:02d}" if h > 0 else f"{m:02d}:{s:02d}"

    @staticmethod
    def _merge_ranges(ranges):
        if not ranges: return []
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
        merged = Downloader._merge_ranges(completed_ranges)
        gaps = []
        current = 0
        for r in merged:
            if r[0] > current:
                gaps.append([current, r[0] - 1])
            current = r[1] + 1
        if current < total_size:
            gaps.append([current, total_size - 1])
        return gaps

    def _natural_key(self, item):
        return [int(s) if s.isdigit() else s.lower() for s in re.split(r'(\d+)', item['name'])]

    def _recursive_sort(self, node):
        if 'files'   in node: node['files'].sort(key=self._natural_key)
        if 'folders' in node:
            node['folders'].sort(key=self._natural_key)
            for f in node['folders']: self._recursive_sort(f)

    def _resolve_conn(self, file_size: int) -> int:
        cfg = Config.CONCURRENT_THREADS
        for threshold, auto in _AUTO_CONN:
            if file_size >= threshold: return max(cfg, auto)
        return max(cfg, 2)

    async def get_tree_and_prepare(self, url, password):
        m = re.search(r"/s/([A-Za-z0-9_-]+)", url)
        if not m: return None
        share_id = m.group(1)
        if not await self.api.refresh_token(): return None
        files, ptoken = await self.api.get_share_info(share_id, password)
        if not files: return None
        tree = await self.tree_builder.build_tree(files, "", share_id, ptoken)
        self._recursive_sort(tree)
        return {"folders": tree["folders"], "files": tree["files"], "share_id": share_id, "pass_token": ptoken}

    def start_monitor(self, total_count, total_size_bytes):
        self.monitor_active    = True
        self.total_files_count = total_count
        self.total_batch_size  = total_size_bytes

    def stop_monitor(self):
        self.monitor_active = False

    def generate_dashboard_table(self):
        all_threads     = list(self.progress_data.values())
        done_count      = sum(1 for p in all_threads if p['status'] == DONE_STATUS)
        skipped_count   = sum(1 for p in all_threads if p['status'] == SKIP_STATUS)
        cancelled_count = sum(1 for p in all_threads if p['status'] == "Cancelled")

        display_list    = [p for p in all_threads if p['status'] not in (*GOOD_STATUSES, "Cancelled", "Waiting")]

        total_speed = sum(p.get('speed', 0) for p in display_list if "DL" in p['status'] or "Resuming" in p['status'])
        total_downloaded = sum(p.get('done_bytes', 0) for p in all_threads)
        remaining        = max(0, self.total_batch_size - total_downloaded)
        eta_str          = self.format_time(remaining / total_speed) if total_speed > 0 else "--:--"

        cancel_hint = ("[bold red] ⛔ CANCELLING...[/]" if self.cancel_event.is_set() else "  [dim]Press [bold]Q[/bold] to cancel[/]")

        stats_grid = Table.grid(expand=True)
        stats_grid.add_column(justify="center", ratio=1)
        stats_grid.add_column(justify="center", ratio=1)
        stats_grid.add_column(justify="center", ratio=1)
        stats_grid.add_row(
            f"[bold cyan]Queue: {self.total_files_count - done_count - skipped_count - cancelled_count}[/]",
            f"[bold green]Done: {done_count}[/] | [bold yellow]Skip: {skipped_count}[/]" + (f" | [bold red]Cancel: {cancelled_count}[/]" if cancelled_count else ""),
            f"[bold white]Speed: {self.format_size(total_speed)}/s | ETA: {eta_str}[/]"
        )
        panel_stats = Panel(Group(stats_grid, cancel_hint), style="blue", title=f"[bold]{Language.get('global_stats')}[/]")

        task_table = Table(box=box.SIMPLE, show_header=True, header_style="bold cyan", expand=True)
        task_table.add_column("ID",       width=4)
        task_table.add_column("Filename", ratio=3)
        task_table.add_column("Progress", ratio=2)
        task_table.add_column("Speed",    width=12, justify="right")
        task_table.add_column("ETA",      width=10, justify="right")
        task_table.add_column("Status",   width=14, justify="center")

        for p in sorted(display_list, key=lambda x: x['id']):
            pct     = min(p.get('percent', 0), 100)
            bad     = p['status'] in ("Error", "Failed", "Restore Fail", "Cancelled", "Cancelling...", "Disk Error", "Disk Full")
            color   = "red" if bad else ("green" if pct == 100 else "cyan")
            filled  = int(20 * pct / 100)
            bar     = f"[{color}]{'━'*filled}[/][dim white]{'━'*(20-filled)}[/]"
            ss      = "bold red" if bad else ("bold yellow" if p['status'] == "Cancelling..." else "cyan")
            task_table.add_row(
                str(p['id']), p['name'], f"{bar} {pct:.0f}%",
                f"{self.format_size(p.get('speed', 0))}/s",
                self.format_time(p.get('eta', 0)),
                f"[{ss}]{p['status']}[/]"
            )
        return Group(panel_stats, task_table)

    # -------------------------------------------------------------------------
    # HÀM TẢI 1 CHUNK - KHÔNG UI CẬP NHẬT Ở ĐÂY ĐỂ TRÁNH NHẢY LOẠN
    # -------------------------------------------------------------------------
    async def _fetch_segment(self, session: aiohttp.ClientSession, url: str, headers: dict, mm: mmap.mmap, worker_id: int, seg_start: int, seg_end: int, seg_progress: dict, url_holders: list, url_lock: asyncio.Lock) -> bool:
        h = headers.copy()
        h['Range'] = f"bytes={seg_start}-{seg_end}"
        proxy = Config.get_proxy_dict()
        proxy_url = proxy.get('http') if proxy else None

        for attempt in range(_503_MAX_RETRY):
            if self.cancel_event.is_set(): return False
            if attempt > 0:
                async with url_lock:
                    n = len(url_holders)
                    url = url_holders[attempt % n] if n > 1 else url_holders[0]

            attempt_bytes = 0
            try:
                async with session.get(url, headers=h, proxy=proxy_url) as r:
                    if r.status in (503, 429):
                        await asyncio.sleep(min(_jitter(_503_BASE_DELAY * (2 ** attempt)), _503_MAX_DELAY))
                        continue
                    if r.status in (401, 403, 500, 502, 504):
                        await asyncio.sleep(_jitter(1.5 ** attempt))
                        continue

                    r.raise_for_status()

                    offset = seg_start
                    async for chunk in r.content.iter_any():
                        if self.cancel_event.is_set(): return False
                        if not chunk: continue

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

                return True

            except DiskFullError:
                raise
            except (aiohttp.ClientProxyConnectionError, aiohttp.ClientHttpProxyError):
                proxy_url = None
                await asyncio.sleep(1)
            except Exception as e:
                logger.debug(f"Fetch error: {e}")
                await asyncio.sleep(min(_jitter(2 ** attempt), 16))

            seg_progress[worker_id] = 0

        return False

    # -------------------------------------------------------------------------
    # HÀM MULTI DOWNLOAD
    # -------------------------------------------------------------------------
    async def _multi_conn_download(self, urls: list, headers: dict, file_path: Path, file_size: int, thread_id: int, num_conn: int, get_fresh_urls_coro) -> bool:
        if isinstance(urls, str): urls = [urls]
        valid_urls = [u for u in urls if u]
        if not valid_urls: return False

        ckpt_file = file_path.with_name(file_path.name + ".ckpt")
        completed_ranges = []

        if ckpt_file.exists():
            try:
                with open(ckpt_file, 'r', encoding='utf-8') as f:
                    ckpt_data = json.load(f)
                    if ckpt_data.get('total') == file_size:
                        if 'completed_ranges' in ckpt_data:
                            completed_ranges = ckpt_data['completed_ranges']
                        elif 'completed' in ckpt_data and 'segment_size' in ckpt_data:
                            old_seg_sz = ckpt_data['segment_size']
                            for si in ckpt_data['completed']:
                                completed_ranges.append([si * old_seg_sz, min((si + 1) * old_seg_sz - 1, file_size - 1)])
                    else:
                        ckpt_file.unlink()
            except Exception:
                pass

        if not file_path.exists() or file_path.stat().st_size != file_size:
            try:
                with open(file_path, "wb") as f:
                    f.truncate(file_size)
            except OSError as e:
                if e.errno == errno.ENOSPC:
                    console.print(f"\n[bold red]✖ LỖI HỆ THỐNG: Ổ cứng đầy! Không thể tạo file mmap.[/]")
                    self.progress_data[thread_id]['status'] = "Disk Full"
                    self.cancel_event.set()
                    return False
                self.progress_data[thread_id]['status'] = "Disk Error"
                return False

        pending_gaps = self._get_gaps(file_size, completed_ranges)
        base_done = sum(r[1] - r[0] + 1 for r in self._merge_ranges(completed_ranges))

        if not pending_gaps:
            return True

        # BIẾN DÙNG ĐỂ THEO DÕI UI ĐỘC LẬP (KHÔNG ĐỤNG RACE CONDITION)
        seg_progress = {i: 0 for i in range(num_conn)}
        shared       = {'base_done': base_done}
        shared_lock  = asyncio.Lock()

        url_holders  = list(urls)
        url_lock     = asyncio.Lock()
        ckpt_lock    = asyncio.Lock()

        cond = asyncio.Condition()
        active_downloads = 0

        async def save_checkpoint():
            async with ckpt_lock:
                try:
                    ckpt_data = {
                        'total': file_size,
                        'completed_ranges': self._merge_ranges(completed_ranges)
                    }
                    async with aiofiles.open(ckpt_file, 'w', encoding='utf-8') as f:
                        await f.write(json.dumps(ckpt_data))
                except Exception:
                    pass

        n_acc  = len(valid_urls)
        label  = f"{n_acc} acc" if n_acc > 1 else f"{num_conn} conn"
        self.progress_data[thread_id]['status'] = f"DL x{label}"

        connector = _make_connector()
        timeout   = ClientTimeout(total=120, connect=15)

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

                    # TASK ĐO TỐC ĐỘ ĐỘC LẬP (MƯỢT VÀ CHÍNH XÁC NHƯ IDM)
                    async def speed_monitor():
                        last_time = time.time()
                        last_done = base_done
                        current_speed = 0.0
                        while not self.cancel_event.is_set():
                            await asyncio.sleep(0.5)
                            now = time.time()
                            done = shared['base_done'] + sum(seg_progress.values())

                            # CHỐT CHẶN: Dùng max(0, ...) để tránh số âm khi các chunk bị fail và reset tiến trình
                            delta_done = max(0, done - last_done)
                            delta_time = now - last_time

                            if delta_time > 0:
                                inst_speed = delta_done / delta_time
                                # Hàm Exponential Moving Average làm mịn tốc độ (không nhảy loạn)
                                if current_speed == 0: current_speed = inst_speed
                                else: current_speed = (current_speed * 0.7) + (inst_speed * 0.3)

                            last_done = done
                            last_time = now

                            percent = min((done / file_size) * 100, 100) if file_size else 0
                            eta = (file_size - done) / current_speed if current_speed > 0 else 0

                            self.progress_data[thread_id].update({
                                'done_bytes': done,
                                'speed': current_speed,
                                'percent': percent,
                                'eta': eta
                            })
                            if done >= file_size: break

                    flusher = asyncio.create_task(auto_flush())
                    ui_monitor = asyncio.create_task(speed_monitor())

                    async with aiohttp.ClientSession(
                        connector=connector,
                        timeout=timeout,
                        read_bufsize=1024 * 1024
                    ) as session:

                        async def worker(worker_id):
                            nonlocal active_downloads
                            current_chunk_size = START_CHUNK_SIZE

                            while True:
                                if self.cancel_event.is_set(): break

                                async with cond:
                                    while not pending_gaps and active_downloads > 0 and not self.cancel_event.is_set():
                                        await cond.wait()

                                    if self.cancel_event.is_set() or not pending_gaps:
                                        break

                                    gap_start, gap_end = pending_gaps.pop(0)
                                    chunk_end = min(gap_start + current_chunk_size - 1, gap_end)

                                    if chunk_end < gap_end:
                                        pending_gaps.insert(0, [chunk_end + 1, gap_end])

                                    ss, se = gap_start, chunk_end
                                    active_downloads += 1

                                url_idx = worker_id % len(url_holders)
                                async with url_lock:
                                    cur_url = url_holders[url_idx] or next((u for u in url_holders if u), None)

                                ctx_acc.set(f"#{url_idx}" if len(url_holders) > 1 else "MAIN")
                                ctx_range.set(f"{ss}-{se}")

                                if not cur_url:
                                    async with cond:
                                        pending_gaps.append([ss, se])
                                        pending_gaps.sort(key=lambda x: x[0])
                                        active_downloads -= 1
                                        cond.notify_all()
                                    continue

                                start_time = time.time()
                                logger.debug(f"Worker {worker_id} bắt đầu tải Range {ss}-{se} (Size: {self.format_size(se - ss + 1)})")
                                ok = False
                                try:
                                    ok = await self._fetch_segment(session, cur_url, headers, mm, worker_id, ss, se, seg_progress, url_holders, url_lock)

                                    if not ok and not self.cancel_event.is_set():
                                        logger.warning("Tải Range thất bại. Đang refresh URL...")
                                        fresh = await get_fresh_urls_coro()
                                        if fresh:
                                            async with url_lock:
                                                for i, u in enumerate(fresh):
                                                    if u and i < len(url_holders): url_holders[i] = u
                                        ok = await self._fetch_segment(session, url_holders[url_idx] or url_holders[0], headers, mm, worker_id, ss, se, seg_progress, url_holders, url_lock)

                                except Exception as e:
                                    logger.error(f"Worker Exception, trả Range về Queue: {e}")
                                    async with cond:
                                        active_downloads -= 1
                                        pending_gaps.append([ss, se])
                                        pending_gaps.sort(key=lambda x: x[0])
                                        seg_progress[worker_id] = 0
                                        cond.notify_all()
                                    raise

                                time_taken = time.time() - start_time
                                chunk_bytes = se - ss + 1

                                if ok:
                                    speed_str = self.format_size(chunk_bytes / max(time_taken, 0.001))
                                    logger.debug(f"Worker {worker_id} hoàn tất Range {ss}-{se} trong {time_taken:.2f}s | Tốc độ: {speed_str}/s")

                                async with cond:
                                    active_downloads -= 1
                                    seg_progress[worker_id] = 0
                                    if ok:
                                        completed_ranges.append([ss, se])
                                        async with shared_lock:
                                            shared['base_done'] += chunk_bytes
                                    else:
                                        pending_gaps.append([ss, se])
                                        pending_gaps.sort(key=lambda x: x[0])
                                    cond.notify_all()

                                if ok:
                                    await save_checkpoint()
                                    speed = chunk_bytes / max(time_taken, 0.001)
                                    if speed > 5 * 1024 * 1024: # > 5MB/s tăng nhẹ Chunk thay vì x2 quá lố
                                        current_chunk_size = min(int(current_chunk_size * 1.5), MAX_CHUNK_SIZE)
                                    elif speed < 1 * 1024 * 1024:
                                        current_chunk_size = max(current_chunk_size // 2, MIN_CHUNK_SIZE)
                                else:
                                    current_chunk_size = max(current_chunk_size // 2, MIN_CHUNK_SIZE)

                        await asyncio.gather(*[asyncio.create_task(worker(i)) for i in range(num_conn)])

                    if self.cancel_event.is_set():
                        flusher.cancel()
                        ui_monitor.cancel()
                        return False

                    flusher.cancel()
                    ui_monitor.cancel()
                    try: mm.flush()
                    except: pass
        except OSError as e:
            if e.errno == errno.ENOSPC:
                console.print(f"\n[bold red]✖ LỖI HỆ THỐNG: Ổ cứng đầy! Dừng toàn bộ.[/]")
                self.cancel_event.set()
            logger.error(f"Lỗi File / Mmap: {e}")
            return False

        return file_path.exists() and file_path.stat().st_size == file_size

    # -------------------------------------------------------------------------
    # MAIN DOWNLOADER LOGIC CHO SINGLE FILE
    # -------------------------------------------------------------------------
    async def download_single_file(self, file_data, share_id, pass_token, thread_id, api=None):
        if self.cancel_event.is_set():
            if self.progress_data.get(thread_id, {}).get('status') != 'Waiting':
                self.progress_data[thread_id].update({'status': 'Cancelled'})
            return False

        name            = file_data['name']
        real_total_size = int(file_data['size'])
        HEAVY_EXTS      = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.ts', '.iso', '.m4v', '.rar', '.zip', '.7z'}
        use_premium = any(name.lower().endswith(e) for e in HEAVY_EXTS) or Config.FORCE_PREMIUM_MODE

        logger.info(f"Bắt đầu tiến trình tải file: '{name}' | Dung lượng: {self.format_size(real_total_size)} | Tải Premium: {use_premium}")

        self.progress_data[thread_id] = {'id': thread_id, 'name': name, 'percent': 0, 'speed': 0, 'status': "Init...", 'done_bytes': 0, 'total_bytes': real_total_size, 'eta': 0}

        pool = get_pool()
        if api is None: api = pool.acquire() or self.api

        save_dir  = Config.get_download_dir() / Path(file_data['path']).parent
        save_dir.mkdir(parents=True, exist_ok=True)
        file_path = save_dir / name
        temp_file = file_path.parent / f".{file_path.name}.tmp"
        ckpt_file = temp_file.with_name(temp_file.name + ".ckpt")

        BASE_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36", "Referer": "https://mypikpak.com/", "Accept": "*/*", "Accept-Encoding": "identity", "Connection": "keep-alive"}

        def _clean_local():
            for p in (file_path, temp_file, ckpt_file):
                try:
                    if p.exists(): p.unlink()
                except: pass

        def _set_status(s): self.progress_data[thread_id]['status'] = s
        def _cancel_cleanup(): _clean_local(); _set_status('Cancelled')
        def _mark_done(): self.progress_data[thread_id].update({'percent': 100, 'speed': 0, 'status': DONE_STATUS, 'done_bytes': real_total_size, 'eta': 0})

        if use_premium:
            if file_path.exists():
                if file_path.stat().st_size == real_total_size:
                    self.progress_data[thread_id].update({'percent': 100, 'speed': 0, 'status': SKIP_STATUS, 'done_bytes': real_total_size, 'eta': 0})
                    return True
                file_path.unlink(missing_ok=True)

            my_file_id    = None
            dl_success    = False
            was_cancelled = False

            _set_status("Refreshing token...")
            if not await self._ensure_token(api):
                _set_status("Auth Fail"); return False

            if pool.size() > 1: _set_status(f"Init [{pool.size()} acc]...")

            try:
                if self.cancel_event.is_set():
                    was_cancelled = True; _cancel_cleanup(); return False

                _set_status("Checking cloud...")
                stale = await api.wait_for_file(name, max_retries=1)
                if stale:
                    task = asyncio.create_task(self._bg_delete(api, stale))
                    self.bg_tasks.add(task)
                    task.add_done_callback(self.bg_tasks.discard)

                if self.cancel_event.is_set():
                    was_cancelled = True; _cancel_cleanup(); return False

                _set_status(Language.get('status_restore'))
                my_file_id, error = await api.restore_and_poll(share_id, file_data['id'], pass_token)

                if not my_file_id and error == "file_space_not_enough":
                    alt = pool.acquire()
                    if alt:
                        my_file_id, error = await alt.restore_and_poll(share_id, file_data['id'], pass_token)
                        if my_file_id: api = alt

                if not my_file_id:
                    _set_status(Language.get('status_check'))
                    my_file_id = await api.wait_for_file(name, max_retries=15)

                if not my_file_id:
                    _set_status("Restore Fail"); return False

                if self.cancel_event.is_set():
                    was_cancelled = True; _cancel_cleanup(); return False

                _set_status(Language.get('status_getlink'))
                download_url = None
                for _ in range(5):
                    download_url = await api.get_user_file_url(my_file_id)
                    if download_url: break
                    await asyncio.sleep(1.5)
                if not download_url:
                    _set_status("No Link"); return False

                if self.cancel_event.is_set():
                    was_cancelled = True; _cancel_cleanup(); return False

                num_conn = self._resolve_conn(real_total_size)

                async def _get_url_for(a, fid):
                    for _ in range(3):
                        u = await a.get_user_file_url(fid)
                        if u: return u
                        await asyncio.sleep(1)
                    return None

                stripe_urls = await pool.get_stripe_urls_async(lambda a: _get_url_for(a, my_file_id))
                stripe_urls = [u for u in (stripe_urls or []) if u] or [download_url]

                n_acc = len(stripe_urls)
                _set_status(f"DL x{n_acc}acc/{num_conn}conn")
                logger.debug(f"[{name}] Cấu hình Multi-Connection: Dùng {n_acc} tài khoản với {num_conn} luồng đồng thời.")

                async def _get_fresh_urls():
                    fresh = await pool.get_stripe_urls_async(lambda a: _get_url_for(a, my_file_id))
                    return [u for u in (fresh or []) if u] or [download_url]

                try:
                    dl_success = await self._multi_conn_download(stripe_urls, BASE_HEADERS, temp_file, real_total_size, thread_id, num_conn, _get_fresh_urls)
                except DiskFullError:
                    console.print(f"\n[bold red]✖ LỖI HỆ THỐNG: Ổ cứng đầy! Dừng tải.[/]")
                    _set_status("Disk Full")
                    self.cancel_event.set()
                    was_cancelled = True
                    dl_success = False

                if self.cancel_event.is_set():
                    was_cancelled = True; _cancel_cleanup(); return False

                if dl_success:
                    if temp_file.exists(): temp_file.rename(file_path)
                    if ckpt_file.exists(): ckpt_file.unlink(missing_ok=True)
                    _mark_done()
                else:
                    _set_status("Failed"); _clean_local()

            finally:
                if my_file_id:
                    task = asyncio.create_task(self._bg_delete(api, my_file_id))
                    self.bg_tasks.add(task)
                    task.add_done_callback(self.bg_tasks.discard)
                    if was_cancelled: _set_status("Cancelled")

            return dl_success

        else:
            # FALLBACK DOWNLOAD (Khong dung acc premium)
            if self.cancel_event.is_set():
                _cancel_cleanup(); return False

            download_url = await api.get_download_url(share_id, file_data['id'], pass_token)
            if not download_url:
                _set_status("No URL"); return False

            if file_path.exists() and file_path.stat().st_size == real_total_size:
                self.progress_data[thread_id].update({'percent': 100, 'status': SKIP_STATUS})
                return True

            supports_range = False
            try:
                proxy = Config.get_proxy_dict()
                proxy_url = proxy.get('http') if proxy else None
                conn = _make_connector()
                async with aiohttp.ClientSession(connector=conn, timeout=ClientTimeout(total=10)) as s:
                    async with s.head(download_url, headers=BASE_HEADERS, proxy=proxy_url) as probe:
                        supports_range = (probe.status == 200 and 'bytes' in probe.headers.get('Accept-Ranges', ''))
            except Exception: pass

            if supports_range and real_total_size > 1 * 1024 * 1024:
                num_conn = self._resolve_conn(real_total_size)
                _set_status(f"DL x{num_conn}conn")

                async def _fresh_direct():
                    u = await self.api.get_download_url(share_id, file_data['id'], pass_token)
                    return [u] if u else [download_url]

                try:
                    ok = await self._multi_conn_download([download_url], BASE_HEADERS, temp_file, real_total_size, thread_id, num_conn, _fresh_direct)
                except DiskFullError:
                    console.print(f"\n[bold red]✖ LỖI HỆ THỐNG: Ổ cứng đầy! Dừng tải.[/]")
                    _set_status("Disk Full")
                    self.cancel_event.set()
                    ok = False

                if self.cancel_event.is_set():
                    _cancel_cleanup(); return False
                if ok:
                    if temp_file.exists(): temp_file.rename(file_path)
                    if ckpt_file.exists(): ckpt_file.unlink(missing_ok=True)
                    _mark_done(); return True
                _clean_local()
                _set_status("Fallback...")

            try:
                h          = BASE_HEADERS.copy()
                resume_pos = 0
                mode       = 'wb'

                if temp_file.exists():
                    if ckpt_file.exists():
                        _clean_local()
                        _set_status("Fallback...")
                        resume_pos = 0
                    else:
                        resume_pos = temp_file.stat().st_size
                        if resume_pos < real_total_size:
                            mode = 'ab'
                            h['Range'] = f"bytes={resume_pos}-"
                            _set_status("Resuming...")
                        elif resume_pos >= real_total_size:
                            temp_file.rename(file_path)
                            _mark_done(); return True

                proxy = Config.get_proxy_dict()
                proxy_url = proxy.get('http') if proxy else None
                conn    = _make_connector()
                timeout = ClientTimeout(total=Config.TIMEOUT)

                async with aiohttp.ClientSession(
                    connector=conn,
                    timeout=timeout,
                    read_bufsize=1024 * 1024
                ) as session:
                    async with session.get(download_url, headers=h, proxy=proxy_url) as r:
                        if resume_pos > 0 and r.status == 200:
                            resume_pos = 0; mode = 'wb'; temp_file.unlink(missing_ok=True)
                        if r.status not in (200, 206):
                            _clean_local(); _set_status(f"Err {r.status}"); return False

                        done   = resume_pos
                        start  = time.time()
                        last_t = start
                        last_d = done
                        current_speed = 0.0

                        async with aiofiles.open(temp_file, mode) as f:
                            async for chunk in r.content.iter_any():
                                if self.cancel_event.is_set():
                                    _cancel_cleanup(); return False
                                if chunk:
                                    try:
                                        await f.write(chunk)
                                    except OSError as e:
                                        if e.errno == errno.ENOSPC:
                                            console.print(f"\n[bold red]✖ LỖI HỆ THỐNG: Ổ cứng đầy![/]")
                                            _set_status("Disk Full")
                                            self.cancel_event.set()
                                            return False
                                        raise
                                    done += len(chunk)
                                    now = time.time()
                                    if now - last_t >= 0.5:
                                        # CHỐT CHẶN BỔ SUNG CHO FALLBACK DOWNLOAD
                                        inst_speed   = max(0, done - last_d) / max(now - last_t, 0.001)
                                        if current_speed == 0: current_speed = inst_speed
                                        else: current_speed = (current_speed * 0.7) + (inst_speed * 0.3)

                                        percent = min((done / real_total_size) * 100, 100) if real_total_size else 0
                                        eta     = (real_total_size - done) / current_speed if current_speed > 0 else 0
                                        self.progress_data[thread_id].update({'percent': percent, 'speed': current_speed, 'status': "DL...", 'done_bytes': done, 'eta': eta})
                                        last_t = now; last_d = done

                if temp_file.exists() and temp_file.stat().st_size >= real_total_size:
                    temp_file.rename(file_path)
                    _mark_done(); return True
                return False

            except Exception:
                _clean_local(); _set_status("Error"); return False