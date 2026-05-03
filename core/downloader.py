import threading
import time
import asyncio
import random
import re
import aiohttp
import aiofiles
from pathlib import Path
from aiohttp import ClientTimeout
from rich.console import Group
from rich.table import Table
from rich.panel import Panel
from rich import box
from config.settings import Config, console, Language
from core.api import PikPakAPI, TreeBuilder
from core.account_pool import get_pool
from core.logger import logger

DONE_STATUS   = "Done"
SKIP_STATUS   = "Skipped"
GOOD_STATUSES = {DONE_STATUS, SKIP_STATUS}

SEGMENT_SIZE    = 8 * 1024 * 1024 
CHUNK_SIZE      = 512 * 1024  
UPDATE_INTERVAL = 0.4

_AUTO_CONN = [
    (500 * 1024 * 1024, 16), 
    ( 50 * 1024 * 1024,  8),
    ( 10 * 1024 * 1024,  4),
]

TOKEN_TTL = 20 * 60
_503_BASE_DELAY = 10.0 
_503_MAX_DELAY  = 30.0
_503_MAX_RETRY  = 6

def _jitter(base: float) -> float:
    return base + random.uniform(0, base * 0.5)

def _make_connector() -> aiohttp.TCPConnector:
    return aiohttp.TCPConnector(limit=0, limit_per_host=0, verify_ssl=False)

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
            bad     = p['status'] in ("Error", "Failed", "Restore Fail", "Cancelled", "Cancelling...")
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

    async def _fetch_segment(self, session: aiohttp.ClientSession, url: str, headers: dict, file_path: Path, seg_i: int, seg_start: int, seg_end: int, thread_id: int, seg_progress: dict, shared: dict, shared_lock: asyncio.Lock, dl_start: float, url_holders: list, url_lock: asyncio.Lock) -> bool:
        h = headers.copy()
        h['Range'] = f"bytes={seg_start}-{seg_end}"
        last_ui = time.time()
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
                    if r.status == 503 or r.status == 429:
                        await asyncio.sleep(min(_jitter(_503_BASE_DELAY * (2 ** attempt)), _503_MAX_DELAY))
                        continue
                    if r.status in (401, 403, 500, 502, 504):
                        await asyncio.sleep(_jitter(1.5 ** attempt))
                        continue

                    r.raise_for_status()

                    async with aiofiles.open(file_path, "r+b") as f:
                        await f.seek(seg_start)
                        async for chunk in r.content.iter_chunked(CHUNK_SIZE):
                            if self.cancel_event.is_set(): return False
                            if not chunk: continue
                            await f.write(chunk)
                            attempt_bytes += len(chunk)
                            now = time.time()
                            if now - last_ui >= UPDATE_INTERVAL:
                                elapsed = max(now - dl_start, 0.001)
                                async with shared_lock:
                                    seg_progress[seg_i] = attempt_bytes
                                    done = sum(seg_progress.values())
                                    total = shared['total']
                                    speed = done / elapsed
                                    percent = done / total * 100 if total else 0
                                    eta = (total - done) / speed if speed > 0 else 0
                                    shared.update({'speed': speed, 'percent': percent, 'eta': eta})
                                    self.progress_data[thread_id].update({'done_bytes': done, 'speed': speed, 'percent': percent, 'eta': eta})
                                last_ui = now

                async with shared_lock:
                    seg_progress[seg_i] = attempt_bytes
                    done = sum(seg_progress.values())
                    total = shared['total']
                    elapsed = max(time.time() - dl_start, 0.001)
                    speed = done / elapsed
                    percent = done / total * 100 if total else 0
                    eta = (total - done) / speed if speed > 0 else 0
                    shared.update({'speed': speed, 'percent': percent, 'eta': eta})
                    self.progress_data[thread_id].update({'done_bytes': done, 'speed': speed, 'percent': percent, 'eta': eta})
                return True

            except (aiohttp.ClientProxyConnectionError, aiohttp.ClientHttpProxyError):
                proxy_url = None
                await asyncio.sleep(1)
            except Exception:
                await asyncio.sleep(min(_jitter(2 ** attempt), 16))

            async with shared_lock:
                seg_progress[seg_i] = 0

        return False

    async def _multi_conn_download(self, urls: list, headers: dict, file_path: Path, file_size: int, thread_id: int, num_conn: int, get_fresh_urls_coro) -> bool:
        if isinstance(urls, str): urls = [urls]
        valid_urls = [u for u in urls if u]
        if not valid_urls: return False

        seg_q   = asyncio.Queue()
        pos     = 0
        seg_idx = 0
        while pos < file_size:
            end = min(pos + SEGMENT_SIZE - 1, file_size - 1)
            seg_q.put_nowait((seg_idx, pos, end))
            pos     = end + 1
            seg_idx += 1

        try:
            async with aiofiles.open(file_path, "wb"):
                pass
        except Exception:
            self.progress_data[thread_id]['status'] = "Disk Error"
            return False

        seg_progress = {}
        shared       = {'speed': 0.0, 'percent': 0.0, 'eta': 0.0, 'total': file_size}
        shared_lock  = asyncio.Lock()
        dl_start     = time.time()
        url_holders  = list(urls)
        url_lock     = asyncio.Lock()
        failed_segs  = []
        fail_lock    = asyncio.Lock()

        n_acc  = len(valid_urls)
        label  = f"{n_acc} acc" if n_acc > 1 else f"{num_conn} conn"
        self.progress_data[thread_id]['status'] = f"DL x{label}"

        connector = _make_connector()
        timeout   = ClientTimeout(total=120, connect=15)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            async def worker():
                while True:
                    if self.cancel_event.is_set(): break
                    try: si, ss, se = seg_q.get_nowait()
                    except asyncio.QueueEmpty: break

                    url_idx = si % len(url_holders)
                    async with url_lock:
                        cur_url = url_holders[url_idx] or next((u for u in url_holders if u), None)

                    if not cur_url:
                        async with fail_lock: failed_segs.append((si, ss, se))
                        seg_q.task_done()
                        continue

                    ok = await self._fetch_segment(session, cur_url, headers, file_path, si, ss, se, thread_id, seg_progress, shared, shared_lock, dl_start, url_holders, url_lock)

                    if not ok and not self.cancel_event.is_set():
                        fresh = await get_fresh_urls_coro()
                        if fresh:
                            async with url_lock:
                                for i, u in enumerate(fresh):
                                    if u and i < len(url_holders): url_holders[i] = u
                        ok = await self._fetch_segment(session, url_holders[url_idx] or url_holders[0], headers, file_path, si, ss, se, thread_id, seg_progress, shared, shared_lock, dl_start, url_holders, url_lock)

                    if not ok:
                        async with fail_lock: failed_segs.append((si, ss, se))
                    seg_q.task_done()

            await asyncio.gather(*[asyncio.create_task(worker()) for _ in range(num_conn)])

        if self.cancel_event.is_set(): return False

        if failed_segs:
            self.progress_data[thread_id]['status'] = f"Retry {len(failed_segs)} segs"
            fresh = await get_fresh_urls_coro()
            retry_url = next((u for u in (fresh or url_holders) if u), None)
            if not retry_url: return False
            conn2   = _make_connector()
            async with aiohttp.ClientSession(connector=conn2, timeout=timeout) as sess2:
                for si, ss, se in failed_segs:
                    if self.cancel_event.is_set(): return False
                    ok = await self._fetch_segment(sess2, retry_url, headers, file_path, si, ss, se, thread_id, seg_progress, shared, shared_lock, dl_start, [retry_url], asyncio.Lock())
                    if not ok: return False

        return file_path.exists() and file_path.stat().st_size == file_size

    async def download_single_file(self, file_data, share_id, pass_token, thread_id, api=None):
        if self.cancel_event.is_set():
            if self.progress_data.get(thread_id, {}).get('status') != 'Waiting':
                self.progress_data[thread_id].update({'status': 'Cancelled'})
            return False

        name            = file_data['name']
        real_total_size = int(file_data['size'])
        HEAVY_EXTS      = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.ts', '.iso', '.m4v', '.rar', '.zip', '.7z'}
        use_premium = any(name.lower().endswith(e) for e in HEAVY_EXTS) or Config.FORCE_PREMIUM_MODE

        self.progress_data[thread_id] = {'id': thread_id, 'name': name, 'percent': 0, 'speed': 0, 'status': "Init...", 'done_bytes': 0, 'total_bytes': real_total_size, 'eta': 0}

        pool = get_pool()
        if api is None: api = pool.acquire() or self.api

        save_dir  = Config.get_download_dir() / Path(file_data['path']).parent
        save_dir.mkdir(parents=True, exist_ok=True)
        file_path = save_dir / name
        temp_file = file_path.parent / f".{file_path.name}.tmp"
        temp_file.unlink(missing_ok=True)

        BASE_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36", "Referer": "https://mypikpak.com/", "Accept": "*/*", "Accept-Encoding": "identity", "Connection": "keep-alive"}

        def _clean_local():
            for p in (file_path, temp_file):
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

                async def _get_fresh_urls():
                    fresh = await pool.get_stripe_urls_async(lambda a: _get_url_for(a, my_file_id))
                    return [u for u in (fresh or []) if u] or [download_url]

                dl_success = await self._multi_conn_download(stripe_urls, BASE_HEADERS, temp_file, real_total_size, thread_id, num_conn, _get_fresh_urls)

                if self.cancel_event.is_set():
                    was_cancelled = True; _cancel_cleanup(); return False

                if dl_success:
                    if temp_file.exists(): temp_file.rename(file_path)
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

                ok = await self._multi_conn_download([download_url], BASE_HEADERS, temp_file, real_total_size, thread_id, num_conn, _fresh_direct)

                if self.cancel_event.is_set():
                    _cancel_cleanup(); return False
                if ok:
                    if temp_file.exists(): temp_file.rename(file_path)
                    _mark_done(); return True
                _clean_local()
                _set_status("Fallback...")

            try:
                h          = BASE_HEADERS.copy()
                resume_pos = 0
                mode       = 'wb'

                if temp_file.exists():
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
                async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
                    async with session.get(download_url, headers=h, proxy=proxy_url) as r:
                        if resume_pos > 0 and r.status == 200:
                            resume_pos = 0; mode = 'wb'; temp_file.unlink(missing_ok=True)
                        if r.status not in (200, 206):
                            _clean_local(); _set_status(f"Err {r.status}"); return False

                        done   = resume_pos
                        start  = time.time()
                        last_t = start
                        last_d = done

                        async with aiofiles.open(temp_file, mode) as f:
                            async for chunk in r.content.iter_chunked(CHUNK_SIZE):
                                if self.cancel_event.is_set():
                                    _cancel_cleanup(); return False
                                if chunk:
                                    await f.write(chunk); done += len(chunk)
                                    now = time.time()
                                    if now - last_t >= 0.5:
                                        speed   = (done - last_d) / (now - last_t)
                                        percent = done / real_total_size * 100 if real_total_size else 0
                                        eta     = (real_total_size - done) / speed if speed > 0 else 0
                                        self.progress_data[thread_id].update({'percent': percent, 'speed': speed, 'status': "DL...", 'done_bytes': done, 'eta': eta})
                                        last_t = now; last_d = done

                if temp_file.exists() and temp_file.stat().st_size >= real_total_size:
                    temp_file.rename(file_path)
                    _mark_done(); return True
                return False

            except Exception:
                _clean_local(); _set_status("Error"); return False