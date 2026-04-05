import threading
import time
import os
import subprocess
import aiohttp
import aiofiles
import asyncio
import re
import queue as _queue
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


# ── Shared status constants ────────────────────────────────────────────────────
DONE_STATUS   = "Done"
SKIP_STATUS   = "Skipped"
GOOD_STATUSES = {DONE_STATUS, SKIP_STATUS}


# ── Connection pool factory ───────────────────────────────────────────────────
def _make_session(pool_size: int) -> aiohttp.ClientSession:
    """Session với connection pool + retry. Proxy chỉ dùng khi tải file."""
    connector = aiohttp.TCPConnector(limit=pool_size, limit_per_host=pool_size, verify_ssl=False)
    proxy = Config.get_proxy_dict()
    proxy_url = proxy.get('http') if proxy else None
    timeout = ClientTimeout(total=60)
    return aiohttp.ClientSession(connector=connector, timeout=timeout)


class Downloader:
    # ── Tuning ────────────────────────────────────────────────────────────────
    SEGMENT_SIZE    = 4 * 1024 * 1024   # 4 MB per segment
    CHUNK_SIZE      = 1 * 1024 * 1024   # 1 MB read buffer
    UPDATE_INTERVAL = 0.3               # seconds between progress updates

    _AUTO_CONN = [
        (500 * 1024 * 1024, 32),
        ( 50 * 1024 * 1024, 16),
        ( 10 * 1024 * 1024,  8),
    ]
    RETRY_STATUS_CODES = {500, 502, 503, 504}

    TOKEN_TTL = 20 * 60   # re-auth every 20 min (PikPak token ~30 min)

    def _check_file_integrity(self, file_path: Path) -> bool:
        """Check file integrity using ffmpeg for media files."""
        ext = file_path.suffix.lower()
        media_exts = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.ts', '.m4v'}
        
        if ext in media_exts:
            try:
                import subprocess
                result = subprocess.run(
                    ['ffmpeg', '-i', str(file_path), '-f', 'null', '-'],
                    capture_output=True, timeout=30, check=False
                )
                is_valid = result.returncode == 0
                if not is_valid:
                    logger.warning("File integrity check failed for %s (ffmpeg error)", file_path)
                return is_valid
            except subprocess.TimeoutExpired:
                logger.warning("File integrity check timeout for %s", file_path)
                return False
            except FileNotFoundError:
                logger.warning("FFmpeg not found, skipping integrity check for %s", file_path)
                return True  # Assume ok if ffmpeg not available
            except Exception as e:
                logger.exception("Error checking file integrity for %s", file_path)
                return False
        else:
            # For non-media files, assume ok (size check is sufficient)
            return True

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

    def reset_progress(self):
        self.progress_data     = {}
        self.monitor_active    = False
        self.total_files_count = 0
        self.total_batch_size  = 0
        self.cancel_event      = threading.Event()

    async def _ensure_token(self, api) -> bool:
        async with self._token_lock:
            now = time.time()
            if now - self._last_refresh < self.TOKEN_TTL:
                return True
            ok = await api.refresh_token()
            if ok:
                self._last_refresh = time.time()
            return ok

    # ── Formatting ────────────────────────────────────────────────────────────

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
        return [int(s) if s.isdigit() else s.lower()
                for s in re.split(r'(\d+)', item['name'])]

    def _recursive_sort(self, node):
        if 'files'   in node: node['files'].sort(key=self._natural_key)
        if 'folders' in node:
            node['folders'].sort(key=self._natural_key)
            for f in node['folders']: self._recursive_sort(f)

    def _resolve_conn(self, file_size: int) -> int:
        cfg = Config.CONCURRENT_THREADS
        for threshold, auto in self._AUTO_CONN:
            if file_size >= threshold:
                return max(cfg, auto)
        return max(cfg, 4)

    # ── Tree / API ────────────────────────────────────────────────────────────

    async def get_tree_and_prepare(self, url, password):
        m = re.search(r"/s/([A-Za-z0-9_-]+)", url)
        if not m:
            console.print(f"[bold red]{Language.get('link_invalid')}[/]")
            return None
        share_id = m.group(1)
        if not await self.api.refresh_token(): return None
        files, ptoken = await self.api.get_share_info(share_id, password)
        if not files:
            console.print(f"[bold red]{Language.get('no_files')}[/]")
            return None
        console.print(f"[cyan]{Language.get('analyzing')}[/]")
        tree = await self.tree_builder.build_tree(files, "", share_id, ptoken)
        self._recursive_sort(tree)
        return {"folders": tree["folders"], "files": tree["files"],
                "share_id": share_id, "pass_token": ptoken}

    # ── Monitor / dashboard ───────────────────────────────────────────────────

    def start_monitor(self, total_count, total_size_bytes):
        self.monitor_active    = True
        self.total_files_count = total_count
        self.total_batch_size  = total_size_bytes
        self.batch_start_time  = time.time()

    def stop_monitor(self): self.monitor_active = False

    def generate_dashboard_table(self):
        all_threads     = list(self.progress_data.values())
        done_count      = sum(1 for p in all_threads if p['status'] == DONE_STATUS)
        skipped_count   = sum(1 for p in all_threads if p['status'] == SKIP_STATUS)
        cancelled_count = sum(1 for p in all_threads if p['status'] == "Cancelled")
        display_list    = [p for p in all_threads
                           if p['status'] not in (*GOOD_STATUSES, "Cancelled")]
        total_speed      = sum(p['speed'] for p in display_list)
        total_downloaded = sum(p.get('done_bytes', 0) for p in all_threads)
        remaining        = max(0, self.total_batch_size - total_downloaded)
        eta_str          = self.format_time(remaining / total_speed) if total_speed > 0 else "--:--"

        cancel_hint = (
            "[bold red] ⛔ CANCELLING...[/]"
            if self.cancel_event.is_set()
            else "  [dim]Press [bold]Q[/bold] to cancel[/]"
        )

        stats_grid = Table.grid(expand=True)
        stats_grid.add_column(justify="center", ratio=1)
        stats_grid.add_column(justify="center", ratio=1)
        stats_grid.add_column(justify="center", ratio=1)
        stats_grid.add_row(
            f"[bold cyan]Queue: {self.total_files_count - done_count - skipped_count - cancelled_count}[/]",
            f"[bold green]Done: {done_count}[/] | [bold yellow]Skip: {skipped_count}[/]"
            + (f" | [bold red]Cancel: {cancelled_count}[/]" if cancelled_count else ""),
            f"[bold white]Speed: {self.format_size(total_speed)}/s | ETA: {eta_str}[/]"
        )
        panel_stats = Panel(
            Group(stats_grid, cancel_hint),
            style="blue",
            title=f"[bold]{Language.get('global_stats')}[/]"
        )

        task_table = Table(box=box.SIMPLE, show_header=True,
                           header_style="bold cyan", expand=True)
        task_table.add_column("ID",       width=4)
        task_table.add_column("Filename", ratio=3)
        task_table.add_column("Progress", ratio=2)
        task_table.add_column("Speed",    width=12, justify="right")
        task_table.add_column("ETA",      width=10, justify="right")
        task_table.add_column("Status",   width=14, justify="center")

        for p in sorted(display_list, key=lambda x: x['id']):
            pct = p['percent']
            bad = p['status'] in ("Error", "Failed", "Restore Fail",
                                  "Too Large", "Cancelled", "Cancelling...")
            bar_color = "red" if bad else ("green" if pct == 100 else "cyan")
            filled = int(20 * pct / 100)
            bar    = f"[{bar_color}]{'━'*filled}[/][dim white]{'━'*(20-filled)}[/]"
            if bad:                              ss = "bold red"
            elif p['status'] == "Cancelling...": ss = "bold yellow"
            else:                                ss = "cyan"
            task_table.add_row(
                str(p['id']), p['name'], f"{bar} {pct:.0f}%",
                f"{self.format_size(p['speed'])}/s",
                self.format_time(p.get('eta', 0)),
                f"[{ss}]{p['status']}[/]"
            )
        return Group(panel_stats, task_table)

    # ── Segment worker ────────────────────────────────────────────────────────

    async def _fetch_segment(self, url, headers, file_path,
                       seg_start, seg_end, thread_id, shared,
                       shared_lock, dl_start):
        h = headers.copy()
        h['Range'] = f"bytes={seg_start}-{seg_end}"
        last_ui = time.time()
        local_bytes = 0

        proxy = Config.get_proxy_dict()
        proxy_url = proxy.get('http') if proxy else None
        proxy_exc = getattr(aiohttp, "ClientProxyConnectionError", aiohttp.ClientHttpProxyError)
        timeout = ClientTimeout(total=60)

        for attempt in range(5):
            if self.cancel_event.is_set():
                return False
            try:
                connector = aiohttp.TCPConnector(limit=1, limit_per_host=1, verify_ssl=False)
                async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                    async with session.get(url, headers=h, proxy=proxy_url) as r:
                        if r.status in (401, 403):
                            await asyncio.sleep(1); continue
                        r.raise_for_status()
                        async with aiofiles.open(file_path, "r+b") as f:
                            await f.seek(seg_start)
                            async for chunk in r.content.iter_chunked(self.CHUNK_SIZE):
                                if self.cancel_event.is_set():
                                    return False
                                if not chunk: continue
                                await f.write(chunk)
                                local_bytes += len(chunk)
                                now = time.time()
                                if now - last_ui >= self.UPDATE_INTERVAL:
                                    elapsed = max(now - dl_start, 0.001)
                                    with shared_lock:
                                        shared['done_bytes'] += local_bytes
                                        done    = shared['done_bytes']
                                        total   = shared['total']
                                        speed   = done / elapsed
                                        percent = done / total * 100 if total else 0
                                        eta     = (total - done) / speed if speed > 0 else 0
                                        shared.update({'speed': speed,
                                                       'percent': percent, 'eta': eta})
                                        self.progress_data[thread_id].update({
                                            'done_bytes': done, 'speed': speed,
                                            'percent': percent, 'eta': eta,
                                        })
                                    local_bytes = 0
                                    last_ui = now
                # flush remaining
                if local_bytes > 0:
                    with shared_lock:
                        shared['done_bytes'] += local_bytes
                        done    = shared['done_bytes']
                        total   = shared['total']
                        elapsed = max(time.time() - dl_start, 0.001)
                        speed   = done / elapsed
                        percent = done / total * 100 if total else 0
                        eta     = (total - done) / speed if speed > 0 else 0
                        shared.update({'speed': speed, 'percent': percent, 'eta': eta})
                        self.progress_data[thread_id].update({
                            'done_bytes': done, 'speed': speed,
                            'percent': percent, 'eta': eta,
                        })
                return True
            except (aiohttp.ClientHttpProxyError, proxy_exc):
                if proxy_url is not None:
                    logger.warning("Proxy failed for segment %s-%s, retrying without proxy", seg_start, seg_end)
                    proxy_url = None
                    await asyncio.sleep(1)
                    continue
                logger.exception("Segment download proxy error without proxy available: %s %s-%s", url, seg_start, seg_end)
                await asyncio.sleep(min(2 ** attempt, 8))
            except Exception as e:
                logger.exception("Segment download error: %s %s-%s attempt=%s", url, seg_start, seg_end, attempt)
                await asyncio.sleep(min(2 ** attempt, 8))
        logger.error("Failed to download segment after retries: %s %s-%s", url, seg_start, seg_end)
        return False

    # ── Multi-connection download ─────────────────────────────────────────────

    async def _multi_conn_download(self, urls, headers, file_path, file_size,
                             thread_id, num_conn, get_fresh_urls):
        """
        Tải file bằng nhiều connection song song.

        urls          : list URL — 1 URL per account (stripe mode).
                        Nếu chỉ có 1 URL thì dùng 1 account như bình thường.
        get_fresh_urls: callable() → list[str|None] — refresh tất cả URL khi expire.
        num_conn      : tổng số worker thread (phân bổ đều giữa các URL/account).

        Stripe logic:
          segment_index % len(urls) → chọn URL nào
          → 2 account = mỗi account tải 50% segment → tốc độ nhân đôi.
        """
        if isinstance(urls, str):
            urls = [urls]   # backward compat

        n_urls = len([u for u in urls if u])   # số URL hợp lệ
        if n_urls == 0:
            logger.error("No valid download URLs provided for file %s", file_path)
            return False

        logger.debug("Starting multi-connection download for %s with %s urls and %s conns", file_path, n_urls, num_conn)
        # Build segment queue — mỗi segment mang theo index để stripe
        seg_q    = _queue.Queue()
        pos      = 0
        seg_idx  = 0
        while pos < file_size:
            end = min(pos + self.SEGMENT_SIZE - 1, file_size - 1)
            seg_q.put((seg_idx, pos, end))
            pos     = end + 1
            seg_idx += 1

        # Create empty file instead of pre-allocating full size
        try:
            with open(file_path, "wb") as f:
                pass  # Just create the file, let it grow as data is written
        except Exception:
            self.progress_data[thread_id]['status'] = "Disk Error"
            return False

        shared      = {'done_bytes': 0, 'speed': 0.0,
                       'percent': 0.0, 'eta': 0.0, 'total': file_size}
        shared_lock = threading.Lock()
        dl_start    = time.time()
        url_holders = list(urls)   # mutable list, index-mapped per account
        url_lock    = threading.Lock()
        failed_segs = []
        fail_lock   = threading.Lock()

        acc_label = f"{n_urls} acc" if n_urls > 1 else f"{num_conn} conn"
        self.progress_data[thread_id]['status'] = f"DL x{acc_label}"

        def worker():
            while True:
                if self.cancel_event.is_set(): break
                try:
                    seg_i, seg_start, seg_end = seg_q.get_nowait()
                except _queue.Empty:
                    break

                # Stripe: chọn URL theo segment index
                url_idx = seg_i % len(url_holders)
                with url_lock:
                    cur_url = url_holders[url_idx]

                if not cur_url:
                    # URL này null — thử URL khác
                    with url_lock:
                        cur_url = next((u for u in url_holders if u), None)
                    if not cur_url:
                        with fail_lock: failed_segs.append((seg_i, seg_start, seg_end))
                        seg_q.task_done(); continue

                ok = asyncio.run(self._fetch_segment(
                    cur_url, headers, file_path,
                    seg_start, seg_end, thread_id, shared, shared_lock, dl_start))

                if not ok and not self.cancel_event.is_set():
                    # Refresh tất cả URL rồi retry segment này
                    fresh = get_fresh_urls()
                    if fresh:
                        with url_lock:
                            for i, u in enumerate(fresh):
                                if u and i < len(url_holders):
                                    url_holders[i] = u
                        with url_lock:
                            cur_url = url_holders[url_idx] or next(
                                (u for u in url_holders if u), None)
                    if cur_url:
                        ok = asyncio.run(self._fetch_segment(
                            cur_url, headers, file_path,
                            seg_start, seg_end, thread_id, shared, shared_lock, dl_start))

                if not ok:
                    with fail_lock: failed_segs.append((seg_i, seg_start, seg_end))
                seg_q.task_done()

        workers = [threading.Thread(target=worker, daemon=True)
                   for _ in range(num_conn)]
        for w in workers: w.start()
        for w in workers: w.join()

        if self.cancel_event.is_set():
            return False

        # Retry failed segments single-thread
        if failed_segs:
            self.progress_data[thread_id]['status'] = f"Retry {len(failed_segs)} segs"
            fresh = get_fresh_urls()
            retry_url = next((u for u in (fresh or url_holders) if u), None)
            for seg_i, seg_start, seg_end in failed_segs:
                if self.cancel_event.is_set():
                    return False
                ok = asyncio.run(self._fetch_segment(
                    retry_url, headers, file_path,
                    seg_start, seg_end, thread_id, shared, shared_lock, dl_start))
                if not ok:
                    return False

        return file_path.exists() and file_path.stat().st_size == file_size

    async def _download_direct_file(self, file_data, share_id, pass_token, thread_id, force_no_proxy: bool = False):
        name            = file_data['name']
        real_total_size = int(file_data['size'])
        self.progress_data[thread_id] = {
            'id': thread_id, 'name': name, 'percent': 0, 'speed': 0,
            'status': "Init...", 'done_bytes': 0,
            'total_bytes': real_total_size, 'eta': 0
        }
        logger.info("Fallback direct download for %s (%s bytes) force_no_proxy=%s", name, real_total_size, force_no_proxy)

        save_dir  = Config.get_download_dir() / Path(file_data['path']).parent
        save_dir.mkdir(parents=True, exist_ok=True)
        file_path = save_dir / name
        temp_file = file_path.parent / f".{file_path.name}.tmp"

        BASE_HEADERS = {
            "User-Agent":      ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) "
                                "Chrome/124.0.0.0 Safari/537.36"),
            "Referer":         "https://mypikpak.com/",
            "Accept":          "*/*",
            "Accept-Encoding": "identity",
            "Connection":      "keep-alive",
        }

        def _clean_local():
            for p in (file_path, temp_file):
                try:
                    if p.exists(): p.unlink()
                except: pass

        def _set_status(s): self.progress_data[thread_id]['status'] = s
        def _cancel_cleanup(): _clean_local(); _set_status('Cancelled')

        if self.cancel_event.is_set():
            _cancel_cleanup(); return False

        download_url = await self.api.get_download_url(share_id, file_data['id'], pass_token)
        if not download_url:
            _set_status("No URL")
            logger.error("Fallback direct download had no URL for %s", name)
            return False
        logger.info("Fallback direct download URL resolved for %s", name)

        # Check if we can use FFmpeg for media files
        ext = file_path.suffix.lower()
        media_exts = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.ts', '.m4v'}
        use_ffmpeg = ext in media_exts

        if use_ffmpeg:
            # Use FFmpeg for media files
            logger.info("Using FFmpeg to download media file %s", name)
            _set_status("FFmpeg Init...")
            
            # Build headers string
            headers_list = [f"{k}: {v}" for k, v in BASE_HEADERS.items()]
            headers_str = '\n'.join(headers_list)
            
            # FFmpeg command
            cmd = [
                'ffmpeg',
                '-headers', headers_str,
                '-i', download_url,
                '-c', 'copy',
                '-y',  # overwrite
                str(temp_file)
            ]
            
            # Add proxy if available
            env = os.environ.copy()
            if proxy_url:
                env['http_proxy'] = proxy_url
                env['https_proxy'] = proxy_url
            
            try:
                _set_status("FFmpeg DL...")
                process = subprocess.run(cmd, env=env, timeout=Config.TIMEOUT, check=False)
                if process.returncode == 0 and temp_file.exists():
                    temp_file.rename(file_path)
                    if self._check_file_integrity(file_path):
                        self.progress_data[thread_id].update(
                            {'percent': 100, 'status': DONE_STATUS})
                        return True
                    else:
                        logger.warning("FFmpeg downloaded file %s is corrupted", file_path)
                        try:
                            file_path.unlink()
                        except:
                            pass
                        return False
                else:
                    logger.error("FFmpeg download failed for %s (exit code %s)", name, process.returncode)
                    return False
            except subprocess.TimeoutExpired:
                logger.error("FFmpeg download timeout for %s", name)
                return False
            except FileNotFoundError:
                logger.warning("FFmpeg not found, falling back to HTTP download for %s", name)
                use_ffmpeg = False
            except Exception as e:
                logger.exception("FFmpeg download error for %s", name)
                return False

        # Skip nếu đã có file đủ size và valid (for both FFmpeg and HTTP)
        if file_path.exists() and file_path.stat().st_size == real_total_size:
            if self._check_file_integrity(file_path):
                self.progress_data[thread_id].update(
                    {'percent': 100, 'status': SKIP_STATUS})
                return True
            else:
                logger.info("Existing file %s is corrupted, removing and re-downloading", file_path)
                try:
                    file_path.unlink()
                except:
                    pass

        supports_range = False
        connector = aiohttp.TCPConnector(verify_ssl=False)
        proxy = Config.get_proxy_dict() if not force_no_proxy else None
        proxy_url = proxy.get('http') if proxy else None
        proxy_exc = getattr(aiohttp, "ClientProxyConnectionError", aiohttp.ClientHttpProxyError)
        try:
            async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.head(download_url, headers=BASE_HEADERS, proxy=proxy_url) as probe:
                    supports_range = (probe.status == 200 and 'bytes' in probe.headers.get('Accept-Ranges', ''))
                    logger.debug("Fallback range support for %s: %s (status=%s) proxy=%s", name, supports_range, probe.status, bool(proxy_url))
        except (aiohttp.ClientHttpProxyError, proxy_exc):
            if proxy_url is not None:
                logger.warning("Proxy HEAD probe failed for %s, retrying without proxy", name)
                proxy_url = None
                try:
                    async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=10)) as session:
                        async with session.head(download_url, headers=BASE_HEADERS, proxy=None) as probe:
                            supports_range = (probe.status == 200 and 'bytes' in probe.headers.get('Accept-Ranges', ''))
                            logger.debug("Fallback range support for %s: %s (status=%s) proxy=false", name, supports_range, probe.status)
                except Exception:
                    logger.exception("Direct HEAD probe failed for %s", name)
        except Exception:
            logger.exception("Error probing fallback direct URL range support for %s", name)

        if supports_range and real_total_size > 1 * 1024 * 1024:
            num_conn = self._resolve_conn(real_total_size)
            _set_status(f"DL x{num_conn} conn")
            logger.info("Using fallback segmented download for %s with %s connections", name, num_conn)
            ok = await self._multi_conn_download(
                [download_url], BASE_HEADERS, file_path,
                real_total_size, thread_id, num_conn,
                lambda: [download_url]
            )
            if self.cancel_event.is_set():
                _cancel_cleanup(); return False
            if ok:
                self.progress_data[thread_id].update(
                    {'percent': 100, 'speed': 0, 'status': DONE_STATUS,
                     'done_bytes': real_total_size})
                return True
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
                    self.progress_data[thread_id].update(
                        {'percent': 100, 'status': DONE_STATUS})
                    return True

            connector = aiohttp.TCPConnector(verify_ssl=False)
            proxy = Config.get_proxy_dict() if not force_no_proxy else None
            proxy_url = proxy.get('http') if proxy else None
            timeout = aiohttp.ClientTimeout(total=Config.TIMEOUT)
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                try:
                    async with session.get(download_url, headers=h, proxy=proxy_url) as r:
                        if resume_pos > 0 and r.status == 200:
                            resume_pos = 0
                            mode = 'wb'
                            temp_file.unlink(missing_ok=True)
                        if r.status not in (200, 206):
                            _clean_local()
                            _set_status(f"Err {r.status}")
                            return False

                        done = resume_pos
                        start = time.time()
                        last_t = start
                        last_d = done
                        async with aiofiles.open(temp_file, mode) as f:
                            async for chunk in r.content.iter_chunked(self.CHUNK_SIZE):
                                if self.cancel_event.is_set():
                                    _cancel_cleanup()
                                    return False
                                if chunk:
                                    await f.write(chunk)
                                    done += len(chunk)
                                    now = time.time()
                                    if now - last_t >= 0.5:
                                        speed = (done - last_d) / (now - last_t)
                                        percent = done / real_total_size * 100 if real_total_size else 0
                                        eta = (real_total_size - done) / speed if speed > 0 else 0
                                        self.progress_data[thread_id].update(
                                            {'percent': percent, 'speed': speed,
                                             'status': "DL...", 'done_bytes': done, 'eta': eta})
                                        last_t = now
                                        last_d = done
                except (aiohttp.ClientHttpProxyError, proxy_exc):
                    if proxy_url is not None and not force_no_proxy:
                        logger.warning("Proxy failed during fallback direct download for %s, retrying without proxy", name)
                        return await self._download_direct_file(file_data, share_id, pass_token, thread_id, force_no_proxy=True)
                    raise

            if temp_file.exists() and temp_file.stat().st_size >= real_total_size:
                temp_file.rename(file_path)
                self.progress_data[thread_id].update(
                    {'percent': 100, 'status': DONE_STATUS})
                return True
            return False

        except Exception as e:
            logger.exception("Fallback direct download failed for %s", name)
            _clean_local()
            _set_status("Error")
            return False

    # ── Main download entry ───────────────────────────────────────────────────

    async def download_single_file(self, file_data, share_id, pass_token, thread_id, api=None):
        # Early cancel
        if self.cancel_event.is_set():
            self.progress_data[thread_id] = {
                'id': thread_id, 'name': file_data['name'],
                'percent': 0, 'speed': 0, 'status': 'Cancelled',
                'done_bytes': 0, 'total_bytes': int(file_data.get('size', 0)), 'eta': 0
            }
            return False

        name            = file_data['name']
        real_total_size = int(file_data['size'])
        HEAVY_EXTS      = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv',
                           '.webm', '.ts', '.iso', '.m4v', '.rar', '.zip', '.7z'}
        use_premium = (any(name.lower().endswith(e) for e in HEAVY_EXTS)
                       or Config.FORCE_PREMIUM_MODE)

        self.progress_data[thread_id] = {
            'id': thread_id, 'name': name, 'percent': 0, 'speed': 0,
            'status': "Init...", 'done_bytes': 0,
            'total_bytes': real_total_size, 'eta': 0
        }
        logger.info("Start file download: %s (%s bytes) premium=%s", name, real_total_size, use_premium)

        pool = get_pool()
        if api is None:
            api = pool.acquire() or self.api

        save_dir  = Config.get_download_dir() / Path(file_data['path']).parent
        save_dir.mkdir(parents=True, exist_ok=True)
        file_path = save_dir / name
        temp_file = file_path.parent / f".{file_path.name}.tmp"

        # Xóa file tạm lỗi từ lần tải trước để tránh resume file corrupted
        if temp_file.exists():
            logger.info("Removing leftover temp file from previous failed download: %s", temp_file)
            temp_file.unlink(missing_ok=True)

        BASE_HEADERS = {
            "User-Agent":      ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) "
                                "Chrome/124.0.0.0 Safari/537.36"),
            "Referer":         "https://mypikpak.com/",
            "Accept":          "*/*",
            "Accept-Encoding": "identity",
            "Connection":      "keep-alive",
        }

        def _clean_local():
            for p in (file_path, temp_file):
                try:
                    if p.exists(): p.unlink()
                except: pass

        def _set_status(s): self.progress_data[thread_id]['status'] = s
        def _cancel_cleanup(): _clean_local(); _set_status('Cancelled')

        # ─────────────────────────────────────────────────────────────────────
        # CASE 1 — PREMIUM / HEAVY  (Restore → multi-conn DL → Cloud cleanup)
        # ─────────────────────────────────────────────────────────────────────
        if use_premium:
            if file_path.exists():
                if file_path.stat().st_size == real_total_size:
                    self.progress_data[thread_id].update(
                        {'percent': 100, 'speed': 0, 'status': SKIP_STATUS,
                         'done_bytes': real_total_size})
                    return True
                file_path.unlink(missing_ok=True)

            my_file_id    = None
            dl_success    = False
            was_cancelled = False

            pool = get_pool()
            api  = pool.acquire() or self.api
            _set_status("Refreshing token...")
            if not await self._ensure_token(api):
                _set_status("Auth Fail")
                logger.error("Token refresh failed for premium download: %s", name)
                return False
            logger.debug("Token refreshed for premium download: %s", name)
            if pool.size() > 1:
                _set_status(f"Init [{pool.size()} acc]...")

            try:
                # 0. Pre-cleanup
                if self.cancel_event.is_set():
                    was_cancelled = True; _set_status('Cancelled'); return False
                _set_status("Checking cloud...")
                stale = await api.wait_for_file(name, max_retries=1)
                if stale:
                    logger.info("Deleting stale cloud file for %s: %s", name, stale)
                    _set_status("Cleaning Old...")
                    await api.delete_file(stale); await asyncio.sleep(0.5)

                # 1. Restore
                if self.cancel_event.is_set():
                    was_cancelled = True; _set_status('Cancelled'); return False
                _set_status(Language.get('status_restore'))
                my_file_id, error = await api.restore_and_poll(share_id, file_data['id'], pass_token)
                if not my_file_id:
                    if error == "file_space_not_enough":
                        logger.warning("Primary account storage full (file_space_not_enough), trying secondary account...")
                        alt_api = pool.acquire()
                        if alt_api:
                            my_file_id, alt_error = await alt_api.restore_and_poll(share_id, file_data['id'], pass_token)
                            if my_file_id:
                                logger.info("Successfully restored using secondary account")
                                api = alt_api  # Switch to alt_api for subsequent operations
                            else:
                                logger.error("Secondary account also failed: %s", alt_error or "unknown")
                        else:
                            logger.warning("No secondary account available for restore")
                    if not my_file_id:
                        _set_status(Language.get('status_check'))
                        my_file_id, _ = await api.wait_for_file(name, max_retries=15)
                if not my_file_id:
                    _set_status("Fallback...")
                    logger.warning("Premium restore failed for %s, switching to direct download", name)
                    return await self._download_direct_file(file_data, share_id, pass_token, thread_id)

                # 2. Get download link (retry up to 5×)
                if self.cancel_event.is_set():
                    was_cancelled = True; _cancel_cleanup(); return False
                _set_status(Language.get('status_getlink'))
                download_url = None
                for _ in range(5):
                    download_url = await api.get_user_file_url(my_file_id)
                    if download_url: break
                    await asyncio.sleep(1.5)
                if not download_url:
                    _set_status("No Link")
                    logger.error("Unable to obtain premium download URL for %s", name)
                    return False
                logger.info("Obtained premium download URL for %s", name)

                # 3. Multi-connection download — stripe segments giữa các account
                if self.cancel_event.is_set():
                    was_cancelled = True; _cancel_cleanup(); return False

                num_conn = self._resolve_conn(real_total_size)

                # Lấy download URL từ TẤT CẢ account song song
                # Mỗi account có URL riêng → stripe segment → tốc độ nhân lên
                all_apis = pool.all_apis() or [api]

                async def _get_url_for(a, fid):
                    for _ in range(3):
                        u = await a.get_user_file_url(fid)
                        if u: return u
                        await asyncio.sleep(1)
                    return None

                # Lấy URL song song từ mọi account
                stripe_urls = await pool.get_stripe_urls_async(
                    lambda a: _get_url_for(a, my_file_id)
                ) or [download_url]

                # Loại None, đảm bảo có ít nhất 1 URL
                stripe_urls = [u for u in stripe_urls if u] or [download_url]

                n_acc = len(stripe_urls)
                _set_status(f"DL x{n_acc} acc / {num_conn} conn")
                logger.info("Starting stripe download for %s with %s accounts", name, n_acc)

                def _get_fresh_urls():
                    async def inner():
                        return await pool.get_stripe_urls_async(
                            lambda a: _get_url_for(a, my_file_id)
                        )
                    return asyncio.run(inner()) or [download_url]

                dl_success = await self._multi_conn_download(
                    stripe_urls, BASE_HEADERS, file_path,
                    real_total_size, thread_id, num_conn, _get_fresh_urls
                )

                if self.cancel_event.is_set():
                    was_cancelled = True; _cancel_cleanup(); return False

                if dl_success:
                    if not self._check_file_integrity(file_path):
                        logger.warning("Downloaded file %s is corrupted, removing and marking as failed", file_path)
                        try:
                            file_path.unlink()
                        except:
                            pass
                        dl_success = False
                        _set_status("Corrupted")
                    else:
                        self.progress_data[thread_id].update(
                            {'percent': 100, 'speed': 0, 'status': DONE_STATUS,
                             'done_bytes': real_total_size})
                else:
                    _set_status("Failed"); _clean_local()

            finally:
                if my_file_id:
                    _set_status("Cloud Clean..." if was_cancelled
                                else Language.get('status_clean'))
                    await api.delete_file(my_file_id)
                    if dl_success:      _set_status(DONE_STATUS)
                    elif was_cancelled: _set_status("Cancelled")

            return dl_success

        # ─────────────────────────────────────────────────────────────────────
        # CASE 2 — DIRECT / LIGHT  (multi-connection download trực tiếp)
        # ─────────────────────────────────────────────────────────────────────
        else:
            if self.cancel_event.is_set():
                _cancel_cleanup(); return False

            download_url = await api.get_download_url(share_id, file_data['id'], pass_token)
            if not download_url:
                _set_status("No URL")
                logger.error("No direct download URL for %s", name)
                return False
            logger.info("Direct download URL resolved for %s", name)

            # Skip nếu đã có file đủ size và valid
            if file_path.exists() and file_path.stat().st_size == real_total_size:
                if self._check_file_integrity(file_path):
                    self.progress_data[thread_id].update(
                        {'percent': 100, 'status': SKIP_STATUS})
                    return True
                else:
                    logger.info("Existing file %s is corrupted, removing and re-downloading", file_path)
                    try:
                        file_path.unlink()
                    except:
                        pass

            # Kiểm tra server hỗ trợ Range
            supports_range = False
            try:
                connector = aiohttp.TCPConnector(verify_ssl=False)
                proxy = Config.get_proxy_dict()
                proxy_url = proxy.get('http') if proxy else None
                async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=10)) as session:
                    async with session.head(download_url, headers=BASE_HEADERS, proxy=proxy_url) as probe:
                        supports_range = (probe.status == 200 and 'bytes' in probe.headers.get('Accept-Ranges', ''))
                        logger.debug("Range support for %s: %s (status=%s)", name, supports_range, probe.status)
            except Exception as e:
                logger.exception("Error probing direct URL range support for %s", name)

            # Multi-connection nếu hỗ trợ Range và file đủ lớn
            if supports_range and real_total_size > 1 * 1024 * 1024:
                num_conn = self._resolve_conn(real_total_size)
                _set_status(f"DL x{num_conn} conn")
                logger.info("Using segmented download for %s with %s connections", name, num_conn)
                def _get_fresh_urls():
                    async def inner():
                        fresh_url = await self.api.get_download_url(share_id, file_data['id'], pass_token)
                        return [fresh_url] if fresh_url else [download_url]
                    return asyncio.run(inner())

                ok = await self._multi_conn_download(
                    [download_url], BASE_HEADERS, file_path,
                    real_total_size, thread_id, num_conn,
                    _get_fresh_urls
                )
                if self.cancel_event.is_set():
                    _cancel_cleanup(); return False
                if ok:
                    self.progress_data[thread_id].update(
                        {'percent': 100, 'speed': 0, 'status': DONE_STATUS,
                         'done_bytes': real_total_size})
                    return True
                _clean_local()
                _set_status("Fallback...")

            # Single-thread fallback
            logger.info("Starting direct fallback download for %s", name)
            proxy_exc = getattr(aiohttp, "ClientProxyConnectionError", aiohttp.ClientHttpProxyError)
            max_direct_attempts = 3
            for attempt in range(1, max_direct_attempts + 1):
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
                            self.progress_data[thread_id].update(
                                {'percent': 100, 'status': DONE_STATUS})
                            return True

                    connector = aiohttp.TCPConnector(verify_ssl=False)
                    proxy = Config.get_proxy_dict()
                    proxy_url = proxy.get('http') if proxy else None
                    timeout = aiohttp.ClientTimeout(total=Config.TIMEOUT)
                    if attempt > 1:
                        download_url = await self.api.get_download_url(share_id, file_data['id'], pass_token)
                        if not download_url:
                            logger.error("No direct download URL for %s on retry %s", name, attempt)
                            return False
                    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                        async with session.get(download_url, headers=h, proxy=proxy_url) as r:
                            if resume_pos > 0 and r.status == 200:
                                resume_pos = 0
                                mode = 'wb'
                                temp_file.unlink(missing_ok=True)
                            if r.status not in (200, 206):
                                if r.status in self.RETRY_STATUS_CODES and attempt < max_direct_attempts:
                                    logger.warning("Direct fallback download %s received %s, retrying attempt %s/%s", name, r.status, attempt, max_direct_attempts)
                                    await asyncio.sleep(2 ** attempt)
                                    continue
                                _clean_local()
                                _set_status(f"Err {r.status}")
                                return False

                            done = resume_pos
                            start = time.time()
                            last_t = start
                            last_d = done
                            async with aiofiles.open(temp_file, mode) as f:
                                async for chunk in r.content.iter_chunked(self.CHUNK_SIZE):
                                    if self.cancel_event.is_set():
                                        _cancel_cleanup()
                                        return False
                                    if chunk:
                                        await f.write(chunk)
                                        done += len(chunk)
                                        now = time.time()
                                        if now - last_t >= 0.5:
                                            speed = (done - last_d) / (now - last_t)
                                            percent = done / real_total_size * 100 if real_total_size else 0
                                            eta = (real_total_size - done) / speed if speed > 0 else 0
                                            self.progress_data[thread_id].update(
                                                {'percent': percent, 'speed': speed,
                                                 'status': "DL...", 'done_bytes': done, 'eta': eta})
                                            last_t = now
                                            last_d = done

                    if temp_file.exists() and temp_file.stat().st_size >= real_total_size:
                        temp_file.rename(file_path)
                        if self._check_file_integrity(file_path):
                            self.progress_data[thread_id].update(
                                {'percent': 100, 'status': DONE_STATUS})
                            return True
                        else:
                            logger.warning("Downloaded file %s is corrupted, removing and retrying", file_path)
                            try:
                                file_path.unlink()
                            except:
                                pass
                            if attempt < max_direct_attempts:
                                continue
                            return False
                    if attempt < max_direct_attempts:
                        logger.warning("Direct fallback download incomplete for %s, retrying attempt %s/%s", name, attempt, max_direct_attempts)
                        await asyncio.sleep(2 ** attempt)
                        continue
                    return False

                except (aiohttp.ClientHttpProxyError, proxy_exc):
                    if proxy_url is not None and not force_no_proxy:
                        logger.warning("Proxy failed during fallback direct download for %s, retrying without proxy", name)
                        return await self._download_direct_file(file_data, share_id, pass_token, thread_id, force_no_proxy=True)
                    raise
                except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as e:
                    if attempt < max_direct_attempts:
                        logger.warning("Direct fallback connection failed for %s attempt %s/%s: %s", name, attempt, max_direct_attempts, type(e).__name__)
                        await asyncio.sleep(2 ** attempt)
                        continue
                    raise
                except Exception:
                    logger.exception("Direct fallback download failed for %s", name)
                    _clean_local()
                    _set_status("Error")
                    return False

            _clean_local()
            _set_status("Error")
            return False