import threading
import time
import os
import requests
import re
import queue as _queue
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from rich.console import Group
from rich.table import Table
from rich.panel import Panel
from rich import box
from config.settings import Config, console, Language
from core.api import PikPakAPI, TreeBuilder
from core.account_pool import get_pool


# ── Shared status constants ────────────────────────────────────────────────────
DONE_STATUS   = "Done"
SKIP_STATUS   = "Skipped"
GOOD_STATUSES = {DONE_STATUS, SKIP_STATUS}


# ── Connection pool factory ───────────────────────────────────────────────────
def _make_session(pool_size: int) -> requests.Session:
    """Session với connection pool + retry. Proxy chỉ dùng khi tải file."""
    session = requests.Session()
    proxy = Config.get_proxy_dict()
    if proxy:
        session.proxies.update(proxy)
    retry = Retry(
        total=3, backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(
        pool_connections=pool_size,
        pool_maxsize=pool_size,
        max_retries=retry,
    )
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    return session


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

    TOKEN_TTL = 20 * 60   # re-auth every 20 min (PikPak token ~30 min)

    def __init__(self):
        self.api               = PikPakAPI()
        self.tree_builder      = TreeBuilder(self.api)
        self.progress_data     = {}
        self.monitor_active    = False
        self.total_files_count = 0
        self.total_batch_size  = 0
        self.cancel_event      = threading.Event()
        self._last_refresh     = 0.0
        self._token_lock       = threading.Lock()

    def reset_progress(self):
        self.progress_data     = {}
        self.monitor_active    = False
        self.total_files_count = 0
        self.total_batch_size  = 0
        self.cancel_event      = threading.Event()

    def _ensure_token(self, api) -> bool:
        now = time.time()
        with self._token_lock:
            if now - self._last_refresh >= self.TOKEN_TTL:
                ok = api.refresh_token()
                if ok:
                    self._last_refresh = now
                return ok
        return True

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

    def get_tree_and_prepare(self, url, password):
        m = re.search(r"/s/([A-Za-z0-9_-]+)", url)
        if not m:
            console.print(f"[bold red]{Language.get('link_invalid')}[/]")
            return None
        share_id = m.group(1)
        if not self.api.refresh_token(): return None
        files, ptoken = self.api.get_share_info(share_id, password)
        if not files:
            console.print(f"[bold red]{Language.get('no_files')}[/]")
            return None
        console.print(f"[cyan]{Language.get('analyzing')}[/]")
        tree = self.tree_builder.build_tree(files, "", share_id, ptoken)
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

    def _fetch_segment(self, session, url, headers, file_path,
                       seg_start, seg_end, thread_id, shared,
                       shared_lock, dl_start):
        h = headers.copy()
        h['Range'] = f"bytes={seg_start}-{seg_end}"
        last_ui = time.time()
        local_bytes = 0

        for attempt in range(5):
            if self.cancel_event.is_set():
                return False
            try:
                with session.get(url, headers=h, stream=True,
                                 timeout=60, verify=False) as r:
                    if r.status_code in (401, 403):
                        time.sleep(1); continue
                    r.raise_for_status()
                    with open(file_path, "r+b") as f:
                        f.seek(seg_start)
                        for chunk in r.iter_content(chunk_size=self.CHUNK_SIZE):
                            if self.cancel_event.is_set():
                                return False
                            if not chunk: continue
                            f.write(chunk)
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
            except Exception:
                time.sleep(min(2 ** attempt, 8))
        return False

    # ── Multi-connection download ─────────────────────────────────────────────

    def _multi_conn_download(self, url, headers, file_path, file_size,
                             thread_id, num_conn, get_fresh_url):
        # Build segment queue
        seg_q = _queue.Queue()
        pos   = 0
        while pos < file_size:
            end = min(pos + self.SEGMENT_SIZE - 1, file_size - 1)
            seg_q.put((pos, end))
            pos = end + 1

        # Pre-allocate file
        try:
            with open(file_path, "wb") as f:
                f.truncate(file_size)
        except Exception:
            self.progress_data[thread_id]['status'] = "Disk Error"
            return False

        shared      = {'done_bytes': 0, 'speed': 0.0,
                       'percent': 0.0, 'eta': 0.0, 'total': file_size}
        shared_lock = threading.Lock()
        dl_start    = time.time()
        url_holder  = [url]
        url_lock    = threading.Lock()
        failed_segs = []
        fail_lock   = threading.Lock()

        self.progress_data[thread_id]['status'] = f"DL x{num_conn} conn"
        session = _make_session(num_conn)

        def worker():
            while True:
                if self.cancel_event.is_set(): break
                try:
                    seg_start, seg_end = seg_q.get_nowait()
                except _queue.Empty:
                    break
                with url_lock:
                    cur_url = url_holder[0]
                ok = self._fetch_segment(
                    session, cur_url, headers, file_path,
                    seg_start, seg_end, thread_id, shared, shared_lock, dl_start)
                if not ok and not self.cancel_event.is_set():
                    new_url = get_fresh_url()
                    if new_url:
                        with url_lock: url_holder[0] = new_url
                        ok = self._fetch_segment(
                            session, new_url, headers, file_path,
                            seg_start, seg_end, thread_id, shared, shared_lock, dl_start)
                if not ok:
                    with fail_lock: failed_segs.append((seg_start, seg_end))
                seg_q.task_done()

        workers = [threading.Thread(target=worker, daemon=True)
                   for _ in range(num_conn)]
        for w in workers: w.start()
        for w in workers: w.join()
        session.close()

        if self.cancel_event.is_set():
            return False

        # Retry failed segments
        if failed_segs:
            self.progress_data[thread_id]['status'] = f"Retry {len(failed_segs)} segs"
            retry_session = _make_session(1)
            fresh_url = get_fresh_url() or url_holder[0]
            for seg_start, seg_end in failed_segs:
                if self.cancel_event.is_set():
                    retry_session.close(); return False
                ok = self._fetch_segment(
                    retry_session, fresh_url, headers, file_path,
                    seg_start, seg_end, thread_id, shared, shared_lock, dl_start)
                if not ok:
                    retry_session.close(); return False
            retry_session.close()

        return file_path.exists() and file_path.stat().st_size == file_size

    # ── Main download entry ───────────────────────────────────────────────────

    def download_single_file(self, file_data, share_id, pass_token, thread_id):
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
            self._ensure_token(api)
            if pool.size() > 1:
                _set_status(f"Init [{pool.size()} acc]...")

            try:
                # 0. Pre-cleanup
                if self.cancel_event.is_set():
                    was_cancelled = True; _set_status('Cancelled'); return False
                _set_status("Checking cloud...")
                stale = api.wait_for_file(name, max_retries=1)
                if stale:
                    _set_status("Cleaning Old...")
                    api.delete_file(stale); time.sleep(0.5)

                # 1. Restore
                if self.cancel_event.is_set():
                    was_cancelled = True; _set_status('Cancelled'); return False
                _set_status(Language.get('status_restore'))
                my_file_id = api.restore_and_poll(share_id, file_data['id'], pass_token)
                if not my_file_id:
                    _set_status(Language.get('status_check'))
                    my_file_id = api.wait_for_file(name, max_retries=15)
                if not my_file_id:
                    _set_status("Restore Fail"); return False

                # 2. Get download link (retry up to 5×)
                if self.cancel_event.is_set():
                    was_cancelled = True; _cancel_cleanup(); return False
                _set_status(Language.get('status_getlink'))
                download_url = None
                for _ in range(5):
                    download_url = api.get_user_file_url(my_file_id)
                    if download_url: break
                    time.sleep(1.5)
                if not download_url:
                    _set_status("No Link"); return False

                # 3. Multi-connection download
                if self.cancel_event.is_set():
                    was_cancelled = True; _cancel_cleanup(); return False

                num_conn = self._resolve_conn(real_total_size)
                _set_status(f"DL x{num_conn} conn")

                def _get_fresh():
                    for _ in range(3):
                        u = api.get_user_file_url(my_file_id)
                        if u: return u
                        time.sleep(1)
                    return None

                dl_success = self._multi_conn_download(
                    download_url, BASE_HEADERS, file_path,
                    real_total_size, thread_id, num_conn, _get_fresh
                )

                if self.cancel_event.is_set():
                    was_cancelled = True; _cancel_cleanup(); return False

                if dl_success:
                    self.progress_data[thread_id].update(
                        {'percent': 100, 'speed': 0, 'status': DONE_STATUS,
                         'done_bytes': real_total_size})
                else:
                    _set_status("Failed"); _clean_local()

            finally:
                if my_file_id:
                    _set_status("Cloud Clean..." if was_cancelled
                                else Language.get('status_clean'))
                    api.delete_file(my_file_id)
                    if dl_success:      _set_status(DONE_STATUS)
                    elif was_cancelled: _set_status("Cancelled")

            return dl_success

        # ─────────────────────────────────────────────────────────────────────
        # CASE 2 — DIRECT / LIGHT  (multi-connection download trực tiếp)
        # ─────────────────────────────────────────────────────────────────────
        else:
            if self.cancel_event.is_set():
                _cancel_cleanup(); return False

            download_url = self.api.get_download_url(share_id, file_data['id'], pass_token)
            if not download_url:
                _set_status("No URL"); return False

            # Skip nếu đã có file đủ size
            if file_path.exists() and file_path.stat().st_size == real_total_size:
                self.progress_data[thread_id].update(
                    {'percent': 100, 'status': SKIP_STATUS})
                return True

            # Kiểm tra server hỗ trợ Range
            supports_range = False
            try:
                probe = requests.head(download_url, headers=BASE_HEADERS,
                                      timeout=10, verify=False,
                                      proxies=Config.get_proxy_dict())
                supports_range = (probe.status_code == 200
                                  and 'bytes' in probe.headers.get('Accept-Ranges', ''))
            except: pass

            # Multi-connection nếu hỗ trợ Range và file đủ lớn
            if supports_range and real_total_size > 1 * 1024 * 1024:
                num_conn = self._resolve_conn(real_total_size)
                _set_status(f"DL x{num_conn} conn")
                ok = self._multi_conn_download(
                    download_url, BASE_HEADERS, file_path,
                    real_total_size, thread_id, num_conn,
                    lambda: None
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
            try:
                h          = BASE_HEADERS.copy()
                resume_pos = 0
                mode       = 'wb'

                if temp_file.exists():
                    resume_pos = temp_file.stat().st_size
                    if resume_pos < real_total_size:
                        mode = 'ab'; h['Range'] = f"bytes={resume_pos}-"
                        _set_status("Resuming...")
                    elif resume_pos >= real_total_size:
                        temp_file.rename(file_path)
                        self.progress_data[thread_id].update(
                            {'percent': 100, 'status': DONE_STATUS})
                        return True

                session = _make_session(1)
                r = session.get(download_url, headers=h, stream=True,
                                timeout=Config.TIMEOUT, verify=False)
                if resume_pos > 0 and r.status_code == 200:
                    resume_pos = 0; mode = 'wb'; temp_file.unlink(missing_ok=True)
                if r.status_code not in (200, 206):
                    _clean_local(); _set_status(f"Err {r.status_code}")
                    session.close(); return False

                done = resume_pos; start = time.time()
                last_t = start; last_d = done

                with open(temp_file, mode) as f:
                    for chunk in r.iter_content(chunk_size=self.CHUNK_SIZE):
                        if self.cancel_event.is_set():
                            session.close(); _cancel_cleanup(); return False
                        if chunk:
                            f.write(chunk); done += len(chunk)
                            now = time.time()
                            if now - last_t >= 0.5:
                                speed   = (done - last_d) / (now - last_t)
                                percent = done / real_total_size * 100 if real_total_size else 0
                                eta     = (real_total_size - done) / speed if speed > 0 else 0
                                self.progress_data[thread_id].update(
                                    {'percent': percent, 'speed': speed,
                                     'status': "DL...", 'done_bytes': done, 'eta': eta})
                                last_t = now; last_d = done

                session.close()
                if temp_file.exists() and temp_file.stat().st_size >= real_total_size:
                    temp_file.rename(file_path)
                    self.progress_data[thread_id].update(
                        {'percent': 100, 'status': DONE_STATUS})
                    return True
                return False

            except Exception:
                _clean_local(); _set_status("Error"); return False