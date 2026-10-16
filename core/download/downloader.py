import threading
import time
import asyncio
import re
import aiofiles
import errno
from config.settings import Config, console, Language
from core.pikpak.api import PikPakAPI, TreeBuilder
from core.pikpak.account_pool import get_pool
from core.logger import logger
from core.download.paths import safe_name, safe_rel_dir
from core.download.dashboard import DashboardMixin, DONE_STATUS, SKIP_STATUS
from core.download.segmented import SegmentedDownloadMixin, DiskFullError
from core.download.cloud_restore import CloudRestoreMixin, STRIPE_MIN_SIZE

_AUTO_CONN = [
    (2 * 1024 * 1024 * 1024, 32),  # > 2GB: 32 Connections
    (1024 * 1024 * 1024, 24),  # > 1GB: 24 Connections
    (500 * 1024 * 1024, 16),  # > 500MB: 16 Connections
    (50 * 1024 * 1024, 8),  # > 50MB: 8 Connections
]


class Downloader(DashboardMixin, SegmentedDownloadMixin, CloudRestoreMixin):
    def __init__(self):
        self.api = PikPakAPI()
        self.tree_builder = TreeBuilder(self.api)
        self.reset_progress()

    def reset_progress(self):
        self.progress_data = {}
        self.monitor_active = False
        self.total_files_count = 0
        self.total_batch_size = 0
        self.cancel_event = threading.Event()
        self.bg_tasks = set()
        self.full_accounts = set()
        self.active_cloud_ids = set()
        self._gc_done = False
        self._gc_lock = asyncio.Lock()

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
            if size < 1024:
                return f"{size:.2f} {unit}"
            size /= 1024
        return f"{size:.2f} PB"

    @staticmethod
    def format_time(seconds):
        if seconds < 0 or seconds > 86400 * 3:
            return "--:--"
        m, s = divmod(int(seconds), 60)
        h, m = divmod(m, 60)
        return f"{h:02d}:{m:02d}:{s:02d}" if h > 0 else f"{m:02d}:{s:02d}"

    def _natural_key(self, item):
        return [
            int(s) if s.isdigit() else s.lower()
            for s in re.split(r"(\d+)", item["name"])
        ]

    def _recursive_sort(self, node):
        if "files" in node:
            node["files"].sort(key=self._natural_key)
        if "folders" in node:
            node["folders"].sort(key=self._natural_key)
            for f in node["folders"]:
                self._recursive_sort(f)

    def _resolve_conn(self, file_size: int) -> int:
        cfg = Config.CONCURRENT_THREADS
        for threshold, auto in _AUTO_CONN:
            if file_size >= threshold:
                return max(cfg, auto)
        return max(cfg, 2)

    async def get_tree_and_prepare(self, url, password):
        m = re.search(r"/s/([A-Za-z0-9_-]+)", url)
        if not m:
            return None
        share_id = m.group(1)
        if not await self.api.ensure_token():
            return None
        files, ptoken = await self.api.get_share_info(share_id, password)
        if not files:
            return None
        tree = await self.tree_builder.build_tree(files, "", share_id, ptoken)
        self._recursive_sort(tree)
        return {
            "folders": tree["folders"],
            "files": tree["files"],
            "share_id": share_id,
            "pass_token": ptoken,
        }

    def start_monitor(self, total_count, total_size_bytes):
        self.monitor_active = True
        self.total_files_count = total_count
        self.total_batch_size = total_size_bytes

    def stop_monitor(self):
        self.monitor_active = False

    # -------------------------------------------------------------------------
    # MAIN DOWNLOADER LOGIC CHO SINGLE FILE
    # -------------------------------------------------------------------------
    async def download_single_file(
        self, file_data, share_id, pass_token, thread_id, api=None
    ):
        if self.cancel_event.is_set():
            if self.progress_data.get(thread_id, {}).get("status") != "Waiting":
                self.progress_data[thread_id].update({"status": "Cancelled"})
            return False

        name = file_data["name"]
        real_total_size = int(file_data["size"])
        HEAVY_EXTS = {
            ".mp4",
            ".mkv",
            ".avi",
            ".mov",
            ".wmv",
            ".flv",
            ".webm",
            ".ts",
            ".iso",
            ".m4v",
            ".rar",
            ".zip",
            ".7z",
        }
        use_premium = (
            any(name.lower().endswith(e) for e in HEAVY_EXTS)
            or Config.FORCE_PREMIUM_MODE
        )

        logger.info(
            f"Bắt đầu tiến trình tải file: '{name}' | Dung lượng: {self.format_size(real_total_size)} | Tải Premium: {use_premium}"
        )

        self.progress_data[thread_id] = {
            "id": thread_id,
            "name": name,
            "percent": 0,
            "speed": 0,
            "status": "Init...",
            "done_bytes": 0,
            "total_bytes": real_total_size,
            "eta": 0,
        }

        pool = get_pool()
        if api is None:
            api = pool.acquire() or self.api

        save_dir = Config.get_download_dir() / safe_rel_dir(file_data["path"])
        try:
            save_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.error(f"Không tạo được thư mục {save_dir}: {e}")
            self.progress_data[thread_id]["status"] = "Disk Error"
            return False
        # `name` giữ nguyên để so khớp file trên cloud, còn tên file local thì làm sạch
        file_path = save_dir / safe_name(name)
        temp_file = file_path.parent / f".{file_path.name}.tmp"
        ckpt_file = temp_file.with_name(temp_file.name + ".ckpt")

        BASE_HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
            "Referer": "https://mypikpak.com/",
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Connection": "keep-alive",
        }

        def _clean_local():
            for p in (file_path, temp_file, ckpt_file):
                try:
                    if p.exists():
                        p.unlink()
                except OSError:
                    pass

        t_start = time.time()
        _FAIL_STATUSES = ("Failed", "Restore Fail", "No Link", "No URL", "Auth Fail",
                          "Err ", "Error", "Disk Full", "Disk Error")

        def _set_status(s):
            self.progress_data[thread_id]["status"] = s
            if s.startswith(_FAIL_STATUSES):
                logger.error(f"Tải thất bại '{name}': {s}")
            elif s == "Cancelled":
                logger.info(f"Đã hủy '{name}'")

        def _cancel_cleanup():
            _clean_local()
            _set_status("Cancelled")

        def _mark_done():
            took = max(time.time() - t_start, 0.001)
            logger.info(
                f"Hoàn tất '{name}' ({self.format_size(real_total_size)}) trong {took:.0f}s"
                f" | TB {self.format_size(real_total_size / took)}/s"
            )
            self.progress_data[thread_id].update(
                {
                    "percent": 100,
                    "speed": 0,
                    "status": DONE_STATUS,
                    "done_bytes": real_total_size,
                    "eta": 0,
                }
            )

        if use_premium:
            if file_path.exists():
                if file_path.stat().st_size == real_total_size:
                    logger.info(f"Bỏ qua '{name}': đã có file cùng dung lượng")
                    self.progress_data[thread_id].update(
                        {
                            "percent": 100,
                            "speed": 0,
                            "status": SKIP_STATUS,
                            "done_bytes": real_total_size,
                            "eta": 0,
                        }
                    )
                    return True
                file_path.unlink(missing_ok=True)

            my_file_id = None
            active_api = None
            owned = True
            dl_success = False
            was_cancelled = False
            replicas = []
            # Chỉ xóa sau khi tải những file do tool tạo ra (không xóa file của người dùng)
            cleanup = []

            # Xoay thứ tự để file này bắt đầu từ account được gán,
            # tránh mọi file cùng dồn vào account #1
            apis = pool.all_apis()
            if api in apis:
                k = apis.index(api)
                apis = apis[k:] + apis[:k]
            else:
                apis = [api] + apis

            if len(apis) > 1:
                _set_status(f"Init [{len(apis)} acc]...")

            try:
                if self.cancel_event.is_set():
                    was_cancelled = True
                    _cancel_cleanup()
                    return False

                _set_status(Language.get("status_restore"))

                my_file_id, active_api, owned = await self._restore_with_failover(
                    apis, name, real_total_size, share_id, file_data["id"], pass_token
                )

                if not my_file_id:
                    if self.cancel_event.is_set():
                        was_cancelled = True
                        _cancel_cleanup()
                        return False
                    _set_status("Restore Fail")
                    return False

                self.active_cloud_ids.add(my_file_id)
                replicas = [(active_api, my_file_id)]
                if owned:
                    cleanup.append((active_api, my_file_id))

                # File lớn: nhân bản sang các account rảnh để cộng dồn băng thông
                if real_total_size >= STRIPE_MIN_SIZE and len(apis) > 1:
                    extra = await self._restore_replicas(
                        apis, active_api, share_id, file_data["id"], pass_token
                    )
                    for a, fid in extra:
                        self.active_cloud_ids.add(fid)
                    replicas += extra
                    cleanup += extra

                if self.cancel_event.is_set():
                    was_cancelled = True
                    _cancel_cleanup()
                    return False

                _set_status(Language.get("status_getlink"))
                url_replicas = []
                stripe_urls = []
                for a, fid in replicas:
                    url = await a.get_user_file_url(
                        fid, attempts=8 if a is active_api else 3
                    )
                    if url:
                        url_replicas.append((a, fid))
                        stripe_urls.append(url)

                if not stripe_urls:
                    _set_status("No Link")
                    return False

                if self.cancel_event.is_set():
                    was_cancelled = True
                    _cancel_cleanup()
                    return False

                num_conn = self._resolve_conn(real_total_size)

                _set_status(f"DL x{num_conn}conn")
                logger.info(
                    f"[{name}] Tải bằng {len(stripe_urls)} account × {num_conn} luồng."
                )

                async def _get_fresh_urls():
                    out = []
                    for a, fid in url_replicas:
                        try:
                            await a.ensure_token()
                            out.append(await a.get_user_file_url(fid, attempts=2))
                        except Exception:
                            out.append(None)
                    return out

                try:
                    dl_success = await self._multi_conn_download(
                        stripe_urls,
                        BASE_HEADERS,
                        temp_file,
                        real_total_size,
                        thread_id,
                        num_conn,
                        _get_fresh_urls,
                    )
                except DiskFullError:
                    console.print(f"\n[bold red]{Language.get('err_disk_full')}[/]")
                    _set_status("Disk Full")
                    self.cancel_event.set()
                    was_cancelled = True
                    dl_success = False

                if self.cancel_event.is_set():
                    was_cancelled = True
                    _cancel_cleanup()
                    return False

                if dl_success:
                    if temp_file.exists():
                        temp_file.replace(file_path)
                    if ckpt_file.exists():
                        ckpt_file.unlink(missing_ok=True)
                    _mark_done()
                else:
                    _set_status("Failed")
                    _clean_local()

            finally:
                for a, fid in replicas:
                    self.active_cloud_ids.discard(fid)
                for a, fid in cleanup:
                    task = asyncio.create_task(self._bg_delete(a, fid))
                    self.bg_tasks.add(task)
                    task.add_done_callback(self.bg_tasks.discard)
                if was_cancelled:
                    _set_status("Cancelled")

            return dl_success

        else:
            # FALLBACK DOWNLOAD (Khong dung acc premium)
            if self.cancel_event.is_set():
                _cancel_cleanup()
                return False

            if not await api.ensure_token():
                _set_status("Auth Fail")
                return False

            download_url = await api.get_download_url(
                share_id, file_data["id"], pass_token
            )
            if not download_url:
                _set_status("No URL")
                return False

            if file_path.exists() and file_path.stat().st_size == real_total_size:
                logger.info(f"Bỏ qua '{name}': đã có file cùng dung lượng")
                self.progress_data[thread_id].update(
                    {"percent": 100, "status": SKIP_STATUS}
                )
                return True

            # Không hỏi trước bằng HEAD: server không hỗ trợ Range sẽ bị phát hiện
            # ngay ở phản hồi đầu tiên và chuyển sang tải 1 luồng
            if real_total_size > 1 * 1024 * 1024:
                num_conn = self._resolve_conn(real_total_size)
                _set_status(f"DL x{num_conn}conn")

                async def _fresh_direct():
                    await api.ensure_token()
                    u = await api.get_download_url(
                        share_id, file_data["id"], pass_token
                    )
                    return [u] if u else [download_url]

                try:
                    ok = await self._multi_conn_download(
                        [download_url],
                        BASE_HEADERS,
                        temp_file,
                        real_total_size,
                        thread_id,
                        num_conn,
                        _fresh_direct,
                    )
                except DiskFullError:
                    console.print(f"\n[bold red]{Language.get('err_disk_full')}[/]")
                    _set_status("Disk Full")
                    self.cancel_event.set()
                    ok = False

                if self.cancel_event.is_set():
                    _cancel_cleanup()
                    return False
                if ok:
                    if temp_file.exists():
                        temp_file.replace(file_path)
                    if ckpt_file.exists():
                        ckpt_file.unlink(missing_ok=True)
                    _mark_done()
                    return True
                _clean_local()
                logger.warning(f"Tải đa luồng '{name}' thất bại, chuyển sang tải 1 luồng")
                _set_status("Fallback...")

            try:
                h = BASE_HEADERS.copy()
                resume_pos = 0
                mode = "wb"

                if temp_file.exists():
                    if ckpt_file.exists():
                        _clean_local()
                        _set_status("Fallback...")
                        resume_pos = 0
                    else:
                        resume_pos = temp_file.stat().st_size
                        if resume_pos < real_total_size:
                            mode = "ab"
                            h["Range"] = f"bytes={resume_pos}-"
                            _set_status("Resuming...")
                        elif resume_pos >= real_total_size:
                            temp_file.replace(file_path)
                            _mark_done()
                            return True

                session = self._dl_session()
                async with session.get(download_url, headers=h) as r:
                    if resume_pos > 0 and r.status == 200:
                        resume_pos = 0
                        mode = "wb"
                        temp_file.unlink(missing_ok=True)
                    if r.status not in (200, 206):
                        _clean_local()
                        _set_status(f"Err {r.status}")
                        return False

                    done = resume_pos
                    start = time.time()
                    last_t = start
                    last_d = done
                    current_speed = 0.0

                    async with aiofiles.open(temp_file, mode) as f:
                        async for chunk in r.content.iter_any():
                            if self.cancel_event.is_set():
                                _cancel_cleanup()
                                return False
                            if chunk:
                                try:
                                    await f.write(chunk)
                                except OSError as e:
                                    if e.errno == errno.ENOSPC:
                                        console.print(f"\n[bold red]{Language.get('err_disk_full')}[/]")
                                        _set_status("Disk Full")
                                        self.cancel_event.set()
                                        return False
                                    raise
                                done += len(chunk)
                                now = time.time()
                                if now - last_t >= 0.5:
                                    inst_speed = max(0, done - last_d) / max(
                                        now - last_t, 0.001
                                    )
                                    if current_speed == 0:
                                        current_speed = inst_speed
                                    else:
                                        current_speed = (current_speed * 0.7) + (
                                            inst_speed * 0.3
                                        )

                                    percent = (
                                        min((done / real_total_size) * 100, 100)
                                        if real_total_size
                                        else 0
                                    )
                                    eta = (
                                        (real_total_size - done) / current_speed
                                        if current_speed > 0
                                        else 0
                                    )
                                    self.progress_data[thread_id].update(
                                        {
                                            "percent": percent,
                                            "speed": current_speed,
                                            "status": "DL...",
                                            "done_bytes": done,
                                            "eta": eta,
                                        }
                                    )
                                    last_t = now
                                    last_d = done

                if temp_file.exists() and temp_file.stat().st_size >= real_total_size:
                    temp_file.replace(file_path)
                    _mark_done()
                    return True
                # Giữ file tạm để lần sau tải tiếp từ chỗ dừng
                _set_status("Failed")
                return False

            except Exception:
                logger.exception(f"Lỗi khi tải 1 luồng '{name}'")
                _clean_local()
                _set_status("Error")
                return False
