import re
import time
import threading
import asyncio
from rich.markup import escape as rich_escape
from rich.live import Live
from rich.text import Text
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt, Confirm
from rich import box
from config.settings import Config, console, Language
from core.download.dashboard import GOOD_STATUSES
from core.pikpak.account_pool import get_pool, reload_pool
from core.logger import logger
from ui.common import _run, _read_key_nonblocking


class DownloadMixin:
    def _collect_files(self, tree):
        res = []
        for item in tree:
            if item["type"] == "file":
                res.append(item)
            else:
                res.extend(self._collect_files(item["folders"] + item["files"]))
        return res

    def download_menu(self):
        Config.load_config()
        self.print_header()
        raw_url = Prompt.ask(
            f"\n  [bold green]🔗 {Language.get('input_link')}[/] [dim]{Language.get('input_link_hint')}[/]",
            default="",
            show_default=False,
        )
        # Chấp nhận cả dấu phẩy, khoảng trắng và xuống dòng làm dấu phân cách; bỏ link trùng
        url_list = list(dict.fromkeys(u for u in re.split(r"[,\s]+", raw_url) if u))
        if not url_list:
            return
        pwd = Prompt.ask(
            f"  [bold white]🔑 {Language.get('input_pwd')}[/]",
            password=True,
            default="",
            show_default=False,
        )
        console.print()
        all_link_data = []
        grand_total_files = 0
        grand_total_size = 0
        for url in url_list:
            console.print(f"  [bold cyan]{Language.get('dl_analyzing')}[/] {url}")
            try:
                data = _run(self.downloader.get_tree_and_prepare(url, pwd))
            except Exception:
                logger.exception("Phân tích link thất bại: %s", url)
                data = None
            if not data:
                console.print(f"  [bold red]{Language.get('dl_load_fail')}[/]")
                continue
            flat = self._collect_files(data["folders"] + data["files"])
            all_link_data.append({"tree": data, "files": flat})
            grand_total_files += len(flat)
            grand_total_size += sum(f["size"] for f in flat)
        if not all_link_data:
            console.print(f"\n  [bold red]{Language.get('no_files')}[/]")
            time.sleep(2)
            return
        console.print()
        summary = Table.grid(expand=True)
        summary.add_column()
        summary.add_column(justify="right")
        summary.add_row(
            f"[bold cyan]{Language.get('links_ok')}: {len(all_link_data)}[/]  |  {Language.get('total_files')}: [bold cyan]{grand_total_files}[/]",
            f"{Language.get('total_size')}: [bold cyan]{self.downloader.format_size(grand_total_size)}[/]",
        )
        console.print(
            Panel(
                summary,
                title=Language.get("link_info"),
                border_style="green",
                box=box.ROUNDED,
            )
        )
        console.print(f"  [1] {Language.get('dl_opt_1')}")
        console.print(f"  [2] {Language.get('dl_opt_2')}")
        console.print(f"  [0] {Language.get('dl_opt_0')}")
        mode = Prompt.ask(
            f"\n  {Language.get('prompt_choice')}", choices=["1", "2", "0"]
        )
        if mode == "0":
            return
        master_files = []
        for link_idx, ld in enumerate(all_link_data):
            for f in ld["files"]:
                master_files.append({**f, "_link_idx": link_idx})
        if mode == "2":
            self.clear()
            ftable = Table(title=Language.get("file_list"), box=None, padding=(0, 1))
            ftable.add_column("ID", width=4, style="cyan")
            ftable.add_column("Link", width=5, style="dim")
            ftable.add_column("File")
            ftable.add_column("Size", justify="right", style="green")
            # Hiện toàn bộ danh sách (trước đây chỉ hiện 100 file nhưng vẫn cho chọn ID > 100)
            for i, f in enumerate(master_files, 1):
                ftable.add_row(
                    str(i),
                    f"L{f['_link_idx'] + 1}",
                    rich_escape(f["path"] or f["name"]),
                    self.downloader.format_size(f["size"]),
                )
            console.print(ftable)
            raw = Prompt.ask(f"\n  [bold green]{Language.get('dl_pick')}[/]")
            idxs = set()
            for p in raw.replace(" ", "").split(","):
                if not p:
                    continue
                try:
                    if "-" in p:
                        a, b = map(int, p.split("-", 1))
                        idxs.update(range(min(a, b), max(a, b) + 1))
                    else:
                        idxs.add(int(p))
                except ValueError:
                    continue
            master_files = [
                master_files[i - 1] for i in sorted(idxs) if 1 <= i <= len(master_files)
            ]
        if not master_files:
            return
        download_items = []
        for f in master_files:
            tree_data = all_link_data[f["_link_idx"]]["tree"]
            f["_share_id"] = tree_data["share_id"]
            f["_pass_token"] = tree_data["pass_token"]
            download_items.append(f)
        _run(reload_pool())
        link_count = len({f["_link_idx"] for f in download_items})
        console.print(
            f"\n[bold yellow]{Language.get('dl_starting', n=len(download_items), m=link_count)}[/]"
        )
        cancelled = self.run_download_with_retry(download_items)
        if cancelled:
            console.print(f"\n[bold red]{Language.get('dl_cancelled')}[/]")
            time.sleep(2)

    def run_download_with_retry(self, files) -> bool:
        while True:
            self.clear()
            files.sort(key=lambda x: x["name"])
            total_size_bytes = sum(f["size"] for f in files)
            self.downloader.reset_progress()
            self.downloader.start_monitor(len(files), total_size_bytes)
            cancel_event = threading.Event()
            self.downloader.cancel_event = cancel_event
            user_cancelled = False
            with Live(
                self.downloader.generate_dashboard_table(),
                refresh_per_second=4,
                screen=True,
            ) as live:
                self.downloader.monitor_active = True

                def update_dashboard():
                    nonlocal user_cancelled
                    while self.downloader.monitor_active:
                        key = _read_key_nonblocking()
                        if key == "q" and not cancel_event.is_set():
                            cancel_event.set()
                            user_cancelled = True
                            for pdata in self.downloader.progress_data.values():
                                if pdata.get("status") not in (
                                    *GOOD_STATUSES,
                                    "Error",
                                    "Failed",
                                    "Cancelled",
                                    "Waiting",
                                ):
                                    pdata["status"] = "Cancelling..."
                        live.update(self.downloader.generate_dashboard_table())
                        time.sleep(0.25)

                dash_thread = threading.Thread(target=update_dashboard, daemon=True)
                dash_thread.start()

                async def download_files():
                    pool = get_pool()
                    apis = pool.all_apis() or [self.downloader.api]
                    tasks = []
                    semaphore = asyncio.Semaphore(Config.MAX_WORKERS)
                    for i, f in enumerate(files):
                        self.downloader.progress_data[i + 1] = {
                            "id": i + 1,
                            "name": f["name"],
                            "percent": 0,
                            "speed": 0,
                            "status": "Waiting",
                            "done_bytes": 0,
                            "total_bytes": int(f.get("size", 0)),
                            "eta": 0,
                        }

                    async def sem_download(f, i, api_for_file):
                        async with semaphore:
                            return await self.downloader.download_single_file(
                                f,
                                f["_share_id"],
                                f["_pass_token"],
                                i + 1,
                                api=api_for_file,
                            )

                    for i, f in enumerate(files):
                        api_for_file = apis[i % len(apis)]
                        tasks.append(
                            asyncio.create_task(sem_download(f, i, api_for_file))
                        )
                    try:
                        await asyncio.gather(*tasks, return_exceptions=True)
                        if self.downloader.bg_tasks:
                            await asyncio.gather(
                                *list(self.downloader.bg_tasks), return_exceptions=True
                            )
                    finally:
                        await self.downloader.close_session()

                _run(download_files())
                self.downloader.stop_monitor()
                dash_thread.join(timeout=1)
            self.clear()
            if user_cancelled:
                done_count = sum(
                    1
                    for i in range(len(files))
                    if self.downloader.progress_data.get(i + 1, {}).get("status")
                    in GOOD_STATUSES
                )
                console.print(
                    Panel(
                        f"[bold red]{Language.get('dl_cancel_by_user')}[/]\n\n"
                        f"[green]{Language.get('dl_cancel_done')}: {done_count}[/]\n"
                        f"[red]{Language.get('dl_cancel_left')}: {len(files) - done_count}[/]",
                        title=Language.get("dl_cancel_title"),
                        border_style="red",
                        box=box.ROUNDED,
                    )
                )
                Prompt.ask(f"\n  [dim]{Language.get('press_enter')}[/]", default="", show_default=False)
                return True
            done_count = 0
            failed_files = []
            for i, f in enumerate(files):
                status = self.downloader.progress_data.get(i + 1, {}).get(
                    "status", "Unknown"
                )
                if status in GOOD_STATUSES:
                    done_count += 1
                else:
                    failed_files.append(f)
            failed_count = len(failed_files)
            console.print(
                Panel(
                    f"[bold green]{Language.get('dl_complete')}[/]\n\n"
                    f"[green]{Language.get('dl_success')}: {done_count}[/]\n"
                    f"[red]{Language.get('dl_failed')}: {failed_count}[/]",
                    title=Language.get("dl_result"),
                    border_style="green",
                    box=box.ROUNDED,
                )
            )
            if failed_count > 0:
                if Confirm.ask(
                    f"[bold yellow]{Language.get('dl_retry_q', n=failed_count)}[/]"
                ):
                    files = failed_files
                    continue
                else:
                    break
            else:
                self._auto_continue(seconds=3)
                break
        return False

    def _auto_continue(self, seconds: int = 3):
        def line(n):
            return Text.from_markup(
                f"  [bold green]{Language.get('dl_auto_continue', n=n)}[/]"
            )

        # Live ghi đè cùng một dòng; console.print với "\r" bị rich in thành nhiều dòng
        with Live(line(seconds), console=console, auto_refresh=False) as live:
            for remaining in range(seconds, 0, -1):
                live.update(line(remaining), refresh=True)
                start = time.time()
                while time.time() - start < 1:
                    if _read_key_nonblocking() in ("\r", "\n", " "):
                        return
                    time.sleep(0.05)
