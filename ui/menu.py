import sys
import time
import threading
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt, Confirm
from rich.align import Align
from rich import box
from config.settings import Config, console, Language, APP_VERSION
from core.utils import UpdateManager, CacheManager
from core.api import PikPakLogin
from core.downloader import Downloader, GOOD_STATUSES
from core.account_pool import get_pool, reload_pool

ASCII_LOGO = """
[bold cyan]
██████╗ ██╗██╗  ██╗██████╗  █████╗ ██╗  ██╗
██╔══██╗██║██║ ██╔╝██╔══██╗██╔══██╗██║ ██╔╝
██████╔╝██║█████╔╝ ██████╔╝███████║█████╔╝ 
██╔═══╝ ██║██╔═██╗ ██╔═══╝ ██╔══██║██╔═██╗ 
██║     ██║██║  ██╗██║     ██║  ██║██║  ██╗
╚═╝     ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝
[/]"""

def _read_key_nonblocking():
    try:
        import os
        if os.name == "nt":
            import msvcrt
            if msvcrt.kbhit(): return msvcrt.getwch().lower()
        else:
            import select, tty, termios
            fd  = sys.stdin.fileno()
            old = termios.tcgetattr(fd)
            try:
                tty.setraw(fd)
                r, _, _ = select.select([sys.stdin], [], [], 0)
                if r: return sys.stdin.read(1).lower()
            finally:
                termios.tcsetattr(fd, termios.TCSADRAIN, old)
    except Exception: pass
    return None

class Menu:
    def __init__(self):
        self.downloader = Downloader()

    def clear(self):
        import os
        os.system("cls" if os.name == "nt" else "clear")

    def print_header(self):
        self.clear()
        console.print(Align.center(ASCII_LOGO))
        console.print(Align.center(f"[bold white]VERSION {APP_VERSION}[/] | [dim]{Language.get('menu_dev')}[/]\n"))

    def main_menu(self):
        UpdateManager.check_for_updates()
        Config.load_config()
        n = asyncio.run(reload_pool())
        if n > 1:
            console.print(f"  [bold green]✓ Account pool: {n} accounts active (~{n*11} MB/s max)[/]")
            time.sleep(1)
        while True:
            Config.load_config()
            self.print_header()
            table = Table(show_header=False, box=None, padding=(0, 2), expand=True)
            table.add_column("Key", justify="right", style="bold cyan", width=10)
            table.add_column("Action", style="bold white")
            table.add_row("[1]", Language.get("menu_1"))
            table.add_row("[2]", Language.get("menu_2"))
            table.add_row("[3]", Language.get("menu_3"))
            table.add_row("[4]", Language.get("menu_4"))
            table.add_row("[5]", Language.get("menu_5"))
            table.add_row("[6]", Language.get("menu_6"))
            table.add_row("", "")
            table.add_row("[0]", Language.get("menu_0"))
            console.print(table)
            pool = get_pool()
            ps   = pool.size()
            if ps > 1: console.print(f"\n  [bold green]⚡ Pool: {ps} accounts (~{ps*11} MB/s max)[/]")
            else: console.print(f"\n  [dim]Tip: Add extra accounts ([6]) to multiply speed[/]")
            console.print()
            c = Prompt.ask(f"[bold green]👉 {Language.get('prompt_choice')}[/]", choices=["1", "2", "3", "4", "5", "6", "0"])
            if c == "1":   self.login_with_password()
            elif c == "2": self.download_menu()
            elif c == "3": self.settings_menu()
            elif c == "4": self.cache_menu()
            elif c == "5": self.view_config()
            elif c == "6": self.extra_accounts_menu()
            elif c == "0": sys.exit()

    def login_with_password(self):
        self.print_header()
        console.print(f"[bold cyan]  {Language.get('login_header')}[/]\n")
        username = Prompt.ask(f"  {Language.get('login_user')}")
        password = Prompt.ask(f"  {Language.get('login_pass')}", password=True)
        Config.load_config()
        api = PikPakLogin(username, password, Config.DEVICE_ID)
        try:
            with console.status(f"[bold cyan]  {Language.get('login_wait')}", spinner="dots"):
                result = asyncio.run(api.login())
        except: result = None
        if not result:
            console.print(f"\n  [bold red]{Language.get('login_fail')}[/]")
            time.sleep(2); return
        Config.REFRESH_TOKEN = result["refresh_token"]
        Config.DEVICE_ID     = result["device_id"]
        Config.CAPTCHA_TOKEN = ""
        Config.save_config()
        console.print(f"\n  [bold green]{Language.get('login_success')}[/]")
        time.sleep(2)

    def settings_menu(self):
        while True:
            Config.load_config()
            self.print_header()
            console.print(Align.center(f"[bold yellow]{Language.get('set_header')}[/]"))
            console.print()
            table = Table(show_header=False, box=None, padding=(0, 2))
            table.add_column("Key",    style="bold cyan", justify="right", width=6)
            table.add_column("Chức năng",  style="bold white",  min_width=28)
            table.add_column("Trạng thái", justify="left",      min_width=12)
            table.add_row("[1]", "Proxy",                self._badge(Config.USE_PROXY))
            table.add_row("[2]", "Số luồng/file",        f"[cyan]{Config.CONCURRENT_THREADS} conn[/]")
            table.add_row("[3]", "Cài đặt nâng cao",     "")
            table.add_row("[4]", "Ngôn ngữ",              f"[dim]{Config.LANGUAGE.upper()}[/]")
            table.add_row("[5]", "Premium Transfer Mode", self._badge(Config.FORCE_PREMIUM_MODE))
            table.add_row("[6]", "Cache",                 self._badge(Config.USE_CACHE))
            table.add_row("",    "",                      "")
            table.add_row("[0]", Language.get("menu_0"),  "")
            console.print(table); console.print()
            c = Prompt.ask(f"[bold green]👉 {Language.get('prompt_choice')}[/]", choices=["1", "2", "3", "4", "5", "6", "0"])
            if c == "1":   self.proxy_setup()
            elif c == "2": self.threads_setup()
            elif c == "3": self.advanced_setup()
            elif c == "4": self.change_language()
            elif c == "5": self.premium_mode_setup()
            elif c == "6": self._toggle_cache_quick()
            elif c == "0": break

    def change_language(self):
        self.print_header()
        console.print(f"\n  [bold cyan]{Language.get('lang_select')}[/]")
        table = Table(show_header=False, box=None, padding=(0, 2))
        table.add_row("[1]", "English"); table.add_row("[2]", "Tiếng Việt")
        console.print(table)
        c = Prompt.ask("\n  Option", choices=["1", "2"])
        if c == "1":   Config.LANGUAGE = "en"; console.print("\n  [bold green]✓ Language set to English[/]")
        elif c == "2": Config.LANGUAGE = "vi"; console.print("\n  [bold green]✓ Đã chuyển sang Tiếng Việt[/]")
        Config.save_config(); time.sleep(1)

    def _toggle_cache_quick(self):
        Config.USE_CACHE = self._ask_toggle("Cache", Config.USE_CACHE)
        Config.save_config()
        console.print(f"  [bold green]✓ Đã lưu![/]  Cache: {self._badge(Config.USE_CACHE)}")
        time.sleep(1)

    @staticmethod
    def _badge(enabled: bool) -> str:
        return "[bold green][ ON  ][/]" if enabled else "[bold red][ OFF ][/]"

    @staticmethod
    def _ask_toggle(feature_name: str, current: bool) -> bool:
        badge  = Menu._badge(current)
        console.print(f"\n  {feature_name}: {badge}")
        if current:
            if Confirm.ask("  ➜ Tắt đi?", default=False): return False
        else:
            if Confirm.ask("  ➜ Bật lên?", default=False): return True
        return current

    def proxy_setup(self):
        while True:
            Config.load_config()
            self.print_header()
            console.print("\n  [bold cyan]━━  PROXY  ━━[/]")
            grid = Table.grid(padding=(0, 3))
            grid.add_column(style="dim", justify="right")
            grid.add_column()
            grid.add_row("Trạng thái:", self._badge(Config.USE_PROXY))
            if Config.USE_PROXY and Config.PROXY_HOST:
                auth_part = f"{Config.PROXY_USERNAME}@" if Config.PROXY_USERNAME else ""
                grid.add_row("Địa chỉ:  ", f"[white]{Config.PROXY_TYPE}://{auth_part}{Config.PROXY_HOST}:{Config.PROXY_PORT}[/]")
            console.print(grid); console.print()
            opts = Table(show_header=False, box=None, padding=(0,2))
            opts.add_column("Key", style="bold cyan", justify="right", width=6)
            opts.add_column()
            opts.add_row("[1]", "Bật / Tắt Proxy")
            opts.add_row("[2]", "Chỉnh host / port / auth")
            opts.add_row("[3]", "Test proxy ngay")
            opts.add_row("", "")
            opts.add_row("[0]", "Quay lại")
            console.print(opts); console.print()
            c = Prompt.ask("  Chọn", choices=["1","2","3","0"])
            if c == "0": break
            elif c == "1":
                Config.USE_PROXY = self._ask_toggle("Proxy", Config.USE_PROXY)
                Config.save_config()
                console.print(f"  [bold green]✓ Đã lưu![/]  Proxy: {self._badge(Config.USE_PROXY)}")
                time.sleep(1)
            elif c == "2":
                console.print("\n  [dim]── Để trống Username nếu proxy không cần auth ──[/]\n")
                console.print("  [dim]Loại proxy:[/]")
                console.print("  [dim]  http   — phổ biến nhất, nhưng nhiều proxy free KHÔNG hỗ trợ HTTPS[/]")
                console.print("  [dim]  socks5 — hỗ trợ cả HTTP lẫn HTTPS, ổn định hơn[/]")
                console.print("  [dim]  https  — proxy server dùng SSL[/]")
                Config.PROXY_TYPE = Prompt.ask("  Loại", choices=["http","https","socks5"], default=Config.PROXY_TYPE or "http")
                if Config.PROXY_TYPE == "http":
                    console.print("  [bold yellow]⚠ HTTP proxy có thể không tải được từ PikPak (HTTPS). Nếu lỗi hãy thử socks5.[/]")
                Config.PROXY_HOST = Prompt.ask("  Host / IP",  default=Config.PROXY_HOST)
                port_raw = Prompt.ask("  Port", default=Config.PROXY_PORT)
                try:
                    p = int(port_raw.strip())
                    if not (1 <= p <= 65535): raise ValueError
                    Config.PROXY_PORT = str(p)
                except ValueError:
                    console.print("  [bold red]✖ Port không hợp lệ (1-65535)[/]")
                    time.sleep(1.5); continue
                Config.PROXY_USERNAME = Prompt.ask("  Username (Enter nếu không có)", default=Config.PROXY_USERNAME)
                if Config.PROXY_USERNAME.strip(): Config.PROXY_PASSWORD = Prompt.ask("  Password", default=Config.PROXY_PASSWORD, password=True)
                else: Config.PROXY_USERNAME = ""; Config.PROXY_PASSWORD = ""
                Config.save_config()
                console.print("  [bold green]✓ Đã lưu![/]")
                time.sleep(1)
            elif c == "3":
                with console.status("  Đang test HTTP → HTTPS...", spinner="dots"):
                    ok, msg = Config.test_proxy()
                console.print()
                if ok: console.print(f"  [bold green]{msg}[/]")
                else:
                    console.print(f"  [bold red]✖ {msg}[/]\n")
                    console.print("  [bold yellow]Hướng xử lý:[/]")
                    console.print("  [dim]1. Đổi loại proxy sang [bold]socks5[/bold] (hỗ trợ HTTPS tốt nhất)[/]")
                    console.print("  [dim]2. Kiểm tra proxy còn sống: proxynova.com/proxy-checker[/]")
                    console.print("  [dim]3. Proxy HTTP free thường không tunnel được HTTPS[/]")
                Prompt.ask("\n  Enter để tiếp tục...")

    def threads_setup(self):
        self.print_header()
        console.print("\n  [bold cyan]━━  SỐ LUỒNG TẢI  ━━[/]\n")
        grid = Table.grid(padding=(0, 3))
        grid.add_column(style="dim", justify="right")
        grid.add_column()
        grid.add_row("Luồng/file hiện tại:", f"[cyan]{Config.CONCURRENT_THREADS}[/] connections")
        grid.add_row("Tốc độ ước tính:    ", f"[cyan]~{Config.CONCURRENT_THREADS * 11} MB/s[/] tối đa")
        grid.add_row("Khuyến nghị:        ", "[dim]8–16 cho tốc độ tốt nhất[/]")
        console.print(grid); console.print()
        try:
            v = Prompt.ask("  Số luồng/file", default=str(Config.CONCURRENT_THREADS))
            Config.CONCURRENT_THREADS = max(1, int(v))
        except ValueError: pass
        Config.save_config()
        console.print(f"\n  [bold green]✓ Đã lưu![/]  Luồng: [cyan]{Config.CONCURRENT_THREADS}[/] conn  (~{Config.CONCURRENT_THREADS * 11} MB/s max)")
        time.sleep(1.5)

    def premium_mode_setup(self):
        self.print_header()
        console.print("\n  [bold cyan]━━  PREMIUM TRANSFER MODE  ━━[/]")
        grid = Table.grid(padding=(0, 3))
        grid.add_column(style="dim", justify="right")
        grid.add_column()
        grid.add_row("Trạng thái:", self._badge(Config.FORCE_PREMIUM_MODE))
        grid.add_row("Khi BẬT:  ", "[dim]Restore TẤT CẢ file lên cloud trước khi tải[/]")
        grid.add_row("Khi TẮT:  ", "[dim]Chỉ restore video/zip/rar/iso (mặc định)[/]")
        console.print(grid)
        Config.FORCE_PREMIUM_MODE = self._ask_toggle("Premium Mode", Config.FORCE_PREMIUM_MODE)
        Config.save_config()
        console.print(f"\n  [bold green]✓ Đã lưu![/]  Premium Mode: {self._badge(Config.FORCE_PREMIUM_MODE)}")
        time.sleep(1.5)

    def advanced_setup(self):
        self.print_header()
        console.print("\n  [bold cyan]━━  CÀI ĐẶT NÂNG CAO  ━━[/]")
        grid = Table.grid(padding=(0, 3))
        grid.add_column(style="dim", justify="right")
        grid.add_column()
        grid.add_row("File đồng thời:", f"[cyan]{Config.MAX_WORKERS}[/]")
        grid.add_row("Thư mục tải:  ", f"[dim]{Config.get_download_dir()}[/]")
        grid.add_row("Timeout:      ", f"[cyan]{Config.TIMEOUT}[/]s")
        grid.add_row("Cache:        ", self._badge(Config.USE_CACHE))
        console.print(grid); console.print()
        try:
            v = Prompt.ask("  Số file tải đồng thời", default=str(Config.MAX_WORKERS))
            Config.MAX_WORKERS = max(1, int(v))
        except ValueError: pass
        Config.DOWNLOAD_PATH_STR = Prompt.ask("  Thư mục lưu", default=Config.DOWNLOAD_PATH_STR)
        try:
            v = Prompt.ask("  Timeout (giây)", default=str(Config.TIMEOUT))
            Config.TIMEOUT = max(5, int(v))
        except ValueError: pass
        Config.USE_CACHE = self._ask_toggle("Cache", Config.USE_CACHE)
        Config.save_config()
        Config.setup_dirs()
        console.print("\n  [bold green]✓ Đã lưu toàn bộ![/]")
        time.sleep(1.5)

    def cache_menu(self):
        self.print_header()
        size, count = CacheManager.get_cache_size()
        table = Table(title=Language.get("cache_info"), box=None, padding=(0, 2))
        table.add_column(Language.get("prompt_choice"), style="cyan")
        table.add_column("", style="bold white")
        table.add_row(Language.get("cache_files"), str(count))
        table.add_row(Language.get("cache_size"), f"{size/(1024*1024):.2f} MB")
        console.print(table)
        if Confirm.ask(f"\n  [bold red]{Language.get('cache_clear')}[/]"):
            CacheManager.clear_all()
            console.print(f"  [bold green]{Language.get('cache_cleared')}[/]"); time.sleep(1)

    def view_config(self):
        Config.load_config(); self.print_header()
        grid = Table.grid(expand=True, padding=(0, 2))
        grid.add_column(style="cyan", justify="right"); grid.add_column(style="white")
        grid.add_row("Download Path:", str(Config.get_download_dir()))
        grid.add_row("Max Workers:",   str(Config.MAX_WORKERS))
        grid.add_row("Timeout:",       f"{Config.TIMEOUT}s")
        grid.add_row("Language:",      Config.LANGUAGE)
        grid.add_row("Proxy:",         f"{Config.PROXY_HOST}:{Config.PROXY_PORT}" if Config.USE_PROXY else "Off")
        grid.add_row("Prem Mode:",     "ON" if Config.FORCE_PREMIUM_MODE else "Off")
        grid.add_row("Threads/File:",  str(Config.CONCURRENT_THREADS))
        console.print(Panel(grid, title=Language.get("menu_5"), border_style="blue", box=box.ROUNDED))
        Prompt.ask("\n  [dim]Enter...[/]")

    def extra_accounts_menu(self):
        while True:
            Config.load_config()
            self.print_header()
            console.print(Align.center(f"[bold yellow]{Language.get('acc_header')}[/]"))
            console.print(f"\n  [dim]{Language.get('acc_info')}[/]\n")
            accounts = Config.EXTRA_ACCOUNTS
            if accounts:
                tbl = Table(box=None, padding=(0, 2))
                tbl.add_column("#",    style="bold cyan", width=4)
                tbl.add_column("Device ID (partial)", style="dim")
                tbl.add_column("Token (partial)", style="dim")
                for i, acc in enumerate(accounts, start=1):
                    did = acc.get("device_id", "")[:12] + "..."
                    tok = acc.get("refresh_token", "")[:16] + "..."
                    tbl.add_row(str(i), did, tok)
                console.print(tbl)
            else: console.print(f"  [dim]{Language.get('acc_none')}[/]")
            pool = get_pool()
            console.print(f"\n  [bold green]{Language.get('acc_pool_size')}: {pool.size()}[/]\n")
            table = Table(show_header=False, box=None, padding=(0, 2))
            table.add_column("Key", style="bold cyan", justify="right", width=6)
            table.add_column("Desc")
            table.add_row("[1]", Language.get("acc_add"))
            table.add_row("[2]", Language.get("acc_remove"))
            table.add_row("[3]", Language.get("acc_test"))
            table.add_row("", "")
            table.add_row("[0]", Language.get("menu_0"))
            console.print(table); console.print()
            c = Prompt.ask(f"[bold green]👉 {Language.get('prompt_choice')}[/]", choices=["1", "2", "3", "0"])
            if c == "0": break
            elif c == "1": self._add_extra_account()
            elif c == "2": self._remove_extra_account()
            elif c == "3": self._test_accounts()

    def _add_extra_account(self):
        self.print_header()
        console.print(f"[bold cyan]  {Language.get('acc_add')}[/]\n")
        username = Prompt.ask(f"  {Language.get('login_user')}")
        password = Prompt.ask(f"  {Language.get('login_pass')}", password=True)
        import uuid as _uuid
        device_id = _uuid.uuid4().hex
        api = PikPakLogin(username, password, device_id)
        try:
            with console.status(f"[bold cyan]  {Language.get('login_wait')}", spinner="dots"):
                result = asyncio.run(api.login())
        except: result = None
        if not result:
            console.print(f"\n  [bold red]{Language.get('login_fail')}[/]")
            time.sleep(2); return
        Config.EXTRA_ACCOUNTS.append({"refresh_token": result["refresh_token"], "device_id": result["device_id"]})
        Config.save_config()
        n = asyncio.run(reload_pool())
        console.print(f"\n  [bold green]{Language.get('acc_added')}[/]")
        console.print(f"  [green]Pool now: {n} accounts (~{n*11} MB/s max)[/]")
        time.sleep(2)

    def _remove_extra_account(self):
        self.print_header()
        accounts = Config.EXTRA_ACCOUNTS
        if not accounts:
            console.print(f"  [dim]{Language.get('acc_none')}[/]")
            time.sleep(2); return
        idx_str = Prompt.ask(f"  Số thứ tự cần xóa (1-{len(accounts)})")
        try:
            idx = int(idx_str) - 1
            if not (0 <= idx < len(accounts)): raise ValueError
        except ValueError:
            console.print(f"  [bold red]{Language.get('acc_invalid')}[/]")
            time.sleep(2); return
        Config.EXTRA_ACCOUNTS.pop(idx)
        Config.save_config()
        asyncio.run(reload_pool())
        console.print(f"  [bold green]{Language.get('acc_removed')}[/]")
        time.sleep(1)

    def _test_accounts(self):
        self.print_header()
        console.print(f"[bold cyan]  {Language.get('acc_test')}[/]\n")
        n = asyncio.run(reload_pool(verbose=True))
        console.print(f"\n  [bold green]Active: {n} / {1 + len(Config.EXTRA_ACCOUNTS)} accounts[/]")
        console.print(f"  [bold green]Estimated max speed: ~{n * 11} MB/s[/]")
        Prompt.ask("\n  [dim]Enter to continue...[/]")

    def _collect_files(self, tree):
        res = []
        for item in tree:
            if item["type"] == "file": res.append(item)
            else: res.extend(self._collect_files(item["folders"] + item["files"]))
        return res

    def download_menu(self):
        Config.load_config()
        self.print_header()
        raw_url = Prompt.ask(f"\n  [bold green]🔗 {Language.get('input_link')} (Mỗi link cách nhau dấu phẩy)[/]")
        if not raw_url: return
        pwd = Prompt.ask(f"  [bold white]🔑 {Language.get('input_pwd')}[/]", password=True)
        url_list = [u.strip() for u in raw_url.split(',') if u.strip()]
        console.print()
        all_link_data     = []
        grand_total_files = 0
        grand_total_size  = 0
        for url in url_list:
            console.print(f"  [bold cyan]➜ Analyzing:[/] {url}")
            data = asyncio.run(self.downloader.get_tree_and_prepare(url, pwd))
            if not data:
                console.print(f"  [bold red]✖ Skipped (failed to load)[/]")
                continue
            flat = self._collect_files(data["folders"] + data["files"])
            all_link_data.append({"tree": data, "files": flat})
            grand_total_files += len(flat)
            grand_total_size  += sum(f['size'] for f in flat)
        if not all_link_data:
            console.print(f"\n  [bold red]{Language.get('no_files')}[/]")
            time.sleep(2); return
        console.print()
        summary = Table.grid(expand=True)
        summary.add_column(); summary.add_column(justify="right")
        summary.add_row(f"[bold cyan]Links OK: {len(all_link_data)}[/]  |  {Language.get('total_files')}: [bold cyan]{grand_total_files}[/]", f"{Language.get('total_size')}: [bold cyan]{self.downloader.format_size(grand_total_size)}[/]")
        console.print(Panel(summary, title=Language.get("link_info"), border_style="green", box=box.ROUNDED))
        console.print(f"  [1] {Language.get('dl_opt_1')}")
        console.print(f"  [2] {Language.get('dl_opt_2')}")
        console.print(f"  [0] {Language.get('dl_opt_0')}")
        mode = Prompt.ask(f"\n  {Language.get('prompt_choice')}", choices=["1", "2", "0"])
        if mode == "0": return
        master_files = []
        for link_idx, ld in enumerate(all_link_data):
            for f in ld["files"]: master_files.append({**f, "_link_idx": link_idx})
        if mode == "2":
            self.clear()
            ftable = Table(title="FILE LIST", box=None, padding=(0, 1))
            ftable.add_column("ID",   width=4,  style="cyan")
            ftable.add_column("Link", width=5,  style="dim")
            ftable.add_column("File")
            ftable.add_column("Size", justify="right", style="green")
            for i, f in enumerate(master_files[:100], 1): ftable.add_row(str(i), f"L{f['_link_idx']+1}", f['name'], self.downloader.format_size(f['size']))
            console.print(ftable)
            if len(master_files) > 100: console.print(f"  [dim italic]... +{len(master_files)-100} files[/]")
            raw = Prompt.ask(f"\n  [bold green]👉 ID (e.g., 1-3,5)[/]")
            idxs = set()
            try:
                for p in raw.split(','):
                    if '-' in p:
                        s, e = map(int, p.split('-'))
                        idxs.update(range(s, e + 1))
                    else: idxs.add(int(p))
                master_files = [master_files[i - 1] for i in idxs if 1 <= i <= len(master_files)]
            except: master_files = []
        if not master_files: return
        download_items = []
        for f in master_files:
            tree_data = all_link_data[f["_link_idx"]]["tree"]
            f["_share_id"] = tree_data["share_id"]
            f["_pass_token"] = tree_data["pass_token"]
            download_items.append(f)
        asyncio.run(reload_pool())
        link_count = len({f["_link_idx"] for f in download_items})
        console.print(f"\n[bold yellow]⬇ Downloading {len(download_items)} file(s) from {link_count} link(s) concurrently[/]")
        cancelled = self.run_download_with_retry(download_items)
        if cancelled:
            console.print("\n[bold red]⛔ Cancelled – remaining files skipped.[/]")
            time.sleep(2)

    def run_download_with_retry(self, files) -> bool:
        while True:
            self.clear()
            files.sort(key=lambda x: x['name'])
            total_size_bytes = sum(f['size'] for f in files)
            self.downloader.reset_progress()
            self.downloader.start_monitor(len(files), total_size_bytes)
            cancel_event = threading.Event()
            self.downloader.cancel_event = cancel_event
            user_cancelled = False
            with Live(self.downloader.generate_dashboard_table(), refresh_per_second=4, screen=True) as live:
                self.downloader.monitor_active = True
                def update_dashboard():
                    nonlocal user_cancelled
                    while self.downloader.monitor_active:
                        key = _read_key_nonblocking()
                        if key == 'q' and not cancel_event.is_set():
                            cancel_event.set()
                            user_cancelled = True
                            for pdata in self.downloader.progress_data.values():
                                if pdata.get('status') not in (*GOOD_STATUSES, 'Error', 'Failed', 'Cancelled', 'Waiting'):
                                    pdata['status'] = 'Cancelling...'
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
                        self.downloader.progress_data[i + 1] = {'id': i + 1, 'name': f['name'], 'percent': 0, 'speed': 0, 'status': 'Waiting', 'done_bytes': 0, 'total_bytes': int(f.get('size', 0)), 'eta': 0}
                    async def sem_download(f, i, api_for_file):
                        async with semaphore:
                            return await self.downloader.download_single_file(f, f['_share_id'], f['_pass_token'], i + 1, api=api_for_file)
                    for i, f in enumerate(files):
                        api_for_file = apis[i % len(apis)]
                        tasks.append(asyncio.create_task(sem_download(f, i, api_for_file)))
                    await asyncio.gather(*tasks, return_exceptions=True)
                    if self.downloader.bg_tasks:
                        await asyncio.gather(*list(self.downloader.bg_tasks), return_exceptions=True)

                asyncio.run(download_files())
                self.downloader.stop_monitor()
                dash_thread.join(timeout=1)
            self.clear()
            if user_cancelled:
                done_count = sum(1 for i in range(len(files)) if self.downloader.progress_data.get(i + 1, {}).get('status') in GOOD_STATUSES)
                console.print(Panel(f"[bold red]⛔ CANCELLED BY USER[/]\n\n[green]Completed before cancel : {done_count}[/]\n[red]Stopped / not started   : {len(files) - done_count}[/]", title="CANCELLED", border_style="red", box=box.ROUNDED))
                Prompt.ask("\n  [dim]Enter to continue...[/]")
                return True
            done_count   = 0
            failed_files = []
            for i, f in enumerate(files):
                status = self.downloader.progress_data.get(i + 1, {}).get('status', 'Unknown')
                if status in GOOD_STATUSES: done_count += 1
                else: failed_files.append(f)
            failed_count = len(failed_files)
            console.print(Panel(f"[bold green]COMPLETED![/]\n\n[green]Success: {done_count}[/]\n[red]Failed:  {failed_count}[/]", title="RESULT", border_style="green", box=box.ROUNDED))
            if failed_count > 0:
                if Confirm.ask(f"[bold yellow]⚠ Có {failed_count} file bị lỗi. Retry?[/]"):
                    files = failed_files
                    continue
                else: break
            else:
                self._auto_continue(seconds=3)
                break
        return False

    def _auto_continue(self, seconds: int = 3):
        import sys, os
        for remaining in range(seconds, 0, -1):
            msg = f"\r  [bold green]✓ Done! Continuing in {remaining}s... (Press Enter to skip)[/]  "
            console.print(msg, end="")
            start = time.time()
            while time.time() - start < 1:
                key = _read_key_nonblocking()
                if key in ('\r', '\n', ' '):
                    console.print()
                    return
                time.sleep(0.05)
        console.print()