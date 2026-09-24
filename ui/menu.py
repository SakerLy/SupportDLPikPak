import re
import sys
import time
import threading
import asyncio
from rich.markup import escape as rich_escape
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt, Confirm
from rich.align import Align
from rich import box
from config.settings import Config, console, Language, APP_VERSION
from core.utils import UpdateManager, CacheManager, HttpClient
from core.api import PikPakLogin
from core.downloader import Downloader, GOOD_STATUSES
from core.account_pool import get_pool, reload_pool
from core import proxy as proxy_mod
from core.logger import logger, LOG_LEVELS, set_log_level
from core import logger as log_mod

ASCII_LOGO = """
[bold cyan]
██████╗ ██╗██╗  ██╗██████╗  █████╗ ██╗  ██╗
██╔══██╗██║██║ ██╔╝██╔══██╗██╔══██╗██║ ██╔╝
██████╔╝██║█████╔╝ ██████╔╝███████║█████╔╝
██╔═══╝ ██║██╔═██╗ ██╔═══╝ ██╔══██║██╔═██╗
██║     ██║██║  ██╗██║     ██║  ██║██║  ██╗
╚═╝     ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝
[/]"""


def _run(coro):
    # asyncio.run tạo event loop mới mỗi lần, nên phải đóng session HTTP dùng chung
    # trước khi loop chết để không rò rỉ socket
    async def _wrapped():
        try:
            return await coro
        finally:
            await HttpClient.close()

    return asyncio.run(_wrapped())


def _read_key_nonblocking():
    try:
        import os

        if os.name == "nt":
            import msvcrt

            if msvcrt.kbhit():
                return msvcrt.getwch().lower()
        else:
            import select
            import tty
            import termios

            fd = sys.stdin.fileno()
            old = termios.tcgetattr(fd)
            try:
                tty.setcbreak(fd)
                r, _, _ = select.select([sys.stdin], [], [], 0)
                if r:
                    return sys.stdin.read(1).lower()
            finally:
                termios.tcsetattr(fd, termios.TCSADRAIN, old)
    except Exception:
        pass
    return None


class Menu:
    def __init__(self):
        self.downloader = Downloader()

    async def _startup(self) -> int:
        n = await reload_pool()
        if n:
            from core.garbage_collector import CloudGarbageCollector

            await CloudGarbageCollector().run()
        return n

    @staticmethod
    def _warn_failed_accounts() -> bool:
        """Báo rõ account nào hỏng và cách sửa, thay vì để pool âm thầm về 0."""
        failed = get_pool().failed()
        for s in failed:
            if s.api.needs_relogin:
                msg = (
                    Language.get("warn_main_relogin")
                    if s.index == 0
                    else Language.get("warn_extra_relogin", n=s.index)
                )
            else:
                msg = Language.get("warn_acc_error", label=s.api.label, err=s.error)
            console.print(f"  [bold yellow]{rich_escape(msg)}[/]")
        return bool(failed)

    def _prompt_login(self, title: str, device_id: str):
        """Hỏi tài khoản/mật khẩu rồi đăng nhập. Trả về kết quả login hoặc None."""
        self.print_header()
        console.print(f"[bold cyan]  {title}[/]\n")
        username = Prompt.ask(f"  {Language.get('login_user')}")
        password = Prompt.ask(f"  {Language.get('login_pass')}", password=True)
        api = PikPakLogin(username, password, device_id)
        try:
            with console.status(
                f"[bold cyan]  {Language.get('login_wait')}", spinner="dots"
            ):
                result = _run(api.login())
        except Exception:
            logger.exception("Đăng nhập thất bại")
            result = None
        if not result:
            console.print(f"\n  [bold red]{Language.get('login_fail')}[/]")
            time.sleep(2)
        return result

    def clear(self):
        import os

        os.system("cls" if os.name == "nt" else "clear")

    def print_header(self):
        self.clear()
        console.print(Align.center(ASCII_LOGO))
        console.print(
            Align.center(
                f"[bold white]VERSION {APP_VERSION}[/] | [dim]{Language.get('menu_dev')}[/]\n"
            )
        )

    def main_menu(self):
        UpdateManager.check_for_updates()
        Config.load_config()
        n = _run(self._startup())
        if n > 1:
            console.print(
                f"  [bold green]{Language.get('pool_startup', n=n, speed=n * 11)}[/]"
            )
            time.sleep(1)
        if self._warn_failed_accounts():
            Prompt.ask(f"\n  [dim]{Language.get('press_enter')}[/]", default="", show_default=False)
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
            ps = pool.size()
            if ps > 1:
                console.print(
                    f"\n  [bold green]{Language.get('pool_line', n=ps, speed=ps * 11)}[/]"
                )
            else:
                console.print(f"\n  [dim]{Language.get('pool_tip')}[/]")
            console.print()
            c = Prompt.ask(
                f"[bold green]👉 {Language.get('prompt_choice')}[/]",
                choices=["1", "2", "3", "4", "5", "6", "0"],
            )
            if c == "1":
                self.login_with_password()
            elif c == "2":
                self.download_menu()
            elif c == "3":
                self.settings_menu()
            elif c == "4":
                self.cache_menu()
            elif c == "5":
                self.view_config()
            elif c == "6":
                self.extra_accounts_menu()
            elif c == "0":
                sys.exit()

    def login_with_password(self):
        self.print_header()
        console.print(f"[bold cyan]  {Language.get('login_header')}[/]\n")
        username = Prompt.ask(f"  {Language.get('login_user')}")
        password = Prompt.ask(f"  {Language.get('login_pass')}", password=True)
        Config.load_config()
        api = PikPakLogin(username, password, Config.DEVICE_ID)
        try:
            with console.status(
                f"[bold cyan]  {Language.get('login_wait')}", spinner="dots"
            ):
                result = _run(api.login())
        except:
            result = None
        if not result:
            console.print(f"\n  [bold red]{Language.get('login_fail')}[/]")
            time.sleep(2)
            return
        Config.REFRESH_TOKEN = result["refresh_token"]
        Config.DEVICE_ID = result["device_id"]
        Config.MAIN_USER_ID = result.get("user_id") or ""
        Config.CAPTCHA_TOKEN = ""
        Config.save_config()
        _run(reload_pool())
        console.print(f"\n  [bold green]{Language.get('login_success')}[/]")
        time.sleep(2)

    def settings_menu(self):
        while True:
            Config.load_config()
            self.print_header()
            console.print(Align.center(f"[bold yellow]{Language.get('set_header')}[/]"))
            console.print()
            table = Table(show_header=False, box=None, padding=(0, 2))
            table.add_column("Key", style="bold cyan", justify="right", width=6)
            table.add_column("Action", style="bold white", min_width=28)
            table.add_column("State", justify="left", min_width=12)
            proxy_state = self._badge(Config.USE_PROXY)
            if Config.USE_PROXY and Config.PROXY_HOST:
                proxy_state += f" [dim]{Config.PROXY_TYPE}[/]"
            table.add_row("[1]", Language.get("set_proxy"), proxy_state)
            table.add_row(
                "[2]",
                Language.get("set_threads"),
                f"[cyan]{Config.CONCURRENT_THREADS} conn[/]",
            )
            table.add_row("[3]", Language.get("set_adv"), "")
            table.add_row(
                "[4]", Language.get("set_lang_short"), f"[dim]{Config.LANGUAGE.upper()}[/]"
            )
            table.add_row(
                "[5]", Language.get("set_prem"), self._badge(Config.FORCE_PREMIUM_MODE)
            )
            table.add_row("[6]", Language.get("set_cache"), self._badge(Config.USE_CACHE))
            table.add_row("", "", "")
            table.add_row("[0]", Language.get("back"), "")
            console.print(table)
            console.print()
            c = Prompt.ask(
                f"[bold green]👉 {Language.get('prompt_choice')}[/]",
                choices=["1", "2", "3", "4", "5", "6", "0"],
            )
            if c == "1":
                self.proxy_setup()
            elif c == "2":
                self.threads_setup()
            elif c == "3":
                self.advanced_setup()
            elif c == "4":
                self.change_language()
            elif c == "5":
                self.premium_mode_setup()
            elif c == "6":
                self._toggle_cache_quick()
            elif c == "0":
                break

    def change_language(self):
        self.print_header()
        console.print(f"\n  [bold cyan]{Language.get('lang_select')}[/]")
        table = Table(show_header=False, box=None, padding=(0, 2))
        table.add_row("[1]", "English")
        table.add_row("[2]", "Tiếng Việt")
        console.print(table)
        c = Prompt.ask(f"\n  {Language.get('prompt_choice')}", choices=["1", "2"])
        Config.LANGUAGE = "en" if c == "1" else "vi"
        Config.save_config()
        console.print(f"\n  [bold green]{Language.get('lang_set')}[/]")
        time.sleep(1)

    def _toggle_cache_quick(self):
        Config.USE_CACHE = self._ask_toggle("Cache", Config.USE_CACHE)
        Config.save_config()
        console.print(
            f"  [bold green]{Language.get('saved')}[/]  Cache: {self._badge(Config.USE_CACHE)}"
        )
        time.sleep(1)

    @staticmethod
    def _badge(enabled: bool) -> str:
        if enabled:
            return f"[bold green][ {Language.get('on'):^4} ][/]"
        return f"[bold red][ {Language.get('off'):^4} ][/]"

    @staticmethod
    def _ask_toggle(feature_name: str, current: bool) -> bool:
        badge = Menu._badge(current)
        console.print(f"\n  {feature_name}: {badge}")
        if current:
            if Confirm.ask(f"  {Language.get('toggle_off_q')}", default=False):
                return False
        else:
            if Confirm.ask(f"  {Language.get('toggle_on_q')}", default=False):
                return True
        return current

    def proxy_setup(self):
        while True:
            Config.load_config()
            self.print_header()
            console.print(f"\n  [bold cyan]━━  {Language.get('px_header')}  ━━[/]")
            grid = Table.grid(padding=(0, 3))
            grid.add_column(style="dim", justify="right")
            grid.add_column()
            grid.add_row(Language.get("px_status"), self._badge(Config.USE_PROXY))
            if Config.PROXY_HOST:
                grid.add_row(
                    Language.get("px_address"),
                    f"[white]{proxy_mod.display_url()}[/]",
                )
            console.print(grid)
            console.print()
            opts = Table(show_header=False, box=None, padding=(0, 2))
            opts.add_column("Key", style="bold cyan", justify="right", width=6)
            opts.add_column()
            opts.add_row("[1]", Language.get("px_opt_toggle"))
            opts.add_row("[2]", Language.get("px_opt_edit"))
            opts.add_row("[3]", Language.get("px_opt_paste"))
            opts.add_row("[4]", Language.get("px_opt_test"))
            opts.add_row("", "")
            opts.add_row("[0]", Language.get("back"))
            console.print(opts)
            console.print()
            c = Prompt.ask(
                f"  {Language.get('prompt_choice')}", choices=["1", "2", "3", "4", "0"]
            )
            if c == "0":
                break
            elif c == "1":
                Config.USE_PROXY = self._ask_toggle("Proxy", Config.USE_PROXY)
                Config.save_config()
                console.print(
                    f"  [bold green]{Language.get('saved')}[/]  Proxy: {self._badge(Config.USE_PROXY)}"
                )
                time.sleep(1)
            elif c == "2":
                self._proxy_edit_fields()
            elif c == "3":
                self._proxy_paste()
            elif c == "4":
                self._proxy_test()

    def _proxy_edit_fields(self):
        console.print(f"\n  [dim]{Language.get('px_hint_auth')}[/]\n")
        console.print(f"  [dim]{Language.get('px_type_label')}[/]")
        for t in proxy_mod.PROXY_TYPES:
            console.print(f"  [dim]  {Language.get('px_type_' + t)}[/]")
        ptype = Prompt.ask(
            f"  {Language.get('px_type')}",
            choices=list(proxy_mod.PROXY_TYPES),
            default=Config.PROXY_TYPE if Config.PROXY_TYPE in proxy_mod.PROXY_TYPES else "socks5",
        )
        if ptype == "http":
            console.print(f"  [bold yellow]{Language.get('px_warn_http')}[/]")
        host = Prompt.ask(f"  {Language.get('px_host')}", default=Config.PROXY_HOST).strip()
        port_raw = Prompt.ask(f"  {Language.get('px_port')}", default=Config.PROXY_PORT)
        try:
            p = int(port_raw.strip())
            if not host or not (1 <= p <= 65535):
                raise ValueError
        except ValueError:
            console.print(f"  [bold red]{Language.get('px_port_invalid')}[/]")
            time.sleep(1.5)
            return
        user = ""
        pwd = ""
        if ptype != "socks4":
            user = Prompt.ask(
                f"  {Language.get('px_user')}", default=Config.PROXY_USERNAME
            ).strip()
            if user:
                pwd = Prompt.ask(
                    f"  {Language.get('px_pass')}",
                    default=Config.PROXY_PASSWORD,
                    password=True,
                    show_default=False,
                )
        self._proxy_save(ptype, host, str(p), user, pwd)

    def _proxy_paste(self):
        console.print(f"\n  [dim]{Language.get('px_paste_formats')}[/]")
        raw = Prompt.ask(f"  {Language.get('px_paste_prompt')}")
        try:
            info = proxy_mod.parse_proxy_string(raw)
        except ValueError:
            console.print(f"  [bold red]{Language.get('px_paste_invalid')}[/]")
            time.sleep(1.5)
            return
        ptype = info["type"]
        if not ptype:
            ptype = Prompt.ask(
                f"  {Language.get('px_type')}",
                choices=list(proxy_mod.PROXY_TYPES),
                default="socks5",
            )
        self._proxy_save(
            ptype, info["host"], info["port"], info["username"], info["password"]
        )

    def _proxy_save(self, ptype, host, port, user, pwd):
        Config.PROXY_TYPE = ptype
        Config.PROXY_HOST = host
        Config.PROXY_PORT = port
        Config.PROXY_USERNAME = user
        Config.PROXY_PASSWORD = pwd if user else ""
        if not Config.USE_PROXY and Confirm.ask(
            f"  {Language.get('px_enable_q')}", default=True
        ):
            Config.USE_PROXY = True
        Config.save_config()
        console.print(f"  [bold green]{Language.get('saved')}[/]")
        if Config.USE_PROXY:
            self._proxy_test()
        else:
            time.sleep(1)

    def _proxy_test(self):
        with console.status(f"  {Language.get('px_testing')}", spinner="dots"):
            try:
                results = _run(proxy_mod.test_proxy())
            except Exception as e:
                results = [("Proxy", False, str(e))]
        logger.info(
            "Kiểm tra proxy %s: %s",
            proxy_mod.display_url(),
            " | ".join(f"{n}={'OK' if ok else 'FAIL'} ({d})" for n, ok, d in results),
        )
        console.print()
        for name, ok, detail in results:
            icon = "[bold green]✓[/]" if ok else "[bold red]✖[/]"
            console.print(f"  {icon} [bold]{name:<7}[/] {detail}")
        all_ok = bool(results) and all(ok for _, ok, _ in results) and len(results) == 3
        console.print()
        if all_ok:
            console.print(f"  [bold green]{Language.get('px_test_ok')}[/]")
        else:
            console.print(f"  [bold red]{Language.get('px_test_fail')}[/]\n")
            console.print(f"  [bold yellow]{Language.get('px_fix_title')}[/]")
            console.print(f"  [dim]{Language.get('px_fix_1')}[/]")
            console.print(f"  [dim]{Language.get('px_fix_2')}[/]")
            console.print(f"  [dim]{Language.get('px_fix_3')}[/]")
        Prompt.ask(f"\n  [dim]{Language.get('press_enter')}[/]", default="", show_default=False)

    def threads_setup(self):
        self.print_header()
        console.print(f"\n  [bold cyan]━━  {Language.get('thr_header')}  ━━[/]\n")
        grid = Table.grid(padding=(0, 3))
        grid.add_column(style="dim", justify="right")
        grid.add_column()
        grid.add_row(
            Language.get("thr_current"), f"[cyan]{Config.CONCURRENT_THREADS}[/] connections"
        )
        grid.add_row(
            Language.get("thr_est"),
            f"[cyan]~{Config.CONCURRENT_THREADS * 11} MB/s[/] {Language.get('thr_max')}",
        )
        grid.add_row(Language.get("thr_reco"), f"[dim]{Language.get('thr_reco_val')}[/]")
        console.print(grid)
        console.print()
        try:
            v = Prompt.ask(
                f"  {Language.get('thread_prompt')}", default=str(Config.CONCURRENT_THREADS)
            )
            Config.CONCURRENT_THREADS = min(64, max(1, int(v)))
        except ValueError:
            pass
        Config.save_config()
        n = Config.CONCURRENT_THREADS
        console.print(
            f"\n  [bold green]{Language.get('saved')}[/]  {Language.get('thr_saved', n=n, speed=n * 11)}"
        )
        time.sleep(1.5)

    def premium_mode_setup(self):
        self.print_header()
        console.print(f"\n  [bold cyan]━━  {Language.get('prem_header')}  ━━[/]")
        grid = Table.grid(padding=(0, 3))
        grid.add_column(style="dim", justify="right")
        grid.add_column()
        grid.add_row(Language.get("px_status"), self._badge(Config.FORCE_PREMIUM_MODE))
        grid.add_row(Language.get("prem_when_on"), f"[dim]{Language.get('prem_on_desc')}[/]")
        grid.add_row(Language.get("prem_when_off"), f"[dim]{Language.get('prem_off_desc')}[/]")
        console.print(grid)
        Config.FORCE_PREMIUM_MODE = self._ask_toggle(
            "Premium Mode", Config.FORCE_PREMIUM_MODE
        )
        Config.save_config()
        console.print(
            f"\n  [bold green]{Language.get('saved')}[/]  {Language.get('set_prem')}: {self._badge(Config.FORCE_PREMIUM_MODE)}"
        )
        time.sleep(1.5)

    def advanced_setup(self):
        self.print_header()
        console.print(f"\n  [bold cyan]━━  {Language.get('adv_header')}  ━━[/]")
        grid = Table.grid(padding=(0, 3))
        grid.add_column(style="dim", justify="right")
        grid.add_column()
        grid.add_row(Language.get("adv_workers"), f"[cyan]{Config.MAX_WORKERS}[/]")
        grid.add_row(Language.get("adv_path"), f"[dim]{Config.get_download_dir()}[/]")
        grid.add_row(Language.get("adv_timeout"), f"[cyan]{Config.TIMEOUT}[/]s")
        grid.add_row(Language.get("cfg_log"), f"[cyan]{Config.LOG_LEVEL}[/]")
        grid.add_row("Cache:", self._badge(Config.USE_CACHE))
        console.print(grid)
        console.print()
        try:
            v = Prompt.ask(
                f"  {Language.get('worker_prompt')}", default=str(Config.MAX_WORKERS)
            )
            Config.MAX_WORKERS = min(32, max(1, int(v)))
        except ValueError:
            pass
        old_path = Config.DOWNLOAD_PATH_STR
        Config.DOWNLOAD_PATH_STR = (
            Prompt.ask(
                f"  {Language.get('path_prompt')}", default=Config.DOWNLOAD_PATH_STR
            ).strip().strip('"')
            or old_path
        )
        if not Config.setup_dirs():
            console.print(f"  [bold red]{Language.get('path_invalid')}[/]")
            Config.DOWNLOAD_PATH_STR = old_path
        try:
            v = Prompt.ask(
                f"  {Language.get('timeout_prompt')}", default=str(Config.TIMEOUT)
            )
            Config.TIMEOUT = min(600, max(5, int(v)))
        except ValueError:
            pass
        raw = Prompt.ask(
            f"  {Language.get('log_prompt')} [dim]({'/'.join(LOG_LEVELS)})[/]",
            default=Config.LOG_LEVEL,
        )
        # Gõ thường hay hoa đều được; gõ sai thì giữ nguyên mức cũ
        if raw.strip().upper() in (*LOG_LEVELS, "WARN"):
            Config.LOG_LEVEL = set_log_level(raw)
        Config.USE_CACHE = self._ask_toggle("Cache", Config.USE_CACHE)
        Config.save_config()
        console.print(f"\n  [bold green]{Language.get('adv_saved')}[/]")
        time.sleep(1.5)

    def cache_menu(self):
        self.print_header()
        size, count = CacheManager.get_cache_size()
        table = Table(title=Language.get("cache_info"), box=None, padding=(0, 2))
        table.add_column("", style="cyan")
        table.add_column("", style="bold white")
        table.add_row(Language.get("cache_files"), str(count))
        table.add_row(Language.get("cache_size"), f"{size / (1024 * 1024):.2f} MB")
        console.print(table)
        if Confirm.ask(f"\n  [bold red]{Language.get('cache_clear')}[/]"):
            CacheManager.clear_all()
            console.print(f"  [bold green]{Language.get('cache_cleared')}[/]")
            time.sleep(1)

    def view_config(self):
        Config.load_config()
        self.print_header()
        grid = Table.grid(expand=True, padding=(0, 2))
        grid.add_column(style="cyan", justify="right")
        grid.add_column(style="white")
        grid.add_row(Language.get("cfg_path"), str(Config.get_download_dir()))
        grid.add_row(Language.get("cfg_workers"), str(Config.MAX_WORKERS))
        grid.add_row(Language.get("cfg_timeout"), f"{Config.TIMEOUT}s")
        grid.add_row(Language.get("cfg_lang"), Config.LANGUAGE)
        grid.add_row(
            Language.get("cfg_proxy"),
            proxy_mod.display_url() if proxy_mod.is_enabled() else Language.get("off"),
        )
        grid.add_row(
            Language.get("cfg_prem"),
            Language.get("on") if Config.FORCE_PREMIUM_MODE else Language.get("off"),
        )
        grid.add_row(Language.get("cfg_threads"), str(Config.CONCURRENT_THREADS))
        grid.add_row(
            Language.get("cfg_accounts"),
            f"{1 if Config.REFRESH_TOKEN else 0} + {len(Config.EXTRA_ACCOUNTS)}",
        )
        grid.add_row(Language.get("cfg_log"), Config.LOG_LEVEL)
        if log_mod.LOG_FILE:
            grid.add_row(Language.get("cfg_log_file"), str(log_mod.LOG_FILE))
        console.print(
            Panel(
                grid, title=Language.get("menu_5"), border_style="blue", box=box.ROUNDED
            )
        )
        Prompt.ask(f"\n  [dim]{Language.get('press_enter')}[/]", default="", show_default=False)

    def extra_accounts_menu(self):
        while True:
            Config.load_config()
            self.print_header()
            console.print(Align.center(f"[bold yellow]{Language.get('acc_header')}[/]"))
            console.print(f"\n  [dim]{Language.get('acc_info')}[/]\n")
            accounts = Config.EXTRA_ACCOUNTS
            if accounts:
                pool = get_pool()
                tbl = Table(box=None, padding=(0, 2))
                tbl.add_column("#", style="bold cyan", width=4)
                tbl.add_column(Language.get("acc_col_user"), style="dim")
                tbl.add_column("Device ID", style="dim")
                tbl.add_column(Language.get("acc_col_status"))
                for i, acc in enumerate(accounts, start=1):
                    did = acc.get("device_id", "")[:12] + "..."
                    uid = acc.get("user_id") or "-"
                    slot = pool.slot_by_index(i)
                    if slot is None:
                        st = f"[dim]{Language.get('acc_st_unknown')}[/]"
                    elif slot.ready:
                        st = f"[green]{Language.get('acc_st_ok')}[/]"
                    elif slot.api.needs_relogin:
                        st = f"[bold red]{Language.get('acc_st_relogin')}[/]"
                    else:
                        st = f"[red]{Language.get('acc_st_error')}[/] [dim]{rich_escape(slot.error[:40])}[/]"
                    tbl.add_row(str(i), uid, did, st)
                console.print(tbl)
            else:
                console.print(f"  [dim]{Language.get('acc_none')}[/]")
            pool = get_pool()
            console.print(
                f"\n  [bold green]{Language.get('acc_pool_size')}: {pool.size()}[/]\n"
            )
            table = Table(show_header=False, box=None, padding=(0, 2))
            table.add_column("Key", style="bold cyan", justify="right", width=6)
            table.add_column("Desc")
            table.add_row("[1]", Language.get("acc_add"))
            table.add_row("[2]", Language.get("acc_remove"))
            table.add_row("[3]", Language.get("acc_test"))
            table.add_row("[4]", Language.get("acc_relogin"))
            table.add_row("", "")
            table.add_row("[0]", Language.get("back"))
            console.print(table)
            console.print()
            c = Prompt.ask(
                f"[bold green]👉 {Language.get('prompt_choice')}[/]",
                choices=["1", "2", "3", "4", "0"],
            )
            if c == "0":
                break
            elif c == "1":
                self._add_extra_account()
            elif c == "2":
                self._remove_extra_account()
            elif c == "3":
                self._test_accounts()
            elif c == "4":
                self._relogin_extra_account()

    def _relogin_extra_account(self):
        accounts = Config.EXTRA_ACCOUNTS
        if not accounts:
            console.print(f"  [dim]{Language.get('acc_none')}[/]")
            time.sleep(2)
            return
        # Chỉ có 1 tài khoản hỏng thì chọn luôn, khỏi hỏi số thứ tự
        broken = [s.index for s in get_pool().failed() if s.index > 0]
        default = str(broken[0]) if len(broken) == 1 else None
        idx_str = Prompt.ask(
            f"  {Language.get('acc_relogin_prompt', n=len(accounts))}", default=default
        )
        try:
            idx = int(idx_str) - 1
            if not (0 <= idx < len(accounts)):
                raise ValueError
        except (TypeError, ValueError):
            console.print(f"  [bold red]{Language.get('acc_invalid')}[/]")
            time.sleep(2)
            return
        import uuid as _uuid

        # Giữ device_id cũ để PikPak coi là cùng một thiết bị
        device_id = accounts[idx].get("device_id") or _uuid.uuid4().hex
        result = self._prompt_login(
            f"{Language.get('acc_relogin')} #{idx + 1}", device_id
        )
        if not result:
            return
        Config.load_config()
        uid = result.get("user_id") or ""
        others = {
            a.get("user_id")
            for i, a in enumerate(Config.EXTRA_ACCOUNTS)
            if i != idx and a.get("user_id")
        }
        if uid and (uid in others or uid == Config.MAIN_USER_ID):
            console.print(f"\n  [bold red]{Language.get('acc_duplicate')}[/]")
            time.sleep(2)
            return
        Config.EXTRA_ACCOUNTS[idx] = {
            "refresh_token": result["refresh_token"],
            "device_id": result["device_id"],
            "user_id": uid,
        }
        Config.save_config()
        n = _run(reload_pool())
        console.print(f"\n  [bold green]{Language.get('acc_relogin_done', n=idx + 1)}[/]")
        console.print(f"  [green]{Language.get('acc_pool_now', n=n, speed=n * 11)}[/]")
        time.sleep(2)

    def _add_extra_account(self):
        import uuid as _uuid

        result = self._prompt_login(Language.get("acc_add"), _uuid.uuid4().hex)
        if not result:
            return
        Config.load_config()
        uid = result.get("user_id") or ""
        known = {a.get("user_id") for a in Config.EXTRA_ACCOUNTS if a.get("user_id")}
        if uid and (uid in known or uid == Config.MAIN_USER_ID):
            console.print(f"\n  [bold red]{Language.get('acc_duplicate')}[/]")
            time.sleep(2)
            return
        Config.EXTRA_ACCOUNTS.append(
            {
                "refresh_token": result["refresh_token"],
                "device_id": result["device_id"],
                "user_id": uid,
            }
        )
        Config.save_config()
        n = _run(reload_pool())
        console.print(f"\n  [bold green]{Language.get('acc_added')}[/]")
        console.print(f"  [green]{Language.get('acc_pool_now', n=n, speed=n * 11)}[/]")
        time.sleep(2)

    def _remove_extra_account(self):
        self.print_header()
        accounts = Config.EXTRA_ACCOUNTS
        if not accounts:
            console.print(f"  [dim]{Language.get('acc_none')}[/]")
            time.sleep(2)
            return
        idx_str = Prompt.ask(f"  {Language.get('acc_remove_prompt', n=len(accounts))}")
        try:
            idx = int(idx_str) - 1
            if not (0 <= idx < len(accounts)):
                raise ValueError
        except ValueError:
            console.print(f"  [bold red]{Language.get('acc_invalid')}[/]")
            time.sleep(2)
            return
        Config.EXTRA_ACCOUNTS.pop(idx)
        Config.save_config()
        _run(reload_pool())
        console.print(f"  [bold green]{Language.get('acc_removed')}[/]")
        time.sleep(1)

    def _test_accounts(self):
        self.print_header()
        console.print(f"[bold cyan]  {Language.get('acc_test')}[/]\n")
        n = _run(reload_pool(verbose=True))
        total = (1 if Config.REFRESH_TOKEN else 0) + len(Config.EXTRA_ACCOUNTS)
        console.print(
            f"\n  [bold green]{Language.get('acc_active', n=n, total=total)}[/]"
        )
        console.print(f"  [bold green]{Language.get('acc_speed', speed=n * 11)}[/]")
        Prompt.ask(f"\n  [dim]{Language.get('press_enter')}[/]", default="", show_default=False)

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
                    await asyncio.gather(*tasks, return_exceptions=True)
                    if self.downloader.bg_tasks:
                        await asyncio.gather(
                            *list(self.downloader.bg_tasks), return_exceptions=True
                        )

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
        for remaining in range(seconds, 0, -1):
            msg = f"\r  [bold green]{Language.get('dl_auto_continue', n=remaining)}[/]  "
            console.print(msg, end="")
            start = time.time()
            while time.time() - start < 1:
                key = _read_key_nonblocking()
                if key in ("\r", "\n", " "):
                    console.print()
                    return
                time.sleep(0.05)
        console.print()
