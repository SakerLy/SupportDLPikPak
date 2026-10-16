import time
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt, Confirm
from rich.align import Align
from rich import box
from config.settings import Config, console, Language
from core.cache import CacheManager
from core.net import proxy as proxy_mod
from core.logger import logger, LOG_LEVELS, set_log_level
from core import logger as log_mod
from ui.common import _run


class SettingsMixin:
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
