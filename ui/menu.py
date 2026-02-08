import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt, Confirm
from rich.align import Align
from rich import box

# Import modules
from config.settings import Config, console, Language, APP_VERSION
from core.utils import UpdateManager, CacheManager
from core.api import PikPakLogin
from core.downloader import Downloader

# ASCII ART LOGO
ASCII_LOGO = """
[bold cyan]
██████╗ ██╗██╗  ██╗██████╗  █████╗ ██╗  ██╗
██╔══██╗██║██║ ██╔╝██╔══██╗██╔══██╗██║ ██╔╝
██████╔╝██║█████╔╝ ██████╔╝███████║█████╔╝ 
██╔═══╝ ██║██╔═██╗ ██╔═══╝ ██╔══██║██╔═██╗ 
██║     ██║██║  ██╗██║     ██║  ██║██║  ██╗
╚═╝     ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝
[/]"""

class Menu:
    def __init__(self):
        self.downloader = Downloader()

    def clear(self):
        import os
        os.system("cls" if os.name == "nt" else "clear")

    def print_header(self):
        self.clear()
        # Hiển thị ASCII Art căn giữa
        console.print(Align.center(ASCII_LOGO))
        console.print(Align.center(f"[bold white]Version {APP_VERSION}[/] | [dim]{Language.get('menu_dev')}[/]\n"))

    def main_menu(self):
        UpdateManager.check_for_updates()
        while True:
            Config.load_config()
            self.print_header()
            
            # Tạo bảng menu không viền
            table = Table(show_header=False, box=None, padding=(0, 2), expand=True)
            table.add_column("Key", justify="right", style="bold cyan", width=10)
            table.add_column("Action", style="bold white")
            
            table.add_row("[1]", Language.get("menu_1"))
            table.add_row("[2]", Language.get("menu_2"))
            table.add_row("[3]", Language.get("menu_3"))
            table.add_row("[4]", Language.get("menu_4"))
            table.add_row("[5]", Language.get("menu_5"))
            table.add_row("", "") # Dòng trống
            table.add_row("[0]", Language.get("menu_0"))
            
            console.print(table)
            console.print()
            
            c = Prompt.ask(f"[bold green]👉 {Language.get('prompt_choice')}[/]", choices=["1", "2", "3", "4", "5", "0"])
            
            if c == "1": self.login_with_password()
            elif c == "2": self.download_menu()
            elif c == "3": self.settings_menu()
            elif c == "4": self.cache_menu()
            elif c == "5": self.view_config()
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
                result = api.login()
        except: result = None
        
        if not result:
            console.print(f"\n  [bold red]{Language.get('login_fail')}[/]")
            time.sleep(2); return

        Config.REFRESH_TOKEN = result["refresh_token"]
        Config.DEVICE_ID = result["device_id"]
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
            table.add_column("Key", style="bold cyan", justify="right", width=10)
            table.add_column("Desc")
            
            table.add_row("[1]", Language.get("set_proxy"))
            table.add_row("[2]", Language.get("set_idm"))
            table.add_row("[3]", Language.get("set_adv"))
            table.add_row("[4]", Language.get("set_lang"))
            table.add_row("[5]", Language.get("prem_status"))
            table.add_row("", "")
            table.add_row("[0]", Language.get("menu_0"))
            
            console.print(table)
            console.print()
            
            c = Prompt.ask(f"[bold green]👉 {Language.get('prompt_choice')}[/]", choices=["1", "2", "3", "4", "5", "0"])
            if c == "1": self.proxy_setup()
            elif c == "2": self.idm_setup()
            elif c == "3": self.advanced_setup()
            elif c == "4": self.change_language()
            elif c == "5": self.premium_mode_setup()
            elif c == "0": break

    def change_language(self):
        self.print_header()
        console.print(f"\n  [bold cyan]{Language.get('lang_select')}[/]")
        
        table = Table(show_header=False, box=None, padding=(0, 2))
        table.add_row("[1]", "English")
        table.add_row("[2]", "Tiếng Việt")
        console.print(table)
        
        c = Prompt.ask("\n  Option", choices=["1", "2"])
        if c == "1": Config.LANGUAGE = "en"; console.print("\n  [bold green]✓ Language set to English[/]")
        elif c == "2": Config.LANGUAGE = "vi"; console.print("\n  [bold green]✓ Đã chuyển sang Tiếng Việt[/]")
        Config.save_config()
        time.sleep(1)

    def proxy_setup(self):
        self.print_header()
        status = "[bold green]ON[/]" if Config.USE_PROXY else "[bold red]OFF[/]"
        console.print(f"\n  {Language.get('proxy_status')}: {status}")
        
        if Confirm.ask(f"  {Language.get('proxy_toggle')}"): 
            Config.USE_PROXY = not Config.USE_PROXY
        
        if Config.USE_PROXY:
            Config.PROXY_TYPE = Prompt.ask(f"  {Language.get('proxy_type')}", choices=["http", "https", "socks5"], default=Config.PROXY_TYPE)
            Config.PROXY_HOST = Prompt.ask(f"  {Language.get('proxy_host')}", default=Config.PROXY_HOST)
            Config.PROXY_PORT = Prompt.ask(f"  {Language.get('proxy_port')}", default=Config.PROXY_PORT)
            Config.PROXY_USERNAME = Prompt.ask(f"  {Language.get('proxy_user')}", default=Config.PROXY_USERNAME)
            Config.PROXY_PASSWORD = Prompt.ask(f"  {Language.get('proxy_pass')}", default=Config.PROXY_PASSWORD)
        
        Config.save_config()
        console.print(f"\n  [bold green]{Language.get('save_success')}[/]"); time.sleep(1)
        
    def idm_setup(self):
        self.print_header()
        status = "[bold green]ON[/]" if Config.USE_IDM else "[bold red]OFF[/]"
        console.print(f"\n  {Language.get('idm_status')}: {status}")
        
        if Confirm.ask(f"  {Language.get('idm_toggle')}"): 
            Config.USE_IDM = not Config.USE_IDM
        
        if Config.USE_IDM:
            Config.IDM_PATH = Prompt.ask(f"  {Language.get('idm_path')}", default=Config.IDM_PATH)
        
        Config.CONCURRENT_THREADS = int(Prompt.ask(f"  {Language.get('thread_prompt')}", default=str(Config.CONCURRENT_THREADS)))
        Config.save_config()
        console.print(f"\n  [bold green]{Language.get('save_success')}[/]"); time.sleep(1)

    def premium_mode_setup(self):
        self.print_header()
        status = "[bold green]ON[/]" if Config.FORCE_PREMIUM_MODE else "[bold red]OFF[/]"
        console.print(f"\n  {Language.get('prem_status')}: {status}")
        
        if Confirm.ask(f"  {Language.get('prem_toggle')}", default=Config.FORCE_PREMIUM_MODE):
             Config.FORCE_PREMIUM_MODE = True
        else:
             Config.FORCE_PREMIUM_MODE = False
        Config.save_config()
        console.print(f"\n  [bold green]{Language.get('save_success')}[/]"); time.sleep(1)

    def advanced_setup(self):
        self.print_header()
        console.print("\n")
        Config.MAX_WORKERS = int(Prompt.ask(f"  {Language.get('worker_prompt')}", default=str(Config.MAX_WORKERS)))
        Config.DOWNLOAD_PATH_STR = Prompt.ask(f"  {Language.get('path_prompt')}", default=Config.DOWNLOAD_PATH_STR)
        Config.TIMEOUT = int(Prompt.ask(f"  {Language.get('timeout_prompt')}", default=str(Config.TIMEOUT)))
        Config.USE_CACHE = Confirm.ask(f"  {Language.get('cache_prompt')}", default=Config.USE_CACHE)
        Config.save_config(); Config.setup_dirs()
        console.print(f"\n  [bold green]{Language.get('save_success')}[/]"); time.sleep(1)

    def cache_menu(self):
        self.print_header()
        size, count = CacheManager.get_cache_size()
        
        table = Table(title=Language.get("cache_info"), box=None, padding=(0,2))
        table.add_column(Language.get("prompt_choice"), style="cyan")
        table.add_column("", style="bold white")
        table.add_row(Language.get("cache_files"), str(count))
        table.add_row(Language.get("cache_size"), f"{size/(1024*1024):.2f} MB")
        console.print(table)
        
        if Confirm.ask(f"\n  [bold red]{Language.get('cache_clear')}[/]"):
            CacheManager.clear_all(); console.print(f"  [bold green]{Language.get('cache_cleared')}[/]"); time.sleep(1)

    def view_config(self):
        Config.load_config(); self.print_header()
        grid = Table.grid(expand=True, padding=(0, 2))
        grid.add_column(style="cyan", justify="right"); grid.add_column(style="white")
        grid.add_row("Download Path:", str(Config.get_download_dir()))
        grid.add_row("Max Workers:", str(Config.MAX_WORKERS))
        grid.add_row("Timeout:", f"{Config.TIMEOUT}s")
        grid.add_row("Language:", Config.LANGUAGE)
        grid.add_row("Proxy:", f"{Config.PROXY_HOST}:{Config.PROXY_PORT}" if Config.USE_PROXY else "Off")
        grid.add_row("IDM:", "ON" if Config.USE_IDM else "Off")
        grid.add_row("Prem Mode:", "ON" if Config.FORCE_PREMIUM_MODE else "Off")
        grid.add_row("Threads/File:", str(Config.CONCURRENT_THREADS))
        
        console.print(Panel(grid, title=Language.get("menu_5"), border_style="blue", box=box.ROUNDED)) # Config dùng box rounded cho dễ nhìn
        Prompt.ask("\n  [dim]Enter...[/]")

    def download_menu(self):
        Config.load_config(); self.print_header()
        url = Prompt.ask(f"\n  [bold green]🔗 {Language.get('input_link')}[/]")
        if not url: return
        pwd = Prompt.ask(f"  [bold white]🔑 {Language.get('input_pwd')}[/]", password=True)
        
        data = self.downloader.get_tree_and_prepare(url, pwd)
        if not data: Prompt.ask("\n  [bold red]Enter...[/]"); return
        
        def collect(tree):
            res = []
            for item in tree:
                if item["type"] == "file": res.append(item)
                else: res.extend(collect(item["folders"] + item["files"]))
            return res
        
        flat_files = collect(data["folders"] + data["files"])
        total_size = sum([f['size'] for f in flat_files])
        
        # Info Panel
        info_table = Table.grid(expand=True)
        info_table.add_column(); info_table.add_column(justify="right")
        info_table.add_row(f"{Language.get('total_files')}: [bold cyan]{len(flat_files)}[/]", f"{Language.get('total_size')}: [bold cyan]{self.downloader.format_size(total_size)}[/]")
        console.print(Panel(info_table, title=Language.get("link_info"), border_style="green", box=box.ROUNDED))
        
        console.print(f"  [1] {Language.get('dl_opt_1')}")
        console.print(f"  [2] {Language.get('dl_opt_2')}")
        console.print(f"  [0] {Language.get('dl_opt_0')}")
        
        c = Prompt.ask(f"\n  {Language.get('prompt_choice')}", choices=["1", "2", "0"])
        targets = []
        if c == "1": targets = flat_files
        elif c == "2":
            self.clear()
            ftable = Table(title="FILE LIST", box=None, padding=(0,1))
            ftable.add_column("ID", width=4, style="cyan"); ftable.add_column("File"); ftable.add_column("Size", justify="right", style="green")
            for i, f in enumerate(flat_files[:50], 1): ftable.add_row(str(i), f['name'], self.downloader.format_size(f['size']))
            console.print(ftable)
            if len(flat_files) > 50: console.print(f"  [dim italic]... +{len(flat_files)-50} files[/]")
            raw = Prompt.ask(f"\n  [bold green]👉 ID (e.g., 1-3,5)[/]")
            idxs = set()
            try:
                for p in raw.split(','):
                    if '-' in p: s,e=map(int, p.split('-')); idxs.update(range(s, e+1))
                    else: idxs.add(int(p))
                targets = [flat_files[i-1] for i in idxs if 1<=i<=len(flat_files)]
            except: time.sleep(1); return

        if targets: self.run_download(targets, data)

    def run_download(self, files, tree_data):
        self.clear()
        files.sort(key=lambda x: x['name'])
        total_size_bytes = sum([f['size'] for f in files])
        self.downloader.progress_data = {}
        self.downloader.start_monitor(len(files), total_size_bytes)
        
        # Sửa lỗi: Import Live và dùng đúng cách
        with Live(self.downloader.generate_dashboard_table(), refresh_per_second=4, screen=True) as live:
            self.downloader.monitor_active = True
            
            def update_dashboard():
                while self.downloader.monitor_active: 
                    live.update(self.downloader.generate_dashboard_table())
                    time.sleep(0.5)
            
            dash_thread = threading.Thread(target=update_dashboard, daemon=True)
            dash_thread.start()
            
            with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
                futures = []
                for i, f in enumerate(files): 
                    futures.append(executor.submit(self.downloader.download_single_file, f, tree_data['share_id'], tree_data['pass_token'], i+1))
                for future in as_completed(futures): pass
            
            self.downloader.stop_monitor()
            dash_thread.join(timeout=1)

        self.clear()
        done = sum(1 for p in self.downloader.progress_data.values() if p['status'] == 'Done')
        skip = sum(1 for p in self.downloader.progress_data.values() if p['status'] == 'Skipped')
        err = sum(1 for p in self.downloader.progress_data.values() if p['status'] == 'Error')
        console.print(Panel(f"[bold green]{Language.get('dl_complete')}[/]\n\n{Language.get('dl_success')}: {done}\n{Language.get('dl_skip')}: {skip}\n{Language.get('dl_error')}: {err}", title="RESULT", border_style="green", box=box.ROUNDED))
        Prompt.ask("\n  [bold green]Enter...[/]")