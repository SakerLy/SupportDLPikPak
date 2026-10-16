import sys
import time
from rich.markup import escape as rich_escape
from rich.table import Table
from rich.prompt import Prompt
from config.settings import Config, console, Language
from core.updater import UpdateManager
from core.download.downloader import Downloader
from core.pikpak.account_pool import get_pool, reload_pool
from ui.common import MenuBase, _run
from ui.settings_menu import SettingsMixin
from ui.accounts_menu import AccountsMixin
from ui.download_menu import DownloadMixin


class Menu(SettingsMixin, AccountsMixin, DownloadMixin, MenuBase):
    def __init__(self):
        self.downloader = Downloader()

    async def _startup(self) -> int:
        n = await reload_pool()
        if n:
            from core.pikpak.garbage_collector import CloudGarbageCollector

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

