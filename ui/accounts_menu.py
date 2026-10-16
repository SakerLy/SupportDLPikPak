import time
import uuid
from rich.markup import escape as rich_escape
from rich.table import Table
from rich.prompt import Prompt
from rich.align import Align
from config.settings import Config, console, Language
from core.pikpak.account_pool import get_pool, reload_pool
from ui.common import _run


class AccountsMixin:
    def login_with_password(self):
        Config.load_config()
        result = self._prompt_login(Language.get("login_header"), Config.DEVICE_ID)
        if not result:
            return
        Config.REFRESH_TOKEN = result["refresh_token"]
        Config.DEVICE_ID = result["device_id"]
        Config.MAIN_USER_ID = result.get("user_id") or ""
        Config.CAPTCHA_TOKEN = ""
        Config.save_config()
        _run(reload_pool())
        console.print(f"\n  [bold green]{Language.get('login_success')}[/]")
        time.sleep(2)

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
        # Giữ device_id cũ để PikPak coi là cùng một thiết bị
        device_id = accounts[idx].get("device_id") or uuid.uuid4().hex
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
        result = self._prompt_login(Language.get("acc_add"), uuid.uuid4().hex)
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
