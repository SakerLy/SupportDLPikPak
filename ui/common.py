import sys
import time
import asyncio
from rich.prompt import Prompt, Confirm
from rich.align import Align
from config.settings import console, Language, APP_VERSION
from core.net.http_client import HttpClient
from core.pikpak.api import PikPakLogin
from core.logger import logger

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



class MenuBase:
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

    @staticmethod
    def _badge(enabled: bool) -> str:
        if enabled:
            return f"[bold green][ {Language.get('on'):^4} ][/]"
        return f"[bold red][ {Language.get('off'):^4} ][/]"

    @staticmethod
    def _ask_toggle(feature_name: str, current: bool) -> bool:
        badge = MenuBase._badge(current)
        console.print(f"\n  {feature_name}: {badge}")
        if current:
            if Confirm.ask(f"  {Language.get('toggle_off_q')}", default=False):
                return False
        else:
            if Confirm.ask(f"  {Language.get('toggle_on_q')}", default=False):
                return True
        return current
