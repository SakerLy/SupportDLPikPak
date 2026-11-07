import os
import shutil
import sys
import time
import webbrowser
import json
import zipfile
import urllib.request
from rich.panel import Panel
from rich.prompt import Confirm
from config.settings import (
    console,
    Language,
    APP_VERSION,
    GITHUB_ZIP_URL,
    GITHUB_VERSION_URL,
    GITHUB_RELEASE_URL,
    IS_FROZEN,
    BASE_DIR,
)
from core.logger import logger


class UpdateManager:
    API_URL = "https://api.github.com/repos/SakerLy/SupportDLPikPak/releases/latest"

    @staticmethod
    def check_for_updates():
        if "raw.githubusercontent.com" not in GITHUB_VERSION_URL:
            return
        found_update = False
        latest_tag = ""
        html_url = ""
        zip_url = GITHUB_ZIP_URL
        body = ""
        try:
            with console.status(
                f"[bold cyan]{Language.get('update_check')}", spinner="dots"
            ):
                with urllib.request.urlopen(UpdateManager.API_URL, timeout=5) as resp:
                    data = json.load(resp)
                latest_tag = data.get("tag_name", "0.0.0").lstrip("v")
                html_url = data.get("html_url", GITHUB_RELEASE_URL)
                # Tải đúng mã của bản release, không phải nhánh main (có thể đang dở dang)
                zip_url = data.get("zipball_url") or GITHUB_ZIP_URL
                body = data.get("body") or "No description"
                if UpdateManager.is_newer_version(latest_tag, APP_VERSION):
                    found_update = True
        except Exception:
            pass

        if found_update:
            console.print(
                Panel(
                    f"[bold yellow]{Language.get('update_found')}: v{latest_tag}[/]\n[dim]Current: {APP_VERSION}[/]\n\n[white]{body}[/]",
                    title="UPDATE",
                    border_style="green",
                )
            )
            time.sleep(0.5)
            try:
                if IS_FROZEN:
                    if Confirm.ask(Language.get("update_ask_web")):
                        webbrowser.open(html_url)
                else:
                    if Confirm.ask(Language.get("update_ask_src")):
                        UpdateManager.perform_full_zip_update(zip_url)
            except Exception:
                pass

    @staticmethod
    def is_newer_version(remote, local):
        try:
            r = [int(x) for x in remote.split(".")]
            l = [int(x) for x in local.split(".")]
            return r > l
        except Exception:
            return False

    @staticmethod
    def perform_full_zip_update(zip_url=GITHUB_ZIP_URL):
        temp_zip = BASE_DIR / "update_temp.zip"
        extract_path = BASE_DIR / "update_temp_folder"
        try:
            with console.status(
                "[bold green]Downloading full update package...", spinner="arrow3"
            ):
                with urllib.request.urlopen(zip_url, timeout=30) as r, open(temp_zip, "wb") as f:
                    shutil.copyfileobj(r, f)

            with console.status(
                "[bold blue]Extracting and applying files...", spinner="dots"
            ):
                with zipfile.ZipFile(temp_zip, "r") as zip_ref:
                    zip_ref.extractall(extract_path)
                subfolders = [f for f in extract_path.iterdir() if f.is_dir()]
                if not subfolders:
                    raise Exception("Invalid ZIP structure")
                source_folder = subfolders[0]
                keep = {"config.json", "downloads", "logs", ".cache", "cloud_temp.json"}
                for item in source_folder.iterdir():
                    if item.name in keep:
                        continue
                    dest_item = BASE_DIR / item.name
                    if item.is_dir():
                        # Chép đè, không xóa trước: lỗi giữa chừng (file bị khóa...) vẫn
                        # còn bản cũ chạy được thay vì mất cả thư mục
                        shutil.copytree(item, dest_item, dirs_exist_ok=True)
                    else:
                        shutil.copy2(item, dest_item)

            console.print(f"[bold green]{Language.get('update_done')}[/]")
            if temp_zip.exists():
                temp_zip.unlink()
            if extract_path.exists():
                shutil.rmtree(extract_path)
            time.sleep(2)
            if os.name == "nt":
                # execv trên Windows không giữ console và lỗi với đường dẫn có dấu cách
                import subprocess

                subprocess.call([sys.executable] + sys.argv)
                sys.exit(0)
            os.execv(sys.executable, [sys.executable] + sys.argv)
        except Exception as e:
            logger.exception("Update failed")
            console.print(f"[bold red]{Language.get('update_fail')}: {e}[/]")
            if temp_zip.exists():
                temp_zip.unlink()
            if extract_path.exists():
                shutil.rmtree(extract_path)
