import requests
import json
import hashlib
import time
import sys
import webbrowser
from datetime import datetime, timedelta
from rich.panel import Panel
from rich.prompt import Confirm

from config.settings import Config, console, Language, APP_VERSION, GITHUB_REPO_URL, GITHUB_RELEASE_URL, IS_FROZEN, BASE_DIR

class CacheManager:
    if IS_FROZEN: CACHE_DIR = BASE_DIR / ".cache"
    else: CACHE_DIR = BASE_DIR / ".cache"
    CACHE_DURATION = 3600
    @classmethod
    def init(cls): 
        try: cls.CACHE_DIR.mkdir(exist_ok=True)
        except: pass
        
    @classmethod
    def _get_cache_key(cls, *args): return hashlib.md5("_".join(str(arg) for arg in args).encode()).hexdigest()
    @classmethod
    def get(cls, cache_type, *args):
        key = cls._get_cache_key(cache_type, *args)
        cache_file = cls.CACHE_DIR / f"{key}.json"
        if not cache_file.exists(): return None
        try:
            with open(cache_file, 'r', encoding='utf-8') as f: data = json.load(f)
            if datetime.now() > datetime.fromisoformat(data.get('expire_time', '2000-01-01')): cache_file.unlink(); return None
            return data.get('data')
        except: return None
    @classmethod
    def set(cls, cache_type, data, *args, duration=None):
        key = cls._get_cache_key(cache_type, *args)
        cache_file = cls.CACHE_DIR / f"{key}.json"
        if duration is None: duration = cls.CACHE_DURATION
        try:
            with open(cache_file, 'w', encoding='utf-8') as f: json.dump({'data': data, 'expire_time': (datetime.now() + timedelta(seconds=duration)).isoformat(), 'created_at': datetime.now().isoformat()}, f)
            return True
        except: return False
    @classmethod
    def clear_all(cls):
        try: 
            for f in cls.CACHE_DIR.glob("*.json"): f.unlink()
            return True
        except: return False
    @classmethod
    def get_cache_size(cls):
        t = 0; c = 0
        try: 
            for f in cls.CACHE_DIR.glob("*.json"): t += f.stat().st_size; c += 1
            return t, c
        except: return 0, 0

class HttpClient:
    @staticmethod
    def request(method, url, headers=None, params=None, json_data=None):
        try:
            proxies = Config.get_proxy_dict()
            response = requests.request(method, url, headers=headers, params=params, json=json_data, proxies=proxies, timeout=(10, Config.TIMEOUT), verify=False)
            try: return response.status_code, response.json(), response.text
            except: return response.status_code, None, response.text
        except Exception as e: return 0, None, str(e)

class UpdateManager:
    API_URL = "https://api.github.com/repos/SakerLy/PikPakDownloader/releases/latest"
    
    @staticmethod
    def check_for_updates():
        if "raw.githubusercontent.com" not in GITHUB_REPO_URL: return 
        
        found_update = False
        latest_tag = ""
        html_url = ""
        body = ""

        try:
            with console.status(f"[bold cyan]{Language.get('update_check')}", spinner="dots"):
                response = requests.get(UpdateManager.API_URL, timeout=5)
                if response.status_code != 200: return
                data = response.json()
                latest_tag = data.get("tag_name", "0.0.0").lstrip("v")
                html_url = data.get("html_url", GITHUB_RELEASE_URL)
                body = data.get("body", "No description")
                
                if UpdateManager.is_newer_version(latest_tag, APP_VERSION):
                    found_update = True
        except: pass

        if found_update:
            console.print(Panel(f"[bold yellow]{Language.get('update_found')}: v{latest_tag}[/]\n[dim]Current: {APP_VERSION}[/]\n\n[white]{body}[/]", title="UPDATE", border_style="green"))
            time.sleep(0.5) 
            try:
                if IS_FROZEN:
                    if Confirm.ask(Language.get('update_ask_web')): webbrowser.open(html_url)
                else:
                    if Confirm.ask(Language.get('update_ask_src')): UpdateManager.perform_source_update()
            except Exception: pass

    @staticmethod
    def is_newer_version(remote, local):
        try:
            r = [int(x) for x in remote.split('.')]
            l = [int(x) for x in local.split('.')]
            return r > l
        except: return False
    @staticmethod
    def perform_source_update():
        try:
            with console.status("[bold green]Downloading update...", spinner="arrow3"):
                response = requests.get(GITHUB_REPO_URL, timeout=10)
                if response.status_code == 200:
                    if "APP_VERSION" in response.text and "def main():" in response.text:
                        with open(sys.argv[0], 'w', encoding='utf-8') as f: f.write(response.text)
                        console.print(f"[bold green]{Language.get('update_done')}[/]"); time.sleep(2)
                        import os
                        os.execv(sys.executable, ['python'] + sys.argv)
                    else: console.print(f"[bold red]✖ {Language.get('update_fail')} (Corrupted)[/]")
                else: console.print(f"[bold red]✖ {Language.get('update_fail')} (Net)[/]")
        except Exception as e: console.print(f"[bold red]✖ {e}[/]")