import hashlib
import time
import uuid
import json
import aiohttp
import asyncio
from config.settings import Config, console, Language
from core.utils import HttpClient, CacheManager
from core.logger import logger

class PikPakLogin:
    USER_API       = "https://user.mypikpak.com"
    CLIENT_ID      = "YNxT9w7GMdWvEOKa"
    CLIENT_SECRET  = "dbw2OtmVEeuUvIptb1Coyg"
    CLIENT_VERSION = "1.47.1"
    PACKAGE_NAME   = "com.pikcloud.pikpak"
    SDK_VERSION = "8.1.4"
    SALTS = [
        "Gez0T9ijiI9WCeTsKSg3SMlx","zQdbalsolyb1R/","ftOjr52zt51JD68C3s",
        "yeOBMH0JkbQdEFNNwQ0RI9T3wU/v","BRJrQZiTQ65WtMvwO","je8fqxKPdQVJiy1DM6Bc9Nb1",
        "niV","9hFCW2R1","sHKHpe2i96","p7c5E6AcXQ/IJUuAEC9W6","",
        "aRv9hjc9P+Pbn+u3krN6","BzStcgE8qVdqjEH16l4","SqgeZvL5j9zoHP95xWHt","zVof5yaJkPe3VFpadPof"
    ]

    def __init__(self, username, password, device_id):
        self.username  = username
        self.password  = password
        self.device_id = device_id or uuid.uuid4().hex

    def _captcha_sign(self, timestamp):
        base = self.CLIENT_ID + self.CLIENT_VERSION + self.PACKAGE_NAME + self.device_id + timestamp
        for s in self.SALTS:
            base = hashlib.md5((base + s).encode()).hexdigest()
        return "1." + base

    def _build_user_agent(self):
        return (f"ANDROID-{self.PACKAGE_NAME}/{self.CLIENT_VERSION} protocolVersion/200 accesstype/ "
                f"clientid/{self.CLIENT_ID} clientversion/{self.CLIENT_VERSION} networktype/WIFI "
                f"deviceid/{self.device_id} devicename/Redmi devicemodel/M2004J7AC "
                f"osversion/13 sdkversion/{self.SDK_VERSION} ")

    async def _captcha_init(self):
        ts = str(int(time.time() * 1000))
        payload = {
            "client_id": self.CLIENT_ID,
            "action":    f"POST:{self.USER_API}/v1/auth/signin",
            "device_id": self.device_id,
            "meta": {
                "username":       self.username,
                "captcha_sign":   self._captcha_sign(ts),
                "client_version": self.CLIENT_VERSION,
                "package_name":   self.PACKAGE_NAME,
                "timestamp":      ts,
                "user_id":        "",
            },
        }
        headers = {
            "Content-Type": "application/json",
            "User-Agent":   self._build_user_agent(),
            "X-Device-Id":  self.device_id,
        }
        code, data, _ = await HttpClient.request("POST", f"{self.USER_API}/v1/shield/captcha/init", headers=headers, json_data=payload)
        return (data or {}).get("captcha_token", "")

    async def login(self):
        captcha_token = await self._captcha_init()
        if not captcha_token: return None
        form_data = {
            "client_id":     self.CLIENT_ID,
            "client_secret": self.CLIENT_SECRET,
            "username":      self.username,
            "password":      self.password,
            "captcha_token": captcha_token,
        }
        headers = {
            "Content-Type":    "application/x-www-form-urlencoded",
            "User-Agent":      self._build_user_agent(),
            "X-Device-Id":     self.device_id,
            "X-Captcha-Token": captcha_token,
        }
        try:
            conn = aiohttp.TCPConnector(verify_ssl=False)
            async with aiohttp.ClientSession(connector=conn) as session:
                async with session.post(f"{self.USER_API}/v1/auth/signin", data=form_data, headers=headers, timeout=20) as resp:
                    data = await resp.json()
                    if "refresh_token" not in data: return None
                    return {
                        "access_token":  data["access_token"],
                        "refresh_token": data["refresh_token"],
                        "user_id":       data.get("sub", ""),
                        "device_id":     self.device_id,
                    }
        except Exception: return None


class PikPakAPI:
    BASE_URL = "https://api-drive.mypikpak.com"
    AUTH_URL = "https://user.mypikpak.com"

    def __init__(self):
        self.access_token = None
        self.headers      = {}

    async def refresh_token(self):
        Config.load_config()
        if not Config.REFRESH_TOKEN:
            console.print(f"[bold red]{Language.get('token_missing')}[/]")
            return False

        ua = (f"ANDROID-{PikPakLogin.PACKAGE_NAME}/{PikPakLogin.CLIENT_VERSION} protocolVersion/200 accesstype/ "
              f"clientid/{PikPakLogin.CLIENT_ID} clientversion/{PikPakLogin.CLIENT_VERSION} action_type/ networktype/WIFI "
              f"sessionid/ deviceid/{Config.DEVICE_ID} providername/NONE refresh_token/ "
              f"sdkversion/{PikPakLogin.SDK_VERSION} datetime/{int(time.time()*1000)} usrno/ "
              f"appname/{PikPakLogin.PACKAGE_NAME} session_origin/ grant_type/ appid/ clientip/ "
              f"devicename/Xiaomi osversion/13 platformversion/10 accessmode/ devicemodel/M2004J7AC")
        headers = {"User-Agent": ua, "X-Device-Id": Config.DEVICE_ID, "Content-Type": "application/x-www-form-urlencoded"}
        form = {"client_id": PikPakLogin.CLIENT_ID, "client_secret": PikPakLogin.CLIENT_SECRET, "grant_type": "refresh_token", "refresh_token": Config.REFRESH_TOKEN}
        try:
            conn = aiohttp.TCPConnector(verify_ssl=False)
            async with aiohttp.ClientSession(connector=conn) as session:
                async with session.post(f"{self.AUTH_URL}/v1/auth/token", data=form, headers=headers, timeout=15) as resp:
                    data = await resp.json()
                    if "access_token" not in data: return False
                    self.access_token = data["access_token"]
                    self.headers = {"Authorization": f"Bearer {self.access_token}", "x-device-id": Config.DEVICE_ID}
                    return True
        except Exception: return False

    async def get_share_info(self, share_id, password):
        if Config.USE_CACHE:
            cached = CacheManager.get("share_info", share_id, password)
            if cached: return cached['files'], cached['pass_code_token']
        all_files = []; next_token = None; data = None
        with console.status(f"[cyan]{Language.get('analyzing')}", spinner="dots"):
            while True:
                params = {"share_id": share_id, "pass_code": password, "limit": "100"}
                if next_token: params["page_token"] = next_token
                code, data, _ = await HttpClient.request("GET", f"{self.BASE_URL}/drive/v1/share", headers=self.headers, params=params)
                if not data or "files" not in data: break
                all_files.extend(data.get("files", []))
                next_token = data.get("next_page_token")
                if not next_token: break
        pass_token = (data or {}).get("pass_code_token", "")
        if Config.USE_CACHE and all_files:
            CacheManager.set("share_info", {'files': all_files, 'pass_code_token': pass_token}, share_id, password, duration=1800)
        return all_files, pass_token

    async def get_folder_files(self, share_id, parent_id, pass_token):
        if Config.USE_CACHE:
            cached = CacheManager.get("folder_files", share_id, parent_id, pass_token)
            if cached: return cached
        all_files = []; next_token = None
        while True:
            params = {"share_id": share_id, "parent_id": parent_id, "pass_code_token": pass_token, "limit": "100"}
            if next_token: params["page_token"] = next_token
            code, data, _ = await HttpClient.request("GET", f"{self.BASE_URL}/drive/v1/share/detail", headers=self.headers, params=params)
            if not data: break
            all_files.extend(data.get("files", []))
            next_token = data.get("next_page_token")
            if not next_token: break
        if Config.USE_CACHE and all_files:
            CacheManager.set("folder_files", all_files, share_id, parent_id, pass_token, duration=1800)
        return all_files

    async def get_download_url(self, share_id, file_id, pass_token):
        params = {"share_id": share_id, "file_id": file_id, "pass_code_token": pass_token}
        code, data, _ = await HttpClient.request("GET", f"{self.BASE_URL}/drive/v1/share/file_info", headers=self.headers, params=params)
        if not data: return None
        info = data.get("file_info", {})
        if info.get("download_url"): return info["download_url"]
        if info.get("web_content_link"): return info["web_content_link"]
        for m in info.get("medias", []):
            url = m.get("link", {}).get("url")
            if url: return url
        return None

    async def get_root_folder_id(self) -> str:
        code, data, _ = await HttpClient.request("GET", f"{self.BASE_URL}/drive/v1/files", headers=self.headers, params={"parent_id": "root", "limit": "1", "with_audit": "false"})
        if data:
            files = data.get("files", [])
            if files and files[0].get("parent_id"): return files[0]["parent_id"]
        code2, data2, _ = await HttpClient.request("GET", f"{self.BASE_URL}/drive/v1/about", headers=self.headers)
        if data2:
            root_id = (data2.get("quota", {}).get("root_id") or data2.get("drive", {}).get("root_id"))
            if root_id: return root_id
        return "root"

    async def restore_and_poll(self, share_id, file_id, pass_token):
        root_id = await self.get_root_folder_id()
        payload = {"share_id": share_id, "pass_code_token": pass_token, "file_ids": [file_id], "to_parent_id": root_id, "params": {"trace_file_ids": file_id}}
        code, data, _ = await HttpClient.request("POST", f"{self.BASE_URL}/drive/v1/share/restore", headers=self.headers, json_data=payload)
        if code != 200 or not data: return None, (data or {}).get("error", "no_response")
        task_id = data.get("restore_task_id") or data.get("task_id")
        if not task_id: return None, "no_task_id"
        for attempt in range(60):
            await asyncio.sleep(2)
            code, tdata, _ = await HttpClient.request("GET", f"{self.BASE_URL}/drive/v1/tasks/{task_id}", headers=self.headers)
            if code != 200 or not tdata: continue
            phase = tdata.get("phase", "")
            if phase == "PHASE_TYPE_ERROR": return None, tdata.get("message", "task_error")
            if phase == "PHASE_TYPE_COMPLETE":
                new_id = self._parse_new_file_id(tdata, file_id)
                return (new_id, None) if new_id else (None, "parse_failed")
        return None, "timeout"

    def _parse_new_file_id(self, task_data: dict, original_file_id: str):
        params_obj = task_data.get("params", {})
        trace = params_obj.get("trace_file_ids")
        if trace:
            try:
                trace_map = json.loads(trace) if isinstance(trace, str) else trace
                if isinstance(trace_map, dict):
                    new_id = trace_map.get(original_file_id)
                    if new_id: return new_id
            except: pass
        file_ids = params_obj.get("file_ids")
        if file_ids:
            try:
                ids = json.loads(file_ids) if isinstance(file_ids, str) else file_ids
                if isinstance(ids, list) and ids: return ids[0]
            except: pass
        direct = params_obj.get("file_id")
        if direct: return direct
        created = task_data.get("created_file_ids")
        if created and isinstance(created, list) and created: return created[0]
        return None

    async def wait_for_file(self, filename: str, max_retries: int = 20):
        for attempt in range(max_retries):
            await asyncio.sleep(2)
            filters = json.dumps({"name": {"eq": filename}, "trashed": {"eq": False}})
            code, data, _ = await HttpClient.request("GET", f"{self.BASE_URL}/drive/v1/files", headers=self.headers, params={"thumbnail_size": "SIZE_MEDIUM", "limit": 20, "with_audit": "true", "filters": filters, "order_by": "modified_time", "sort": "desc"})
            if data and data.get("files"):
                for f in data["files"]:
                    if f.get("name") == filename and not f.get("trashed", False): return f["id"]
            code2, data2, _ = await HttpClient.request("GET", f"{self.BASE_URL}/drive/v1/files", headers=self.headers, params={"thumbnail_size": "SIZE_MEDIUM", "limit": 50, "with_audit": "true", "order_by": "modified_time", "sort": "desc"})
            if data2 and data2.get("files"):
                for f in data2["files"]:
                    if f.get("name") == filename and not f.get("trashed", False): return f["id"]
        return None

    async def get_user_file_url(self, file_id: str):
        for attempt in range(5):
            code, data, _ = await HttpClient.request("GET", f"{self.BASE_URL}/drive/v1/files/{file_id}", headers=self.headers, params={"usage": "FETCH"})
            if data:
                url = (data.get("links", {}).get("application/octet-stream", {}).get("url") or data.get("web_content_link") or data.get("download_url"))
                if url: return url
                medias = data.get("medias", [])
                if medias:
                    url = medias[0].get("link", {}).get("url")
                    if url: return url
            await asyncio.sleep(1)
        return None

    async def delete_file(self, file_id: str):
        code, _, _ = await HttpClient.request("POST", f"{self.BASE_URL}/drive/v1/files:batchDelete", headers=self.headers, json_data={"ids": [file_id]})
        return code == 200

class TreeBuilder:
    def __init__(self, api): self.api = api

    async def build_tree(self, files, parent, share_id, pass_token):
        folders = []; file_list = []
        for f in files:
            name    = f.get("name", "Unknown")
            file_id = f.get("id")
            kind    = f.get("kind", "")
            size    = int(f.get("size", 0)) if f.get("size") else 0
            if kind == "drive#folder":
                folder_path = f"{parent}/{name}".strip("/")
                sub_files   = await self.api.get_folder_files(share_id, file_id, pass_token)
                children    = await self.build_tree(sub_files, folder_path, share_id, pass_token)
                folders.append({"type": "folder", "name": name, "path": folder_path, "folders": children["folders"], "files": children["files"]})
            elif kind == "drive#file":
                file_list.append({"type": "file", "name": name, "id": file_id, "path": f"{parent}/{name}".strip("/"), "size": size})
        return {"folders": folders, "files": file_list}