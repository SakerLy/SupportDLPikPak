import hashlib
import time
import uuid
import json
import asyncio
from config.settings import Config, console, Language
from core.utils import HttpClient, CacheManager
from core.logger import logger


class PikPakLogin:
    USER_API = "https://user.mypikpak.com"
    CLIENT_ID = "YNxT9w7GMdWvEOKa"
    CLIENT_SECRET = "dbw2OtmVEeuUvIptb1Coyg"
    CLIENT_VERSION = "1.47.1"
    PACKAGE_NAME = "com.pikcloud.pikpak"
    SDK_VERSION = "8.1.4"
    SALTS = [
        "Gez0T9ijiI9WCeTsKSg3SMlx",
        "zQdbalsolyb1R/",
        "ftOjr52zt51JD68C3s",
        "yeOBMH0JkbQdEFNNwQ0RI9T3wU/v",
        "BRJrQZiTQ65WtMvwO",
        "je8fqxKPdQVJiy1DM6Bc9Nb1",
        "niV",
        "9hFCW2R1",
        "sHKHpe2i96",
        "p7c5E6AcXQ/IJUuAEC9W6",
        "",
        "aRv9hjc9P+Pbn+u3krN6",
        "BzStcgE8qVdqjEH16l4",
        "SqgeZvL5j9zoHP95xWHt",
        "zVof5yaJkPe3VFpadPof",
    ]

    def __init__(self, username, password, device_id):
        self.username = username
        self.password = password
        self.device_id = device_id or uuid.uuid4().hex

    def _captcha_sign(self, timestamp):
        base = (
            self.CLIENT_ID
            + self.CLIENT_VERSION
            + self.PACKAGE_NAME
            + self.device_id
            + timestamp
        )
        for s in self.SALTS:
            base = hashlib.md5((base + s).encode()).hexdigest()
        return "1." + base

    def _build_user_agent(self):
        return (
            f"ANDROID-{self.PACKAGE_NAME}/{self.CLIENT_VERSION} protocolVersion/200 accesstype/ "
            f"clientid/{self.CLIENT_ID} clientversion/{self.CLIENT_VERSION} networktype/WIFI "
            f"deviceid/{self.device_id} devicename/Redmi devicemodel/M2004J7AC "
            f"osversion/13 sdkversion/{self.SDK_VERSION} "
        )

    async def _captcha_init(self):
        ts = str(int(time.time() * 1000))
        payload = {
            "client_id": self.CLIENT_ID,
            "action": f"POST:{self.USER_API}/v1/auth/signin",
            "device_id": self.device_id,
            "meta": {
                "username": self.username,
                "captcha_sign": self._captcha_sign(ts),
                "client_version": self.CLIENT_VERSION,
                "package_name": self.PACKAGE_NAME,
                "timestamp": ts,
                "user_id": "",
            },
        }
        headers = {
            "Content-Type": "application/json",
            "User-Agent": self._build_user_agent(),
            "X-Device-Id": self.device_id,
        }
        code, data, _ = await HttpClient.request(
            "POST",
            f"{self.USER_API}/v1/shield/captcha/init",
            headers=headers,
            json_data=payload,
        )
        return (data or {}).get("captcha_token", "")

    async def login(self):
        captcha_token = await self._captcha_init()
        if not captcha_token:
            return None
        form_data = {
            "client_id": self.CLIENT_ID,
            "client_secret": self.CLIENT_SECRET,
            "username": self.username,
            "password": self.password,
            "captcha_token": captcha_token,
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": self._build_user_agent(),
            "X-Device-Id": self.device_id,
            "X-Captcha-Token": captcha_token,
        }
        code, data, _ = await HttpClient.request(
            "POST",
            f"{self.USER_API}/v1/auth/signin",
            headers=headers,
            form_data=form_data,
            timeout=20,
        )
        if not data or "refresh_token" not in data:
            return None
        return {
            "access_token": data["access_token"],
            "refresh_token": data["refresh_token"],
            "user_id": data.get("sub", ""),
            "device_id": self.device_id,
        }


class PikPakAPI:
    BASE_URL = "https://api-drive.mypikpak.com"
    AUTH_URL = "https://user.mypikpak.com"
    TOKEN_TTL = 20 * 60

    def __init__(
        self, refresh_token: str = None, device_id: str = None, on_token_update=None
    ):
        # refresh_token=None => instance đại diện account chính, đọc credentials từ Config
        self._is_main = refresh_token is None
        self._refresh_token = refresh_token
        self.device_id = device_id
        self.on_token_update = on_token_update
        self.access_token = None
        self.headers = {}
        self._token_time = 0.0
        self._root_id = None

    def _sync_main_credentials(self):
        Config.load_config()
        self._refresh_token = Config.REFRESH_TOKEN
        self.device_id = Config.DEVICE_ID

    def _persist_new_token(self, new_token: str):
        if new_token == self._refresh_token:
            return
        self._refresh_token = new_token
        try:
            if self._is_main:
                Config.REFRESH_TOKEN = new_token
                Config.save_config()
            elif self.on_token_update:
                self.on_token_update(new_token)
        except Exception:
            logger.exception("Không thể lưu refresh token mới")

    def token_valid(self) -> bool:
        return (
            bool(self.access_token)
            and (time.time() - self._token_time) < self.TOKEN_TTL
        )

    async def ensure_token(self) -> bool:
        return True if self.token_valid() else await self.refresh_token()

    async def refresh_token(self) -> bool:
        if self._is_main:
            self._sync_main_credentials()
        if not self._refresh_token:
            console.print(f"[bold red]{Language.get('token_missing')}[/]")
            return False

        ua = (
            f"ANDROID-{PikPakLogin.PACKAGE_NAME}/{PikPakLogin.CLIENT_VERSION} protocolVersion/200 accesstype/ "
            f"clientid/{PikPakLogin.CLIENT_ID} clientversion/{PikPakLogin.CLIENT_VERSION} action_type/ networktype/WIFI "
            f"sessionid/ deviceid/{self.device_id} providername/NONE refresh_token/ "
            f"sdkversion/{PikPakLogin.SDK_VERSION} datetime/{int(time.time() * 1000)} usrno/ "
            f"appname/{PikPakLogin.PACKAGE_NAME} session_origin/ grant_type/ appid/ clientip/ "
            f"devicename/Xiaomi osversion/13 platformversion/10 accessmode/ devicemodel/M2004J7AC"
        )
        headers = {
            "User-Agent": ua,
            "X-Device-Id": self.device_id,
            "Content-Type": "application/x-www-form-urlencoded",
        }
        form = {
            "client_id": PikPakLogin.CLIENT_ID,
            "client_secret": PikPakLogin.CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": self._refresh_token,
        }
        code, data, text = await HttpClient.request(
            "POST",
            f"{self.AUTH_URL}/v1/auth/token",
            headers=headers,
            form_data=form,
            timeout=15,
        )
        if not data or "access_token" not in data:
            logger.error(
                "Refresh token thất bại (HTTP %s): %s", code, (text or "")[:200]
            )
            return False

        self.access_token = data["access_token"]
        self._token_time = time.time()
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "x-device-id": self.device_id,
        }
        if data.get("refresh_token"):
            self._persist_new_token(data["refresh_token"])
        return True

    async def get_share_info(self, share_id, password):
        if Config.USE_CACHE:
            cached = CacheManager.get("share_info", share_id, password)
            if cached:
                return cached["files"], cached["pass_code_token"]
        all_files = []
        next_token = None
        data = None
        with console.status(f"[cyan]{Language.get('analyzing')}", spinner="dots"):
            while True:
                params = {"share_id": share_id, "pass_code": password, "limit": "100"}
                if next_token:
                    params["page_token"] = next_token
                code, data, _ = await HttpClient.request(
                    "GET",
                    f"{self.BASE_URL}/drive/v1/share",
                    headers=self.headers,
                    params=params,
                )
                if not data or "files" not in data:
                    break
                all_files.extend(data.get("files", []))
                next_token = data.get("next_page_token")
                if not next_token:
                    break
        pass_token = (data or {}).get("pass_code_token", "")
        if Config.USE_CACHE and all_files:
            CacheManager.set(
                "share_info",
                {"files": all_files, "pass_code_token": pass_token},
                share_id,
                password,
                duration=1800,
            )
        return all_files, pass_token

    async def get_folder_files(self, share_id, parent_id, pass_token):
        if Config.USE_CACHE:
            cached = CacheManager.get("folder_files", share_id, parent_id, pass_token)
            if cached:
                return cached
        all_files = []
        next_token = None
        while True:
            params = {
                "share_id": share_id,
                "parent_id": parent_id,
                "pass_code_token": pass_token,
                "limit": "100",
            }
            if next_token:
                params["page_token"] = next_token
            code, data, _ = await HttpClient.request(
                "GET",
                f"{self.BASE_URL}/drive/v1/share/detail",
                headers=self.headers,
                params=params,
            )
            if not data:
                break
            all_files.extend(data.get("files", []))
            next_token = data.get("next_page_token")
            if not next_token:
                break
        if Config.USE_CACHE and all_files:
            CacheManager.set(
                "folder_files",
                all_files,
                share_id,
                parent_id,
                pass_token,
                duration=1800,
            )
        return all_files

    async def get_download_url(self, share_id, file_id, pass_token):
        params = {
            "share_id": share_id,
            "file_id": file_id,
            "pass_code_token": pass_token,
        }
        code, data, _ = await HttpClient.request(
            "GET",
            f"{self.BASE_URL}/drive/v1/share/file_info",
            headers=self.headers,
            params=params,
        )
        if not data:
            return None
        info = data.get("file_info", {})
        if info.get("download_url"):
            return info["download_url"]
        if info.get("web_content_link"):
            return info["web_content_link"]
        for m in info.get("medias", []):
            url = m.get("link", {}).get("url")
            if url:
                return url
        return None

    async def get_root_folder_id(self) -> str:
        if self._root_id:
            return self._root_id
        code, data, _ = await HttpClient.request(
            "GET",
            f"{self.BASE_URL}/drive/v1/files",
            headers=self.headers,
            params={"parent_id": "root", "limit": "1", "with_audit": "false"},
        )
        if data:
            files = data.get("files", [])
            if files and files[0].get("parent_id"):
                self._root_id = files[0]["parent_id"]
                return self._root_id
        code2, data2, _ = await HttpClient.request(
            "GET", f"{self.BASE_URL}/drive/v1/about", headers=self.headers
        )
        if data2:
            root_id = data2.get("quota", {}).get("root_id") or data2.get(
                "drive", {}
            ).get("root_id")
            if root_id:
                self._root_id = root_id
                return root_id
        return "root"

    @staticmethod
    def _extract_error(data: dict, fallback: str) -> str:
        if not isinstance(data, dict):
            return fallback
        parts = [
            str(data.get(k))
            for k in ("error", "error_description", "message")
            if data.get(k)
        ]
        return " | ".join(parts) if parts else fallback

    async def restore_and_poll(self, share_id, file_id, pass_token):
        root_id = await self.get_root_folder_id()
        payload = {
            "share_id": share_id,
            "pass_code_token": pass_token,
            "file_ids": [file_id],
            "to_parent_id": root_id,
            "params": {"trace_file_ids": file_id},
        }
        code, data, _ = await HttpClient.request(
            "POST",
            f"{self.BASE_URL}/drive/v1/share/restore",
            headers=self.headers,
            json_data=payload,
        )
        if code != 200 or not data:
            return None, self._extract_error(data, f"http_{code}")
        task_id = data.get("restore_task_id") or data.get("task_id")
        if not task_id:
            return None, "no_task_id"
        for attempt in range(60):
            await asyncio.sleep(2)
            code, tdata, _ = await HttpClient.request(
                "GET", f"{self.BASE_URL}/drive/v1/tasks/{task_id}", headers=self.headers
            )
            if code != 200 or not tdata:
                continue
            phase = tdata.get("phase", "")
            if phase == "PHASE_TYPE_ERROR":
                return None, self._extract_error(tdata, "task_error")
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
                    if new_id:
                        return new_id
            except Exception:
                pass
        file_ids = params_obj.get("file_ids")
        if file_ids:
            try:
                ids = json.loads(file_ids) if isinstance(file_ids, str) else file_ids
                if isinstance(ids, list) and ids:
                    return ids[0]
            except Exception:
                pass
        direct = params_obj.get("file_id")
        if direct:
            return direct
        created = task_data.get("created_file_ids")
        if created and isinstance(created, list) and created:
            return created[0]
        return None

    def _match_file(self, f: dict, filename: str, expected_size) -> bool:
        if f.get("name") != filename or f.get("trashed", False):
            return False
        if expected_size is None:
            return True
        try:
            return int(f.get("size", -1)) == int(expected_size)
        except (TypeError, ValueError):
            return False

    async def wait_for_file(
        self, filename: str, max_retries: int = 20, expected_size=None
    ):
        for attempt in range(max_retries):
            await asyncio.sleep(2)
            filters = json.dumps({"name": {"eq": filename}, "trashed": {"eq": False}})
            code, data, _ = await HttpClient.request(
                "GET",
                f"{self.BASE_URL}/drive/v1/files",
                headers=self.headers,
                params={
                    "thumbnail_size": "SIZE_MEDIUM",
                    "limit": 20,
                    "with_audit": "true",
                    "filters": filters,
                    "order_by": "modified_time",
                    "sort": "desc",
                },
            )
            if data:
                for f in data.get("files", []):
                    if self._match_file(f, filename, expected_size):
                        return f["id"]
            code2, data2, _ = await HttpClient.request(
                "GET",
                f"{self.BASE_URL}/drive/v1/files",
                headers=self.headers,
                params={
                    "thumbnail_size": "SIZE_MEDIUM",
                    "limit": 50,
                    "with_audit": "true",
                    "order_by": "modified_time",
                    "sort": "desc",
                },
            )
            if data2:
                for f in data2.get("files", []):
                    if self._match_file(f, filename, expected_size):
                        return f["id"]
        return None

    async def get_user_file_url(self, file_id: str):
        for attempt in range(5):
            code, data, _ = await HttpClient.request(
                "GET",
                f"{self.BASE_URL}/drive/v1/files/{file_id}",
                headers=self.headers,
                params={"usage": "FETCH"},
            )
            if data:
                url = (
                    data.get("links", {}).get("application/octet-stream", {}).get("url")
                    or data.get("web_content_link")
                    or data.get("download_url")
                )
                if url:
                    return url
                medias = data.get("medias", [])
                if medias:
                    url = medias[0].get("link", {}).get("url")
                    if url:
                        return url
            await asyncio.sleep(1)
        return None

    async def delete_file(self, file_id: str):
        code, _, _ = await HttpClient.request(
            "POST",
            f"{self.BASE_URL}/drive/v1/files:batchDelete",
            headers=self.headers,
            json_data={"ids": [file_id]},
        )
        return code == 200


class TreeBuilder:
    def __init__(self, api):
        self.api = api

    async def build_tree(self, files, parent, share_id, pass_token):
        folders = []
        file_list = []
        for f in files:
            name = f.get("name", "Unknown")
            file_id = f.get("id")
            kind = f.get("kind", "")
            size = int(f.get("size", 0)) if f.get("size") else 0
            if kind == "drive#folder":
                folder_path = f"{parent}/{name}".strip("/")
                sub_files = await self.api.get_folder_files(
                    share_id, file_id, pass_token
                )
                children = await self.build_tree(
                    sub_files, folder_path, share_id, pass_token
                )
                folders.append(
                    {
                        "type": "folder",
                        "name": name,
                        "path": folder_path,
                        "folders": children["folders"],
                        "files": children["files"],
                    }
                )
            elif kind == "drive#file":
                file_list.append(
                    {
                        "type": "file",
                        "name": name,
                        "id": file_id,
                        "path": f"{parent}/{name}".strip("/"),
                        "size": size,
                    }
                )
        return {"folders": folders, "files": file_list}
