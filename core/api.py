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
    USER_API = "https://user.mypikpak.com"; CLIENT_ID = "YNxT9w7GMdWvEOKa"; CLIENT_SECRET = "dbw2OtmVEeuUvIptb1Coyg"; CLIENT_VERSION = "1.47.1"; PACKAGE_NAME = "com.pikcloud.pikpak"
    SALTS = ["Gez0T9ijiI9WCeTsKSg3SMlx","zQdbalsolyb1R/","ftOjr52zt51JD68C3s","yeOBMH0JkbQdEFNNwQ0RI9T3wU/v","BRJrQZiTQ65WtMvwO","je8fqxKPdQVJiy1DM6Bc9Nb1","niV","9hFCW2R1","sHKHpe2i96","p7c5E6AcXQ/IJUuAEC9W6","","aRv9hjc9P+Pbn+u3krN6","BzStcgE8qVdqjEH16l4","SqgeZvL5j9zoHP95xWHt","zVof5yaJkPe3VFpadPof"]
    def __init__(self, username, password, device_id): self.username = username; self.password = password; self.device_id = device_id or uuid.uuid4().hex
    def _captcha_sign(self, timestamp):
        base = (self.CLIENT_ID + self.CLIENT_VERSION + self.PACKAGE_NAME + self.device_id + timestamp)
        for s in self.SALTS: base = hashlib.md5((base + s).encode()).hexdigest()
        return "1." + base
    def _build_user_agent(self): return f"ANDROID-{self.PACKAGE_NAME}/{self.CLIENT_VERSION} protocolVersion/200 accesstype/ clientid/{self.CLIENT_ID} clientversion/{self.CLIENT_VERSION} networktype/WIFI deviceid/{self.device_id} devicename/Redmi devicemodel/M2004J7AC osversion/13 sdkversion/2.0.4.204000 "
    async def _captcha_init(self):
        ts = str(int(time.time() * 1000))
        payload = {"client_id": self.CLIENT_ID, "action": f"POST:{self.USER_API}/v1/auth/signin", "device_id": self.device_id, "meta": {"username": self.username, "captcha_sign": self._captcha_sign(ts), "client_version": self.CLIENT_VERSION, "package_name": self.PACKAGE_NAME, "timestamp": ts, "user_id": ""}}
        headers = {"Content-Type": "application/json", "User-Agent": self._build_user_agent(), "X-Device-Id": self.device_id}
        code, data, raw = await HttpClient.request("POST", f"{self.USER_API}/v1/shield/captcha/init", headers=headers, json_data=payload)
        return data.get("captcha_token", "")
    async def login(self):
        captcha_token = await self._captcha_init()
        if not captcha_token: return None
        form_data = {"client_id": self.CLIENT_ID, "client_secret": self.CLIENT_SECRET, "username": self.username, "password": self.password, "captcha_token": captcha_token}
        headers = {"Content-Type": "application/x-www-form-urlencoded", "User-Agent": self._build_user_agent(), "X-Device-Id": self.device_id, "X-Captcha-Token": captcha_token}
        try:
            connector = aiohttp.TCPConnector(verify_ssl=False)
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.post(f"{self.USER_API}/v1/auth/signin", data=form_data, headers=headers, timeout=20) as resp:
                    data = await resp.json()
                    if "refresh_token" not in data: return None
                    return {"access_token": data["access_token"], "refresh_token": data["refresh_token"], "user_id": data.get("sub", ""), "device_id": self.device_id}
        except: return None


class PikPakAPI:
    BASE_URL = "https://api-drive.mypikpak.com"
    AUTH_URL = "https://user.mypikpak.com"

    def __init__(self):
        self.access_token = None
        self.headers = {}

    async def refresh_token(self):
        Config.load_config()
        if not Config.REFRESH_TOKEN:
            console.print(f"[bold red]{Language.get('token_missing')}[/]")
            logger.warning("Refresh token missing during refresh_token call")
            return False

        ua = f"ANDROID-com.pikcloud.pikpak/1.47.1 protocolVersion/200 accesstype/ clientid/YNxT9w7GMdWvEOKa clientversion/1.47.1 action_type/ networktype/WIFI sessionid/ deviceid/{Config.DEVICE_ID} providername/NONE refresh_token/ sdkversion/2.0.4.204000 datetime/{int(time.time()*1000)} usrno/ appname/com.pikcloud.pikpak session_origin/ grant_type/ appid/ clientip/ devicename/Xiaomi osversion/13 platformversion/10 accessmode/ devicemodel/M2004J7AC"
        headers = {"User-Agent": ua, "X-Device-Id": Config.DEVICE_ID, "Content-Type": "application/x-www-form-urlencoded"}
        form = {"client_id": PikPakLogin.CLIENT_ID, "client_secret": PikPakLogin.CLIENT_SECRET, "grant_type": "refresh_token", "refresh_token": Config.REFRESH_TOKEN}

        try:
            connector = aiohttp.TCPConnector(verify_ssl=False)
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.post(f"{self.AUTH_URL}/v1/auth/token", data=form, headers=headers, timeout=15) as resp:
                    data = await resp.json()
                    if "access_token" not in data:
                        logger.error("Token refresh response missing access_token: %s", data)
                        return False
                    self.access_token = data["access_token"]
                    self.headers = {"Authorization": f"Bearer {self.access_token}", "x-device-id": Config.DEVICE_ID}
                    logger.debug("Access token refreshed successfully")
                    return True
        except Exception as e:
            logger.exception("Failed to refresh PikPak token")
            return False

    async def get_share_info(self, share_id, password):
        logger.info("Fetching share info for share_id=%s", share_id)
        if Config.USE_CACHE:
            cached = CacheManager.get("share_info", share_id, password)
            if cached:
                logger.debug("Share info cache hit for share_id=%s", share_id)
                return cached['files'], cached['pass_code_token']
        all_files = []; next_token = None
        with console.status(f"[cyan]{Language.get('analyzing')}", spinner="dots"):
            while True:
                params = {"share_id": share_id, "pass_code": password, "limit": "100"}
                if next_token: params["page_token"] = next_token
                code, data, _ = await HttpClient.request("GET", f"{self.BASE_URL}/drive/v1/share", headers=self.headers, params=params)
                if not data or "files" not in data: break
                all_files.extend(data.get("files", []))
                next_token = data.get("next_page_token")
                if not next_token: break
        pass_token = data.get("pass_code_token", "") if data else ""
        if Config.USE_CACHE and all_files: CacheManager.set("share_info", {'files': all_files, 'pass_code_token': pass_token}, share_id, password, duration=1800)
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
        if Config.USE_CACHE and all_files: CacheManager.set("folder_files", all_files, share_id, parent_id, pass_token, duration=1800)
        return all_files

    async def get_download_url(self, share_id, file_id, pass_token):
        params = {"share_id": share_id, "file_id": file_id, "pass_code_token": pass_token}
        code, data, raw = await HttpClient.request("GET", f"{self.BASE_URL}/drive/v1/share/file_info", headers=self.headers, params=params)
        if not data:
            logger.warning("No data returned for get_download_url share_id=%s file_id=%s", share_id, file_id)
            return None
        info = data.get("file_info", {})
        if info.get("download_url"): return info["download_url"]
        if info.get("web_content_link"): return info["web_content_link"]
        for m in info.get("medias", []):
            if m.get("link", {}).get("url"): return m["link"]["url"]
        return None

    async def get_root_folder_id(self) -> str:
        """
        Lấy ID thư mục root trong drive.
        PikPak yêu cầu to_parent_id phải là ID thực khi restore.

        Thử 3 cách theo thứ tự:
          1. Query files với parent_id="root" → lấy parent_id từ file trả về
          2. Query /about → quota.root_id
          3. Fallback: string "root" (PikPak hầu hết version chấp nhận)
        """
        # Cách 1: lấy từ file bất kỳ trong root
        code, data, _ = await HttpClient.request(
            "GET", f"{self.BASE_URL}/drive/v1/files",
            headers=self.headers,
            params={"parent_id": "root", "limit": "1", "with_audit": "false"}
        )
        if data:
            files = data.get("files", [])
            if files and files[0].get("parent_id"):
                return files[0]["parent_id"]

        # Cách 2: /about endpoint
        code2, data2, _ = await HttpClient.request(
            "GET", f"{self.BASE_URL}/drive/v1/about",
            headers=self.headers
        )
        if data2:
            root_id = (data2.get("quota", {}).get("root_id")
                       or data2.get("drive", {}).get("root_id"))
            if root_id:
                return root_id

        # Cách 3: "root" literal — nhiều version PikPak API chấp nhận
        return "root"

    async def restore_and_poll(self, share_id, file_id, pass_token):
        """
        Restore file từ share vào drive của mình.
        Trả về (file_id mới, error_message) tuple.
        Nếu success: (file_id, None)
        Nếu fail: (None, error_message)
        """
        # Lấy root folder ID — to_parent_id rỗng có thể bị server reject
        logger.info("Restoring shared file %s from share %s", file_id, share_id)
        root_id = await self.get_root_folder_id()

        payload = {
            "share_id":        share_id,
            "pass_code_token": pass_token,
            "file_ids":        [file_id],
            "to_parent_id":    root_id,          # phải là ID thực, không để rỗng
            "params":          {"trace_file_ids": file_id},
        }
        code, data, raw_text = await HttpClient.request(
            "POST", f"{self.BASE_URL}/drive/v1/share/restore",
            headers=self.headers, json_data=payload
        )

        if code != 200 or not data:
            error = data.get("error", "unknown") if data else "no_response"
            logger.error("Restore request failed for file %s share %s code=%s error=%s", file_id, share_id, code, error)
            return None, error

        task_id = data.get("restore_task_id") or data.get("task_id")
        if not task_id:
            error = "no_task_id"
            logger.error("No task ID in restore response for file %s", file_id)
            return None, error

        # Poll task — tối đa 60 lần × 2s = 120 giây
        for attempt in range(60):
            await asyncio.sleep(2)
            logger.debug("Polling restore status for task %s attempt %s", task_id, attempt + 1)
            code, tdata, _ = await HttpClient.request(
                "GET", f"{self.BASE_URL}/drive/v1/tasks/{task_id}",
                headers=self.headers
            )
            if code != 200 or not tdata:
                continue

            phase = tdata.get("phase", "")

            if phase == "PHASE_TYPE_ERROR":
                error = tdata.get("message", "task_error")
                logger.error("Restore task failed for file %s: %s", file_id, error)
                return None, error

            if phase == "PHASE_TYPE_COMPLETE":
                new_id = self._parse_new_file_id(tdata, file_id)
                if new_id:
                    return new_id, None
                else:
                    error = "parse_failed"
                    logger.error("Failed to parse new file ID for file %s", file_id)
                    return None, error

            # Vẫn đang chạy — tiếp tục poll

        error = "timeout"
        logger.error("Restore task timeout for file %s", file_id)
        return None, error

    def _parse_new_file_id(self, task_data: dict, original_file_id: str):
        """
        Thử nhiều cách để lấy file_id mới sau khi restore hoàn thành.
        Trả về str nếu tìm được, None nếu không.
        """
        params_obj = task_data.get("params", {})

        # Cách 1: trace_file_ids là JSON string hoặc dict
        trace = params_obj.get("trace_file_ids")
        if trace:
            try:
                if isinstance(trace, str):
                    trace_map = json.loads(trace)
                else:
                    trace_map = trace
                if isinstance(trace_map, dict):
                    new_id = trace_map.get(original_file_id)
                    if new_id:
                        return new_id
            except Exception:
                pass

        # Cách 2: file_ids list trong params
        file_ids = params_obj.get("file_ids")
        if file_ids:
            try:
                if isinstance(file_ids, str):
                    ids = json.loads(file_ids)
                else:
                    ids = file_ids
                if isinstance(ids, list) and ids:
                    return ids[0]
            except Exception:
                pass

        # Cách 3: file_id trực tiếp trong params
        direct = params_obj.get("file_id")
        if direct:
            return direct

        # Cách 4: created_file_ids trong task root
        created = task_data.get("created_file_ids")
        if created and isinstance(created, list) and created:
            return created[0]

        return None

    async def wait_for_file(self, filename: str, max_retries: int = 20):
        """
        Poll danh sách file trong drive cho đến khi tìm thấy `filename`.
        Dùng 2 cách query song song để tăng khả năng tìm thấy:
          - filter by name (có thể lag index)
          - list recent files và so tên (luôn hoạt động)
        """
        logger.debug("Waiting for file to appear in drive: %s", filename)
        for attempt in range(max_retries):
            logger.debug("wait_for_file attempt %s for %s", attempt + 1, filename)
            await asyncio.sleep(2)

            # Cách 1: filter by name
            filters = json.dumps({
                "name":    {"eq": filename},
                "trashed": {"eq": False},
            })
            params = {
                "thumbnail_size": "SIZE_MEDIUM",
                "limit":          20,
                "with_audit":     "true",
                "filters":        filters,
                "order_by":       "modified_time",
                "sort":           "desc",
            }
            code, data, _ = await HttpClient.request(
                "GET", f"{self.BASE_URL}/drive/v1/files",
                headers=self.headers, params=params
            )
            if data and data.get("files"):
                for f in data["files"]:
                    if f.get("name") == filename and not f.get("trashed", False):
                        return f["id"]

            # Cách 2: list recent files (không dùng filter) và tìm theo tên
            # — bỏ qua cách 1 lag index
            params2 = {
                "thumbnail_size": "SIZE_MEDIUM",
                "limit":          50,
                "with_audit":     "true",
                "order_by":       "modified_time",
                "sort":           "desc",
            }
            code2, data2, _ = await HttpClient.request(
                "GET", f"{self.BASE_URL}/drive/v1/files",
                headers=self.headers, params=params2
            )
            if data2 and data2.get("files"):
                for f in data2["files"]:
                    if f.get("name") == filename and not f.get("trashed", False):
                        return f["id"]

        return None

    async def get_user_file_url(self, file_id: str):
        """Lấy download URL của file trong drive."""
        logger.debug("Requesting user file URL for drive file_id=%s", file_id)
        for attempt in range(5):
            code, data, _ = await HttpClient.request(
                "GET", f"{self.BASE_URL}/drive/v1/files/{file_id}",
                headers=self.headers, params={"usage": "FETCH"}
            )
            if data:
                if data.get("web_content_link"): return data["web_content_link"]
                if data.get("download_url"):     return data["download_url"]
                medias = data.get("medias", [])
                if medias:
                    url = medias[0].get("link", {}).get("url")
                    if url: return url
            await asyncio.sleep(1)
        return None


        # Cách 1: links["application/octet-stream"].url — URL đầy đủ nhất
        links = data.get("links", {})
        octet = links.get("application/octet-stream", {})
        url = octet.get("url")
        if url:
            return url

        # Cách 2: web_content_link — URL có token, không cần header
        url = data.get("web_content_link")
        if url:
            return url

        # Cách 3: download_url
        url = data.get("download_url")
        if url:
            return url

        return None


        info = data.get("file_info", {})

        # Ưu tiên links["application/octet-stream"]
        links = info.get("links", {})
        octet = links.get("application/octet-stream", {})
        url = octet.get("url")
        if url:
            return url

        # Fallback
        url = info.get("web_content_link") or info.get("download_url")
        if url:
            return url

        for m in info.get("medias", []):
            u = m.get("link", {}).get("url")
            if u:
                return u

        return None

    async def delete_file(self, file_id: str):
        logger.info("Deleting cloud file %s", file_id)
        url     = f"{self.BASE_URL}/drive/v1/files:batchDelete"
        payload = {"ids": [file_id]}
        code, data, _ = await HttpClient.request("POST", url, headers=self.headers, json_data=payload)
        if code != 200:
            logger.warning("Failed to delete cloud file %s, status=%s data=%s", file_id, code, data)
        return code == 200


class TreeBuilder:
    def __init__(self, api): self.api = api
    async def build_tree(self, files, parent, share_id, pass_token):
        folders = []; file_list = []
        for f in files:
            name = f.get("name", "Unknown"); file_id = f.get("id"); kind = f.get("kind", ""); size = int(f.get("size", 0)) if f.get("size") else 0
            if kind == "drive#folder":
                folder_path = f"{parent}/{name}".strip("/")
                sub_files = await self.api.get_folder_files(share_id, file_id, pass_token)
                children = await self.build_tree(sub_files, folder_path, share_id, pass_token)
                folders.append({"type": "folder", "name": name, "path": folder_path, "folders": children["folders"], "files": children["files"]})
            elif kind == "drive#file":
                file_path = f"{parent}/{name}".strip("/")
                file_list.append({"type": "file", "name": name, "id": file_id, "path": file_path, "size": size})
        return {"folders": folders, "files": file_list}