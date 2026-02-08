import hashlib
import time
import uuid
import json
from config.settings import Config, console, Language, VIP_USERNAME, VIP_PASSWORD
from core.utils import HttpClient, CacheManager

class PikPakLogin:
    USER_API = "https://user.mypikpak.com"; CLIENT_ID = "YNxT9w7GMdWvEOKa"; CLIENT_SECRET = "dbw2OtmVEeuUvIptb1Coyg"; CLIENT_VERSION = "1.47.1"; PACKAGE_NAME = "com.pikcloud.pikpak"
    SALTS = ["Gez0T9ijiI9WCeTsKSg3SMlx","zQdbalsolyb1R/","ftOjr52zt51JD68C3s","yeOBMH0JkbQdEFNNwQ0RI9T3wU/v","BRJrQZiTQ65WtMvwO","je8fqxKPdQVJiy1DM6Bc9Nb1","niV","9hFCW2R1","sHKHpe2i96","p7c5E6AcXQ/IJUuAEC9W6","","aRv9hjc9P+Pbn+u3krN6","BzStcgE8qVdqjEH16l4","SqgeZvL5j9zoHP95xWHt","zVof5yaJkPe3VFpadPof"]
    def __init__(self, username, password, device_id): self.username = username; self.password = password; self.device_id = device_id or uuid.uuid4().hex
    def _captcha_sign(self, timestamp):
        base = (self.CLIENT_ID + self.CLIENT_VERSION + self.PACKAGE_NAME + self.device_id + timestamp)
        for s in self.SALTS: base = hashlib.md5((base + s).encode()).hexdigest()
        return "1." + base
    def _build_user_agent(self): return f"ANDROID-{self.PACKAGE_NAME}/{self.CLIENT_VERSION} protocolVersion/200 accesstype/ clientid/{self.CLIENT_ID} clientversion/{self.CLIENT_VERSION} networktype/WIFI deviceid/{self.device_id} devicename/Redmi devicemodel/M2004J7AC osversion/13 sdkversion/2.0.4.204000 "
    def _captcha_init(self):
        ts = str(int(time.time() * 1000))
        payload = {"client_id": self.CLIENT_ID, "action": f"POST:{self.USER_API}/v1/auth/signin", "device_id": self.device_id, "meta": {"username": self.username, "captcha_sign": self._captcha_sign(ts), "client_version": self.CLIENT_VERSION, "package_name": self.PACKAGE_NAME, "timestamp": ts, "user_id": ""}}
        headers = {"Content-Type": "application/json", "User-Agent": self._build_user_agent(), "X-Device-Id": self.device_id}
        code, data, raw = HttpClient.request("POST", f"{self.USER_API}/v1/shield/captcha/init", headers=headers, json_data=payload)
        return data.get("captcha_token", "")
    def login(self):
        captcha_token = self._captcha_init(); 
        if not captcha_token: return None
        form_data = {"client_id": self.CLIENT_ID, "client_secret": self.CLIENT_SECRET, "username": self.username, "password": self.password, "captcha_token": captcha_token}
        headers = {"Content-Type": "application/x-www-form-urlencoded", "User-Agent": self._build_user_agent(), "X-Device-Id": self.device_id, "X-Captcha-Token": captcha_token}
        
        # Dùng requests trực tiếp vì HttpClient.request trả về tuple, cần xử lý lại để gọn
        import requests
        try:
            resp = requests.post(f"{self.USER_API}/v1/auth/signin", data=form_data, headers=headers, timeout=20, verify=False, proxies=Config.get_proxy_dict())
            data = resp.json()
            if "refresh_token" not in data: return None
            return {"access_token": data["access_token"], "refresh_token": data["refresh_token"], "user_id": data.get("sub", ""), "device_id": self.device_id}
        except: return None

class PikPakAPI:
    BASE_URL = "https://api-drive.mypikpak.com"
    AUTH_URL = "https://user.mypikpak.com"

    def __init__(self):
        self.access_token = None
        self.headers = {}
        self.vip_headers = {}

    def refresh_token(self):
        Config.load_config()
        if not Config.REFRESH_TOKEN:
            console.print(f"[bold red]{Language.get('token_missing')}[/]")
            return False
        
        ua = f"ANDROID-com.pikcloud.pikpak/1.47.1 protocolVersion/200 accesstype/ clientid/YNxT9w7GMdWvEOKa clientversion/1.47.1 action_type/ networktype/WIFI sessionid/ deviceid/{Config.DEVICE_ID} providername/NONE refresh_token/ sdkversion/2.0.4.204000 datetime/{int(time.time()*1000)} usrno/ appname/com.pikcloud.pikpak session_origin/ grant_type/ appid/ clientip/ devicename/Xiaomi osversion/13 platformversion/10 accessmode/ devicemodel/M2004J7AC"
        headers = {"User-Agent": ua, "X-Device-Id": Config.DEVICE_ID, "Content-Type": "application/x-www-form-urlencoded"}
        form = {"client_id": PikPakLogin.CLIENT_ID, "client_secret": PikPakLogin.CLIENT_SECRET, "grant_type": "refresh_token", "refresh_token": Config.REFRESH_TOKEN}
        
        import requests
        try:
            resp = requests.post(f"{self.AUTH_URL}/v1/auth/token", data=form, headers=headers, timeout=15, verify=False, proxies=Config.get_proxy_dict())
            data = resp.json()
            if "access_token" not in data: return False
            self.access_token = data["access_token"]
            self.headers = {"Authorization": f"Bearer {self.access_token}", "x-device-id": Config.DEVICE_ID}
            return True
        except: return False

    def auth_vip(self):
        if not VIP_USERNAME or not VIP_PASSWORD: return False
        if self.vip_headers: return True
        vip_device_id = hashlib.md5(VIP_USERNAME.encode()).hexdigest()
        try:
            login_instance = PikPakLogin(VIP_USERNAME, VIP_PASSWORD, vip_device_id)
            result = login_instance.login()
            if result and "access_token" in result:
                self.vip_headers = {"Authorization": f"Bearer {result['access_token']}", "x-device-id": vip_device_id}
                console.print("[bold green]VIP Login Success![/]")
                return True
            else:
                console.print("[bold red]VIP Login Failed![/]")
                return False
        except Exception as e: 
            console.print(f"[bold red]VIP Auth Error: {e}[/]")
            return False

    def get_share_info(self, share_id, password):
        if Config.USE_CACHE:
            cached = CacheManager.get("share_info", share_id, password)
            if cached: return cached['files'], cached['pass_code_token']
        all_files = []; next_token = None
        with console.status(f"[cyan]{Language.get('analyzing')}", spinner="dots"):
            while True:
                params = {"share_id": share_id, "pass_code": password, "limit": "100"}
                if next_token: params["page_token"] = next_token
                code, data, _ = HttpClient.request("GET", f"{self.BASE_URL}/drive/v1/share", headers=self.headers, params=params)
                if not data or "files" not in data: break
                all_files.extend(data.get("files", []))
                next_token = data.get("next_page_token")
                if not next_token: break
        pass_token = data.get("pass_code_token", "")
        if Config.USE_CACHE and all_files: CacheManager.set("share_info", {'files': all_files, 'pass_code_token': pass_token}, share_id, password, duration=1800)
        return all_files, pass_token

    def get_folder_files(self, share_id, parent_id, pass_token):
        if Config.USE_CACHE:
            cached = CacheManager.get("folder_files", share_id, parent_id, pass_token)
            if cached: return cached
        all_files = []; next_token = None
        while True:
            params = {"share_id": share_id, "parent_id": parent_id, "pass_code_token": pass_token, "limit": "100"}
            if next_token: params["page_token"] = next_token
            code, data, _ = HttpClient.request("GET", f"{self.BASE_URL}/drive/v1/share/detail", headers=self.headers, params=params)
            if not data: break
            all_files.extend(data.get("files", []))
            next_token = data.get("next_page_token")
            if not next_token: break
        if Config.USE_CACHE and all_files: CacheManager.set("folder_files", all_files, share_id, parent_id, pass_token, duration=1800)
        return all_files

    def get_download_url(self, share_id, file_id, pass_token):
        params = {"share_id": share_id, "file_id": file_id, "pass_code_token": pass_token}
        code, data, raw = HttpClient.request("GET", f"{self.BASE_URL}/drive/v1/share/file_info", headers=self.headers, params=params)
        if not data: return None
        info = data.get("file_info", {})
        if info.get("download_url"): return info["download_url"]
        if info.get("web_content_link"): return info["web_content_link"]
        for m in info.get("medias", []):
            if m.get("link", {}).get("url"): return m["link"]["url"]
        return None

    def restore_and_poll(self, share_id, file_id, pass_token, use_vip=False):
        req_headers = self.vip_headers if use_vip else self.headers
        payload = {
            "share_id": share_id,
            "pass_code_token": pass_token,
            "file_ids": [file_id], 
            "to_parent_id": "",    
            "params": {"trace_file_ids": file_id} 
        }
        code, data, raw_text = HttpClient.request("POST", f"{self.BASE_URL}/drive/v1/share/restore", headers=req_headers, json_data=payload)
        
        if code != 200 or not data or "restore_task_id" not in data:
            return None
            
        task_id = data["restore_task_id"]
        max_retries = 30
        for _ in range(max_retries):
            time.sleep(1.5)
            code, tdata, _ = HttpClient.request("GET", f"{self.BASE_URL}/drive/v1/tasks/{task_id}", headers=req_headers)
            if code == 200 and tdata:
                phase = tdata.get("phase")
                if phase == "PHASE_TYPE_COMPLETE":
                    try:
                        params_obj = tdata.get("params", {})
                        trace_str = params_obj.get("trace_file_ids", "{}")
                        if isinstance(trace_str, str): trace_map = json.loads(trace_str)
                        else: trace_map = trace_str
                        new_file_id = trace_map.get(file_id)
                        if new_file_id: return new_file_id
                    except: return None
                elif phase == "PHASE_TYPE_ERROR": return None
        return None

    def wait_for_file(self, filename, max_retries=20, use_vip=False):
        req_headers = self.vip_headers if use_vip else self.headers
        for _ in range(max_retries):
            time.sleep(2.0) 
            params = {"thumbnail_size": "SIZE_MEDIUM", "limit": 10, "with_audit": "true", "filters": f'{{"name":{{"eq":"{filename}"}},"trashed":{{"eq":false}}}}', "order_by": "modified_time", "sort": "desc"}
            code, data, _ = HttpClient.request("GET", f"{self.BASE_URL}/drive/v1/files", headers=req_headers, params=params)
            if data and "files" in data and len(data["files"]) > 0:
                return data["files"][0]["id"]
        return None

    def get_user_file_url(self, file_id, use_vip=False):
        req_headers = self.vip_headers if use_vip else self.headers
        for _ in range(5): 
            code, data, _ = HttpClient.request("GET", f"{self.BASE_URL}/drive/v1/files/{file_id}", headers=req_headers, params={"usage": "FETCH"})
            if data:
                if data.get("web_content_link"): return data["web_content_link"]
                if data.get("download_url"): return data["download_url"]
            time.sleep(1)
        return None
    
    def delete_file(self, file_id, use_vip=False):
        req_headers = self.vip_headers if use_vip else self.headers
        url = f"{self.BASE_URL}/drive/v1/files:batchDelete"
        payload = {"ids": [file_id]}
        HttpClient.request("POST", url, headers=req_headers, json_data=payload)

class TreeBuilder:
    def __init__(self, api): self.api = api
    def build_tree(self, files, parent, share_id, pass_token):
        folders = []; file_list = []
        for f in files:
            name = f.get("name", "Unknown"); file_id = f.get("id"); kind = f.get("kind", ""); size = int(f.get("size", 0)) if f.get("size") else 0
            if kind == "drive#folder": # Updated logic as requested
                folder_path = f"{parent}/{name}".strip("/")
                sub_files = self.api.get_folder_files(share_id, file_id, pass_token)
                children = self.build_tree(sub_files, folder_path, share_id, pass_token)
                folders.append({"type": "folder", "name": name, "path": folder_path, "folders": children["folders"], "files": children["files"]})
            elif kind == "drive#file":
                file_path = f"{parent}/{name}".strip("/")
                file_list.append({"type": "file", "name": name, "id": file_id, "path": file_path, "size": size})
        return {"folders": folders, "files": file_list}