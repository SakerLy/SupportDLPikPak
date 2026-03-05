import sys
import json
import uuid
import os
import urllib3
from pathlib import Path
from rich.console import Console

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

APP_VERSION = "0.0.2"
APP_AUTHOR = "SakerLy"
GITHUB_ZIP_URL = "https://github.com/SakerLy/SupportDLPikPak/archive/refs/heads/main.zip"
GITHUB_VERSION_URL = "https://raw.githubusercontent.com/SakerLy/SupportDLPikPak/refs/heads/main/config/settings.py"
GITHUB_RELEASE_URL = "https://github.com/SakerLy/SupportDLPikPak/releases"

IS_FROZEN = getattr(sys, 'frozen', False)
if IS_FROZEN:
    BASE_DIR = Path(sys.executable).parent
else:
    BASE_DIR = Path(__file__).resolve().parent.parent

console = Console()

class Language:
    STRINGS = {
        "en": {
            "menu_title": "PikPak Downloader",
            "menu_dev": "Developed by SakerLy",
            "menu_1": "Login PikPak Account",
            "menu_2": "Download from Link",
            "menu_3": "Settings (Proxy, IDM, Path)",
            "menu_4": "Cache Manager",
            "menu_5": "View Config",
            "menu_0": "Exit",
            "prompt_choice": "Select Option",
            "login_header": "LOGIN TO PIKPAK",
            "login_user": "Email / Phone / Username",
            "login_pass": "Password",
            "login_wait": "Logging in...",
            "login_fail": "✖ Login failed! Check your credentials.",
            "login_success": "✓ Login successful!",
            "token_missing": "✖ Refresh Token not found. Please Login first.",
            "set_header": "SYSTEM SETTINGS",
            "set_proxy": "Configure Proxy",
            "set_idm": "Configure IDM & Threads",
            "set_adv": "Advanced (Workers, Path, Timeout)",
            "set_lang": "Change Language / Đổi Ngôn Ngữ",
            "proxy_status": "Proxy Status",
            "proxy_toggle": "Enable/Disable Proxy?",
            "idm_status": "IDM Integration",
            "idm_toggle": "Enable IDM for heavy files?",
            "idm_path": "IDM Path (IDMan.exe)",
            "thread_prompt": "Threads per file (Default 4)",
            "prem_status": "Premium Mode",
            "prem_toggle": "Use Premium Account to proxy ALL downloads (Restore->Download->Delete)?",
            "save_success": "✓ Settings saved!",
            "worker_prompt": "Max Concurrent Files",
            "path_prompt": "Download Path",
            "timeout_prompt": "Timeout (seconds)",
            "cache_prompt": "Use Cache?",
            "cache_info": "CACHE INFORMATION",
            "cache_files": "Files count",
            "cache_size": "Total size",
            "cache_clear": "Clear all cache?",
            "cache_cleared": "✓ Cache cleared!",
            "input_link": "Enter PikPak Link",
            "input_pwd": "Password (if any)",
            "analyzing": "➜ Analyzing folder structure...",
            "link_invalid": "✖ Invalid URL!",
            "no_files": "✖ No files found in this link.",
            "link_info": "LINK INFORMATION",
            "total_files": "Total Files",
            "total_size": "Total Size",
            "dl_opt_1": "Download All",
            "dl_opt_2": "Select Files",
            "dl_opt_0": "Cancel",
            "dl_complete": "COMPLETED!",
            "dl_success": "Success",
            "dl_skip": "Skipped",
            "dl_error": "Error",
            "update_check": "Checking for updates...",
            "update_found": "🚀 NEW UPDATE AVAILABLE",
            "update_ask_web": "Open browser to download now?",
            "update_ask_src": "Auto-update source code now?",
            "update_done": "✓ Update successful! Restarting...",
            "update_fail": "✖ Update failed",
            "lang_select": "Select Language / Chọn ngôn ngữ",
            "lang_en": "English",
            "lang_vi": "Tiếng Việt",
            "lang_set": "✓ Language set to English",
            "global_stats": "GLOBAL STATISTICS",
            "status_restore": "Restoring...",
            "status_check": "Checking...",
            "status_getlink": "Get Link...",
            "status_dl": "Multi-DL...",
            "status_clean": "Deleting...",
            "status_idm": "Sent to IDM",
        },
        "vi": {
            "menu_title": "PikPak Downloader",
            "menu_dev": "Phát triển bởi SakerLy",
            "menu_1": "Đăng nhập tài khoản PikPak",
            "menu_2": "Tải file từ link PikPak",
            "menu_3": "Cài đặt (Proxy, IDM, Luồng)",
            "menu_4": "Quản lý Cache",
            "menu_5": "Xem cấu hình",
            "menu_0": "Thoát",
            "prompt_choice": "Chọn chức năng",
            "login_header": "ĐĂNG NHẬP PIKPAK",
            "login_user": "Email / SĐT / Username",
            "login_pass": "Mật khẩu",
            "login_wait": "Đang đăng nhập...",
            "login_fail": "✖ Đăng nhập thất bại! Kiểm tra lại thông tin.",
            "login_success": "✓ Đăng nhập thành công!",
            "token_missing": "✖ Chưa có Refresh Token. Hãy đăng nhập trước.",
            "set_header": "CÀI ĐẶT HỆ THỐNG",
            "set_proxy": "Cấu hình Proxy",
            "set_idm": "Cấu hình IDM & Đa luồng",
            "set_premium": "Chế độ Tải qua Cloud (Premium Transfer)",
            "set_adv": "Nâng cao (File đồng thời, Đường dẫn)",
            "set_lang": "Đổi Ngôn Ngữ / Change Language",
            "proxy_status": "Trạng thái Proxy",
            "proxy_toggle": "Bật/Tắt Proxy?",
            "idm_status": "Tích hợp IDM",
            "idm_toggle": "Sử dụng IDM để tải file lớn?",
            "idm_path": "Đường dẫn IDM (IDMan.exe)",
            "thread_prompt": "Số luồng tải mỗi file (Mặc định 4)",
            "prem_status": "Chế độ Premium",
            "prem_toggle": "Dùng tài khoản Premium để tải TẤT CẢ file (Lưu->Tải->Xóa)?",
            "save_success": "✓ Đã lưu cài đặt!",
            "worker_prompt": "Số file tải cùng lúc",
            "path_prompt": "Thư mục lưu file",
            "timeout_prompt": "Timeout (giây)",
            "cache_prompt": "Sử dụng Cache?",
            "cache_info": "THÔNG TIN CACHE",
            "cache_files": "Số lượng file",
            "cache_size": "Dung lượng",
            "cache_clear": "Xóa toàn bộ cache?",
            "cache_cleared": "✓ Đã dọn dẹp cache!",
            "input_link": "Nhập Link PikPak",
            "input_pwd": "Mật khẩu (nếu có)",
            "analyzing": "➜ Đang phân tích cấu trúc thư mục...",
            "link_invalid": "✖ URL không hợp lệ!",
            "no_files": "✖ Không tìm thấy file trong liên kết này.",
            "link_info": "THÔNG TIN LINK",
            "total_files": "Tổng số file",
            "total_size": "Tổng dung lượng",
            "dl_opt_1": "Tải tất cả",
            "dl_opt_2": "Chọn file để tải",
            "dl_opt_0": "Hủy",
            "dl_complete": "HOÀN TẤT!",
            "dl_success": "Thành công",
            "dl_skip": "Đã có (Skip)",
            "dl_error": "Lỗi",
            "update_check": "Đang kiểm tra bản cập nhật...",
            "update_found": "🚀 CÓ BẢN CẬP NHẬT MỚI",
            "update_ask_web": "Mở trình duyệt để tải ngay?",
            "update_ask_src": "Tự động cập nhật Source Code ngay?",
            "update_done": "✓ Cập nhật thành công! Đang khởi động lại...",
            "update_fail": "✖ Lỗi cập nhật",
            "lang_select": "Chọn ngôn ngữ / Select Language",
            "lang_en": "Tiếng Anh (English)",
            "lang_vi": "Tiếng Việt",
            "lang_set": "✓ Đã chuyển sang Tiếng Việt",
            "global_stats": "THỐNG KÊ TIẾN TRÌNH",
            "status_restore": "Đang lưu vào Cloud...",
            "status_check": "Tìm file...",
            "status_getlink": "Lấy Link...",
            "status_dl": "Đa Luồng...",
            "status_clean": "Xóa Vĩnh Viễn...",
            "status_idm": "Đã gửi IDM",
        }
    }
    @classmethod
    def get(cls, key):
        lang = Config.LANGUAGE
        return cls.STRINGS.get(lang, cls.STRINGS["en"]).get(key, cls.STRINGS["en"].get(key, key))

class Config:
    BASE_DIR = BASE_DIR 
    CONFIG_FILE = BASE_DIR / "config.json"
    
    REFRESH_TOKEN = ""; DEVICE_ID = ""; CAPTCHA_TOKEN = ""; LANGUAGE = "en"
    USE_PROXY = False; PROXY_TYPE = "http"; PROXY_HOST = ""; PROXY_PORT = ""; PROXY_USERNAME = ""; PROXY_PASSWORD = ""
    USE_IDM = False; IDM_PATH = r"C:\Program Files (x86)\Internet Download Manager\IDMan.exe"
    
    FORCE_PREMIUM_MODE = False
    
    MAX_WORKERS = 3; DOWNLOAD_PATH_STR = "downloads"; TIMEOUT = 30; USE_CACHE = True
    CONCURRENT_THREADS = 4
    
    @classmethod
    def get_download_dir(cls):
        cls.load_config()
        if os.path.isabs(cls.DOWNLOAD_PATH_STR): return Path(cls.DOWNLOAD_PATH_STR)
        return cls.BASE_DIR / cls.DOWNLOAD_PATH_STR

    @classmethod
    def get_proxy_dict(cls):
        if not cls.USE_PROXY or not cls.PROXY_HOST: return None
        auth = f"{cls.PROXY_USERNAME}:{cls.PROXY_PASSWORD}@" if cls.PROXY_USERNAME else ""
        proxy_url = f"{cls.PROXY_TYPE}://{auth}{cls.PROXY_HOST}:{cls.PROXY_PORT}"
        return {"http": proxy_url, "https": proxy_url}

    @classmethod
    def load_config(cls):
        if cls.CONFIG_FILE.exists():
            try:
                with open(cls.CONFIG_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    cls.REFRESH_TOKEN = data.get("refresh_token", "")
                    cls.DEVICE_ID = data.get("device_id", "")
                    cls.CAPTCHA_TOKEN = data.get("captcha_token", "")
                    cls.MAX_WORKERS = int(data.get("max_workers", 3))
                    cls.DOWNLOAD_PATH_STR = data.get("download_path", "downloads")
                    cls.TIMEOUT = int(data.get("timeout", 30))
                    cls.USE_CACHE = data.get("use_cache", True)
                    cls.LANGUAGE = data.get("language", "en")
                    cls.USE_PROXY = data.get("use_proxy", False)
                    cls.PROXY_TYPE = data.get("proxy_type", "http")
                    cls.PROXY_HOST = data.get("proxy_host", "")
                    cls.PROXY_PORT = data.get("proxy_port", "")
                    cls.PROXY_USERNAME = data.get("proxy_username", "")
                    cls.PROXY_PASSWORD = data.get("proxy_password", "")
                    cls.USE_IDM = data.get("use_idm", False)
                    cls.IDM_PATH = data.get("idm_path", r"C:\Program Files (x86)\Internet Download Manager\IDMan.exe")
                    cls.CONCURRENT_THREADS = int(data.get("concurrent_threads", 4))
                    cls.FORCE_PREMIUM_MODE = data.get("force_premium_mode", False)
            except: pass
        if not cls.DEVICE_ID: cls.DEVICE_ID = str(uuid.uuid4().hex)

    @classmethod
    def save_config(cls):
        try:
            data = {
                "refresh_token": cls.REFRESH_TOKEN, "device_id": cls.DEVICE_ID, "captcha_token": cls.CAPTCHA_TOKEN,
                "max_workers": cls.MAX_WORKERS, "download_path": cls.DOWNLOAD_PATH_STR, "timeout": cls.TIMEOUT,
                "use_cache": cls.USE_CACHE, "language": cls.LANGUAGE, 
                "use_proxy": cls.USE_PROXY, "proxy_type": cls.PROXY_TYPE, "proxy_host": cls.PROXY_HOST, "proxy_port": cls.PROXY_PORT,
                "proxy_username": cls.PROXY_USERNAME, "proxy_password": cls.PROXY_PASSWORD,
                "use_idm": cls.USE_IDM, "idm_path": cls.IDM_PATH,
                "concurrent_threads": cls.CONCURRENT_THREADS,
                "force_premium_mode": cls.FORCE_PREMIUM_MODE
            }
            with open(cls.CONFIG_FILE, 'w', encoding='utf-8') as f: json.dump(data, f, indent=2)
            return True
        except: return False

    @classmethod
    def setup_dirs(cls):
        try: cls.get_download_dir().mkdir(parents=True, exist_ok=True)
        except: pass