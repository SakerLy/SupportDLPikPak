import sys
import json
import uuid
import os
import logging
import urllib3
from pathlib import Path
from rich.console import Console

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

APP_VERSION = "0.0.5"
APP_AUTHOR  = "SakerLy"
GITHUB_ZIP_URL      = "https://github.com/SakerLy/SupportDLPikPak/archive/refs/heads/main.zip"
GITHUB_VERSION_URL  = "https://raw.githubusercontent.com/SakerLy/SupportDLPikPak/refs/heads/main/config/settings.py"
GITHUB_RELEASE_URL  = "https://github.com/SakerLy/SupportDLPikPak/releases"

IS_FROZEN = getattr(sys, 'frozen', False)
BASE_DIR  = Path(sys.executable).parent if IS_FROZEN else Path(__file__).resolve().parent.parent

console = Console()
logger = logging.getLogger("PikPakTool")


class Language:
    STRINGS = {
        "en": {
            "menu_title":   "PikPak Downloader",
            "menu_dev":     "Developed by SakerLy",
            "menu_1":       "Login PikPak Account",
            "menu_2":       "Download from Link",
            "menu_3":       "Settings (Proxy, Threads, Path)",
            "menu_4":       "Cache Manager",
            "menu_5":       "View Config",
            "menu_6":       "Manage Extra Accounts",
            "menu_0":       "Exit",
            "prompt_choice": "Select Option",
            "login_header": "LOGIN TO PIKPAK",
            "login_user":   "Email / Phone / Username",
            "login_pass":   "Password",
            "login_wait":   "Logging in...",
            "login_fail":   "✖ Login failed! Check your credentials.",
            "login_success": "✓ Login successful!",
            "token_missing": "✖ Refresh Token not found. Please Login first.",
            "set_header":   "SYSTEM SETTINGS",
            "set_proxy":    "Configure Proxy",
            
            "set_adv":      "Advanced (Workers, Path, Timeout)",
            "set_lang":     "Change Language / Đổi Ngôn Ngữ",
            "proxy_status": "Proxy Status",
            "proxy_toggle": "Enable/Disable Proxy?",
            
            
            
            "thread_prompt": "Threads per file (Default 4)",
            "prem_status":  "Premium Mode",
            "prem_toggle":  "Use Premium Account to proxy ALL downloads (Restore→Download→Delete)?",
            "save_success": "✓ Settings saved!",
            "worker_prompt": "Max Concurrent Files",
            "path_prompt":  "Download Path",
            "timeout_prompt": "Timeout (seconds)",
            "cache_prompt": "Use Cache?",
            "cache_info":   "CACHE INFORMATION",
            "cache_files":  "Files count",
            "cache_size":   "Total size",
            "cache_clear":  "Clear all cache?",
            "cache_cleared": "✓ Cache cleared!",
            "input_link":   "Enter PikPak Link",
            "input_pwd":    "Password (if any)",
            "analyzing":    "➜ Analyzing folder structure...",
            "link_invalid": "✖ Invalid URL!",
            "no_files":     "✖ No files found in this link.",
            "link_info":    "LINK INFORMATION",
            "total_files":  "Total Files",
            "total_size":   "Total Size",
            "dl_opt_1":     "Download All",
            "dl_opt_2":     "Select Files",
            "dl_opt_0":     "Cancel",
            "dl_complete":  "COMPLETED!",
            "dl_success":   "Success",
            "dl_skip":      "Skipped",
            "dl_error":     "Error",
            "update_check": "Checking for updates...",
            "update_found": "🚀 NEW UPDATE AVAILABLE",
            "update_ask_web": "Open browser to download now?",
            "update_ask_src": "Auto-update source code now?",
            "update_done":  "✓ Update successful! Restarting...",
            "update_fail":  "✖ Update failed",
            "lang_select":  "Select Language / Chọn ngôn ngữ",
            "lang_en":      "English",
            "lang_vi":      "Tiếng Việt",
            "lang_set":     "✓ Language set to English",
            "global_stats": "GLOBAL STATISTICS",
            "status_restore": "Restoring...",
            "status_check": "Checking...",
            "status_getlink": "Get Link...",
            "status_dl":    "Multi-DL...",
            "status_clean": "Deleting...",
            
            # Extra accounts
            "acc_header":   "EXTRA ACCOUNTS (Bandwidth Aggregation)",
            "acc_info":     "Each extra account adds ~11 MB/s to total speed.",
            "acc_list":     "Current accounts",
            "acc_add":      "Add extra account (login with username/password)",
            "acc_remove":   "Remove extra account",
            "acc_test":     "Test all accounts",
            "acc_none":     "No extra accounts configured.",
            "acc_added":    "✓ Account added!",
            "acc_removed":  "✓ Account removed!",
            "acc_invalid":  "✖ Invalid index.",
            "acc_pool_size": "Active accounts in pool",
        },
        "vi": {
            "menu_title":   "PikPak Downloader",
            "menu_dev":     "Phát triển bởi SakerLy",
            "menu_1":       "Đăng nhập tài khoản PikPak",
            "menu_2":       "Tải file từ link PikPak",
            "menu_3":       "Cài đặt (Proxy, Luồng, Đường dẫn)",
            "menu_4":       "Quản lý Cache",
            "menu_5":       "Xem cấu hình",
            "menu_6":       "Quản lý tài khoản phụ",
            "menu_0":       "Thoát",
            "prompt_choice": "Chọn chức năng",
            "login_header": "ĐĂNG NHẬP PIKPAK",
            "login_user":   "Email / SĐT / Username",
            "login_pass":   "Mật khẩu",
            "login_wait":   "Đang đăng nhập...",
            "login_fail":   "✖ Đăng nhập thất bại! Kiểm tra lại thông tin.",
            "login_success": "✓ Đăng nhập thành công!",
            "token_missing": "✖ Chưa có Refresh Token. Hãy đăng nhập trước.",
            "set_header":   "CÀI ĐẶT HỆ THỐNG",
            "set_proxy":    "Cấu hình Proxy",
            
            "set_adv":      "Nâng cao (File đồng thời, Đường dẫn)",
            "set_lang":     "Đổi Ngôn Ngữ / Change Language",
            "proxy_status": "Trạng thái Proxy",
            "proxy_toggle": "Bật/Tắt Proxy?",
            
            
            
            "thread_prompt": "Số luồng tải mỗi file (Mặc định 4)",
            "prem_status":  "Chế độ Premium",
            "prem_toggle":  "Dùng tài khoản Premium để tải TẤT CẢ file (Lưu→Tải→Xóa)?",
            "save_success": "✓ Đã lưu cài đặt!",
            "worker_prompt": "Số file tải cùng lúc",
            "path_prompt":  "Thư mục lưu file",
            "timeout_prompt": "Timeout (giây)",
            "cache_prompt": "Sử dụng Cache?",
            "cache_info":   "THÔNG TIN CACHE",
            "cache_files":  "Số lượng file",
            "cache_size":   "Dung lượng",
            "cache_clear":  "Xóa toàn bộ cache?",
            "cache_cleared": "✓ Đã dọn dẹp cache!",
            "input_link":   "Nhập Link PikPak",
            "input_pwd":    "Mật khẩu (nếu có)",
            "analyzing":    "➜ Đang phân tích cấu trúc thư mục...",
            "link_invalid": "✖ URL không hợp lệ!",
            "no_files":     "✖ Không tìm thấy file trong liên kết này.",
            "link_info":    "THÔNG TIN LINK",
            "total_files":  "Tổng số file",
            "total_size":   "Tổng dung lượng",
            "dl_opt_1":     "Tải tất cả",
            "dl_opt_2":     "Chọn file để tải",
            "dl_opt_0":     "Hủy",
            "dl_complete":  "HOÀN TẤT!",
            "dl_success":   "Thành công",
            "dl_skip":      "Đã có (Skip)",
            "dl_error":     "Lỗi",
            "update_check": "Đang kiểm tra bản cập nhật...",
            "update_found": "🚀 CÓ BẢN CẬP NHẬT MỚI",
            "update_ask_web": "Mở trình duyệt để tải ngay?",
            "update_ask_src": "Tự động cập nhật Source Code ngay?",
            "update_done":  "✓ Cập nhật thành công! Đang khởi động lại...",
            "update_fail":  "✖ Lỗi cập nhật",
            "lang_select":  "Chọn ngôn ngữ / Select Language",
            "lang_en":      "Tiếng Anh (English)",
            "lang_vi":      "Tiếng Việt",
            "lang_set":     "✓ Đã chuyển sang Tiếng Việt",
            "global_stats": "THỐNG KÊ TIẾN TRÌNH",
            "status_restore": "Đang lưu vào Cloud...",
            "status_check": "Tìm file...",
            "status_getlink": "Lấy Link...",
            "status_dl":    "Đa Luồng...",
            "status_clean": "Xóa Vĩnh Viễn...",
            
            # Extra accounts
            "acc_header":   "TÀI KHOẢN PHỤ (Tổng hợp băng thông)",
            "acc_info":     "Mỗi tài khoản phụ thêm ~11 MB/s vào tổng tốc độ.",
            "acc_list":     "Danh sách tài khoản",
            "acc_add":      "Thêm tài khoản phụ (đăng nhập bằng user/pass)",
            "acc_remove":   "Xóa tài khoản phụ",
            "acc_test":     "Kiểm tra tất cả tài khoản",
            "acc_none":     "Chưa có tài khoản phụ nào.",
            "acc_added":    "✓ Đã thêm tài khoản!",
            "acc_removed":  "✓ Đã xóa tài khoản!",
            "acc_invalid":  "✖ Số thứ tự không hợp lệ.",
            "acc_pool_size": "Tài khoản đang hoạt động",
        },
    }

    @classmethod
    def get(cls, key):
        lang = Config.LANGUAGE
        return (cls.STRINGS.get(lang, cls.STRINGS["en"])
                .get(key, cls.STRINGS["en"].get(key, key)))


class Config:
    BASE_DIR    = BASE_DIR
    CONFIG_FILE = BASE_DIR / "config.json"
    LOG_DIR     = BASE_DIR / "logs"
    LOG_FILE    = LOG_DIR / "pikpak_tool.log"

    # Auth
    REFRESH_TOKEN = ""; DEVICE_ID = ""; CAPTCHA_TOKEN = ""; LANGUAGE = "en"

    # Proxy
    USE_PROXY = False; PROXY_TYPE = "http"; PROXY_HOST = ""
    PROXY_PORT = ""; PROXY_USERNAME = ""; PROXY_PASSWORD = ""


    # Download behaviour
    FORCE_PREMIUM_MODE  = False
    MAX_WORKERS         = 3      # concurrent files
    DOWNLOAD_PATH_STR   = "downloads"
    TIMEOUT             = 30
    USE_CACHE           = True
    CONCURRENT_THREADS  = 8      # connections per file (raised default from 4→8)

    # ── NEW: extra accounts for bandwidth aggregation ─────────────────────────
    # List of {"refresh_token": "...", "device_id": "..."}
    EXTRA_ACCOUNTS: list = []

    @classmethod
    def get_download_dir(cls):
        cls.load_config()
        if os.path.isabs(cls.DOWNLOAD_PATH_STR):
            return Path(cls.DOWNLOAD_PATH_STR)
        return cls.BASE_DIR / cls.DOWNLOAD_PATH_STR

    @classmethod
    def get_proxy_dict(cls):
        """
        Proxy dict dùng cho DOWNLOAD (tải file).
        - Proxy không cần auth: http://1.2.3.4:8080
        - Proxy có auth:        http://user:pass@1.2.3.4:8080
        - Trả None nếu proxy tắt hoặc host/port trống
        """
        if not cls.USE_PROXY:
            return None
        host = (cls.PROXY_HOST or "").strip()
        port = (cls.PROXY_PORT or "").strip()
        if not host or not port:
            return None

        ptype = (cls.PROXY_TYPE or "http").strip().lower()
        user  = (cls.PROXY_USERNAME or "").strip()
        pwd   = (cls.PROXY_PASSWORD or "").strip()

        if user:
            from urllib.parse import quote
            auth = f"{quote(user, safe='')}:{quote(pwd, safe='')}@"
        else:
            auth = ""

        url = f"{ptype}://{auth}{host}:{port}"
        return {"http": url, "https": url}

    @classmethod
    def get_api_proxy_dict(cls):
        """
        Proxy dict cho API calls (login, restore, poll, token refresh...).
        Luôn trả None — API calls KHÔNG đi qua proxy để tránh lỗi auth.
        """
        return None

    @classmethod
    def test_proxy(cls) -> tuple:
        """
        Kiểm tra proxy có hoạt động không.
        Hỗ trợ: http, https, socks4, socks5.
        Trả về (bool, message).
        """
        proxy = cls.get_proxy_dict()
        if not proxy:
            return False, "Proxy chưa cấu hình"

        import requests as _req, urllib3
        urllib3.disable_warnings()

        # Dùng Session để set proxies đúng cách
        session = _req.Session()
        session.proxies.update(proxy)
        session.verify = False

        proxy_type = cls.PROXY_TYPE.lower()
        is_socks    = proxy_type in ("socks4", "socks5")

        # Bước 1: kết nối cơ bản
        try:
            test_url = "http://api.ipify.org" if is_socks else "http://www.gstatic.com/generate_204"
            session.get(test_url, timeout=10)
        except Exception as e:
            session.close()
            err = str(e)
            if "407" in err or "Proxy Authentication" in err:
                return False, "Proxy yêu cầu xác thực — kiểm tra Username/Password"
            if "No module" in err and "socks" in err.lower():
                return False, "Thiếu thư viện: pip install requests[socks]"
            if "SOCKS" in err or "socks" in err.lower():
                return False, f"SOCKS lỗi — kiểm tra loại proxy ({proxy_type})"
            return False, f"Kết nối thất bại: {e}"

        # Bước 2: HTTPS (PikPak dùng HTTPS nên bắt buộc phải pass)
        try:
            r  = session.get("https://api.ipify.org?format=json", timeout=10)
            ip = r.json().get("ip", "?")
            session.close()
            return True, f"OK — IP qua proxy: {ip}"
        except Exception as e:
            session.close()
            err = str(e)
            if "SSLError" in err or "CONNECT" in err or "tunnel" in err.lower():
                return False, "Proxy không hỗ trợ HTTPS tunnel — thử đổi sang socks5"
            if "407" in err or "Proxy Authentication" in err:
                return False, "Proxy yêu cầu xác thực — kiểm tra Username/Password"
            if "timed out" in err.lower() or "Timeout" in err:
                return False, "Proxy timeout — proxy quá chậm hoặc đã chết"
            return False, f"HTTPS thất bại: {e}"

    @classmethod
    def load_config(cls):
        if cls.CONFIG_FILE.exists():
            try:
                with open(cls.CONFIG_FILE, 'r', encoding='utf-8') as f:
                    d = json.load(f)
                cls.REFRESH_TOKEN      = d.get("refresh_token",    "")
                cls.DEVICE_ID          = d.get("device_id",         "")
                cls.CAPTCHA_TOKEN      = d.get("captcha_token",     "")
                cls.MAX_WORKERS        = int(d.get("max_workers",   3))
                cls.DOWNLOAD_PATH_STR  = d.get("download_path",    "downloads")
                cls.TIMEOUT            = int(d.get("timeout",       30))
                cls.USE_CACHE          = d.get("use_cache",         True)
                cls.LANGUAGE           = d.get("language",          "en")
                cls.USE_PROXY          = d.get("use_proxy",         False)
                cls.PROXY_TYPE         = d.get("proxy_type",        "http")
                cls.PROXY_HOST         = d.get("proxy_host",        "")
                cls.PROXY_PORT         = d.get("proxy_port",        "")
                cls.PROXY_USERNAME     = d.get("proxy_username",    "")
                cls.PROXY_PASSWORD     = d.get("proxy_password",    "")
                cls.CONCURRENT_THREADS = int(d.get("concurrent_threads", 8))
                cls.FORCE_PREMIUM_MODE = d.get("force_premium_mode", False)
                cls.EXTRA_ACCOUNTS     = d.get("extra_accounts",    [])
            except: pass
        if not cls.DEVICE_ID:
            cls.DEVICE_ID = uuid.uuid4().hex

    @classmethod
    def save_config(cls):
        try:
            data = {
                "refresh_token":      cls.REFRESH_TOKEN,
                "device_id":          cls.DEVICE_ID,
                "captcha_token":      cls.CAPTCHA_TOKEN,
                "max_workers":        cls.MAX_WORKERS,
                "download_path":      cls.DOWNLOAD_PATH_STR,
                "timeout":            cls.TIMEOUT,
                "use_cache":          cls.USE_CACHE,
                "language":           cls.LANGUAGE,
                "use_proxy":          cls.USE_PROXY,
                "proxy_type":         cls.PROXY_TYPE,
                "proxy_host":         cls.PROXY_HOST,
                "proxy_port":         cls.PROXY_PORT,
                "proxy_username":     cls.PROXY_USERNAME,
                "proxy_password":     cls.PROXY_PASSWORD,
                "concurrent_threads": cls.CONCURRENT_THREADS,
                "force_premium_mode": cls.FORCE_PREMIUM_MODE,
                "extra_accounts":     cls.EXTRA_ACCOUNTS,
            }
            with open(cls.CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            return True
        except:
            return False

    @classmethod
    def migrate_config(cls):
        """
        Gọi 1 lần khi khởi động tool.
        Load config cũ → save lại theo schema hiện tại.
        Tác dụng:
          - Xóa các key cũ không còn dùng (vd: use_idm, idm_path)
          - Thêm key mới với giá trị mặc định nếu chưa có
          - File config luôn sạch, đúng với phiên bản code hiện tại
        """
        if not cls.CONFIG_FILE.exists():
            return
        cls.load_config() 
        cls.save_config()  

    @classmethod
    def setup_dirs(cls):
        try: cls.get_download_dir().mkdir(parents=True, exist_ok=True)
        except: pass