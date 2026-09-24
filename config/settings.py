import sys
import json
import uuid
import os
import shutil
import urllib3
from pathlib import Path
from rich.console import Console
from core.logger import normalize_level, set_log_level

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

APP_VERSION = "0.0.9"
APP_AUTHOR = "SakerLy"
GITHUB_ZIP_URL = "https://github.com/SakerLy/SupportDLPikPak/archive/refs/heads/main.zip"
GITHUB_VERSION_URL = "https://raw.githubusercontent.com/SakerLy/SupportDLPikPak/refs/heads/main/config/settings.py"
GITHUB_RELEASE_URL = "https://github.com/SakerLy/SupportDLPikPak/releases"

IS_FROZEN = getattr(sys, "frozen", False)
BASE_DIR = Path(sys.executable).parent if IS_FROZEN else Path(__file__).resolve().parent.parent

console = Console()


class Language:
    STRINGS = {
        "en": {
            # --- Main menu ---
            "menu_title": "PikPak Downloader",
            "menu_dev": "Developed by SakerLy",
            "menu_1": "Login PikPak Account",
            "menu_2": "Download from Link",
            "menu_3": "Settings (Proxy, Threads, Path)",
            "menu_4": "Cache Manager",
            "menu_5": "View Config",
            "menu_6": "Manage Extra Accounts",
            "menu_0": "Exit",
            "back": "Back",
            "prompt_choice": "Select Option",
            "press_enter": "Press Enter to continue...",
            "saved": "✓ Saved!",
            "on": "ON",
            "off": "OFF",
            "toggle_off_q": "➜ Turn it off?",
            "toggle_on_q": "➜ Turn it on?",
            "pool_line": "⚡ Pool: {n} accounts (~{speed} MB/s max)",
            "pool_startup": "✓ Account pool: {n} accounts active (~{speed} MB/s max)",
            "pool_tip": "Tip: Add extra accounts ([6]) to multiply speed",
            # --- Login ---
            "login_header": "LOGIN TO PIKPAK",
            "login_user": "Email / Phone / Username",
            "login_pass": "Password",
            "login_wait": "Logging in...",
            "login_fail": "✖ Login failed! Check your credentials.",
            "login_success": "✓ Login successful!",
            "token_missing": "✖ Refresh Token not found. Please Login first.",
            # --- Settings ---
            "set_header": "SYSTEM SETTINGS",
            "set_proxy": "Configure Proxy",
            "set_threads": "Threads per file",
            "set_adv": "Advanced (Workers, Path, Timeout)",
            "set_lang": "Change Language / Đổi Ngôn Ngữ",
            "set_lang_short": "Language",
            "set_prem": "Premium Transfer Mode",
            "set_cache": "Cache",
            "proxy_status": "Proxy Status",
            "proxy_toggle": "Enable/Disable Proxy?",
            "thread_prompt": "Threads per file (Default 8)",
            "prem_status": "Premium Mode",
            "prem_toggle": "Use Premium Account to proxy ALL downloads?",
            "save_success": "✓ Settings saved!",
            "worker_prompt": "Max Concurrent Files",
            "path_prompt": "Download Path",
            "timeout_prompt": "Timeout (seconds)",
            "cache_prompt": "Use Cache?",
            # --- Proxy ---
            "px_header": "PROXY",
            "px_status": "Status:",
            "px_address": "Address:",
            "px_opt_toggle": "Enable / Disable proxy",
            "px_opt_edit": "Edit type / host / port / auth",
            "px_opt_paste": "Paste proxy string (e.g. socks5://user:pass@host:port)",
            "px_opt_test": "Test proxy now",
            "px_hint_auth": "── Leave Username empty if the proxy needs no auth ──",
            "px_type_label": "Proxy type:",
            "px_type_http": "http   — most common; many free proxies can't tunnel HTTPS",
            "px_type_https": "https  — proxy server that itself uses SSL",
            "px_type_socks4": "socks4 — old, no auth by password",
            "px_type_socks5": "socks5 — HTTP & HTTPS, most stable (recommended)",
            "px_type": "Type",
            "px_host": "Host / IP",
            "px_port": "Port",
            "px_user": "Username (Enter to skip)",
            "px_pass": "Password",
            "px_port_invalid": "✖ Invalid port (1-65535)",
            "px_warn_http": "⚠ Free HTTP proxies often fail with PikPak (HTTPS). Try socks5 if it errors.",
            "px_paste_prompt": "Proxy",
            "px_paste_formats": "Accepted: type://user:pass@host:port · user:pass@host:port · host:port:user:pass · host:port",
            "px_paste_invalid": "✖ Could not read that proxy string",
            "px_enable_q": "Enable proxy now?",
            "px_testing": "Testing HTTP → HTTPS → PikPak...",
            "px_test_ok": "✓ Proxy works",
            "px_test_fail": "✖ Proxy is not usable",
            "px_fix_title": "How to fix:",
            "px_fix_1": "1. Switch the proxy type to [bold]socks5[/bold] (best HTTPS support)",
            "px_fix_2": "2. Check that the proxy is alive, and host/port/user/password are correct",
            "px_fix_3": "3. Free HTTP proxies usually cannot tunnel HTTPS",
            "px_not_configured": "Proxy is not configured or disabled",
            "px_err_auth": "Proxy rejected the username / password",
            "px_err_timeout": "Proxy timed out",
            "px_err_refused": "Cannot connect to proxy (wrong host/port or proxy is dead)",
            "px_err_tunnel": "Proxy cannot tunnel HTTPS — try socks5",
            # --- Threads / premium / advanced ---
            "thr_header": "DOWNLOAD THREADS",
            "thr_current": "Current threads/file:",
            "thr_est": "Estimated speed:",
            "thr_max": "max",
            "thr_reco": "Recommended:",
            "thr_reco_val": "8–16 for best speed",
            "thr_saved": "Threads: {n} conn (~{speed} MB/s max)",
            "prem_header": "PREMIUM TRANSFER MODE",
            "prem_on_desc": "Save ALL files to cloud before downloading",
            "prem_off_desc": "Only save video/zip/rar/iso to cloud (default)",
            "prem_when_on": "When ON:",
            "prem_when_off": "When OFF:",
            "adv_header": "ADVANCED SETTINGS",
            "adv_workers": "Concurrent files:",
            "adv_path": "Download folder:",
            "adv_timeout": "Timeout:",
            "adv_saved": "✓ All settings saved!",
            "path_invalid": "✖ Cannot create that folder, keeping the old one",
            # --- Cache / config ---
            "cache_info": "CACHE INFORMATION",
            "cache_files": "Files count",
            "cache_size": "Total size",
            "cache_clear": "Clear all cache?",
            "cache_cleared": "✓ Cache cleared!",
            "cfg_path": "Download Path:",
            "cfg_workers": "Max Workers:",
            "cfg_timeout": "Timeout:",
            "cfg_lang": "Language:",
            "cfg_proxy": "Proxy:",
            "cfg_prem": "Premium Mode:",
            "cfg_threads": "Threads/File:",
            "cfg_accounts": "Accounts:",
            "cfg_log": "Log level:",
            "cfg_log_file": "Log file:",
            "log_prompt": "Log level (DEBUG = very detailed, for bug reports)",
            # --- Download ---
            "input_link": "Enter PikPak Link",
            "input_link_hint": "(separate multiple links with commas)",
            "input_pwd": "Password (if any)",
            "analyzing": "➜ Analyzing folder structure...",
            "dl_analyzing": "➜ Analyzing:",
            "dl_load_fail": "✖ Skipped (failed to load)",
            "link_invalid": "✖ Invalid URL!",
            "no_files": "✖ No files found in this link.",
            "link_info": "LINK INFORMATION",
            "links_ok": "Links OK",
            "total_files": "Total Files",
            "total_size": "Total Size",
            "dl_opt_1": "Download All",
            "dl_opt_2": "Select Files",
            "dl_opt_0": "Cancel",
            "file_list": "FILE LIST",
            "more_files": "... +{n} files",
            "dl_pick": "👉 ID (e.g. 1-3,5)",
            "dl_starting": "⬇ Downloading {n} file(s) from {m} link(s) concurrently",
            "dl_cancel_hint": "Press [bold]Q[/bold] to cancel",
            "dl_cancelling": "⛔ CANCELLING...",
            "dl_cancelled": "⛔ Cancelled – remaining files skipped.",
            "dl_cancel_title": "CANCELLED",
            "dl_cancel_by_user": "⛔ CANCELLED BY USER",
            "dl_cancel_done": "Completed before cancel",
            "dl_cancel_left": "Stopped / not started",
            "dl_result": "RESULT",
            "dl_complete": "COMPLETED!",
            "dl_success": "Success",
            "dl_failed": "Failed",
            "dl_skip": "Skipped",
            "dl_error": "Error",
            "dl_retry_q": "⚠ {n} file(s) failed. Retry?",
            "dl_auto_continue": "✓ Done! Continuing in {n}s... (Press Enter to skip)",
            "err_disk_full": "✖ SYSTEM ERROR: Disk is full! Downloads stopped.",
            # --- Update ---
            "update_check": "Checking for updates...",
            "update_found": "🚀 NEW UPDATE AVAILABLE",
            "update_ask_web": "Open browser to download now?",
            "update_ask_src": "Auto-update source code now?",
            "update_done": "✓ Update successful! Restarting...",
            "update_fail": "✖ Update failed",
            # --- Language ---
            "lang_select": "Select Language / Chọn ngôn ngữ",
            "lang_en": "English",
            "lang_vi": "Tiếng Việt",
            "lang_set": "✓ Language set to English",
            # --- Dashboard ---
            "global_stats": "GLOBAL STATISTICS",
            "status_restore": "Restoring...",
            "status_check": "Checking...",
            "status_getlink": "Get Link...",
            "status_dl": "Multi-DL...",
            "status_clean": "Deleting...",
            # --- Accounts ---
            "acc_header": "EXTRA ACCOUNTS",
            "acc_info": "Each extra account adds ~11 MB/s to total speed.",
            "acc_list": "Current accounts",
            "acc_add": "Add extra account",
            "acc_remove": "Remove extra account",
            "acc_remove_prompt": "Number to remove (1-{n})",
            "acc_test": "Test all accounts",
            "acc_none": "No extra accounts configured.",
            "acc_added": "✓ Account added!",
            "acc_removed": "✓ Account removed!",
            "acc_invalid": "✖ Invalid index.",
            "acc_duplicate": "✖ This account is already in the list.",
            "acc_relogin": "Re-login an extra account",
            "acc_relogin_prompt": "Number to re-login (1-{n})",
            "acc_relogin_done": "✓ Account #{n} logged in again!",
            "acc_relogin_needed": "session expired, log in again",
            "acc_col_status": "Status",
            "acc_col_user": "User ID",
            "acc_st_ok": "✓ OK",
            "acc_st_relogin": "✖ Log in again",
            "acc_st_error": "✖ Error",
            "acc_st_unknown": "?",
            "warn_main_relogin": "⚠ Main account session expired (PikPak revoked the token). Choose [1] to log in again.",
            "warn_extra_relogin": "⚠ Extra account #{n}: session expired. Go to [6] → [4] to log in again.",
            "warn_acc_error": "⚠ Account {label} could not connect: {err}",
            "acc_pool_size": "Active accounts in pool",
            "acc_pool_now": "Pool now: {n} accounts (~{speed} MB/s max)",
            "acc_active": "Active: {n} / {total} accounts",
            "acc_speed": "Estimated max speed: ~{speed} MB/s",
            # --- Cloud cleanup ---
            "gc_start": "🧹 Cleaning up temporary files left on cloud...",
            "gc_done": "✓ Cloud cleanup finished!",
            "gc_freed": "➜ {acc}: removed {n} temporary file(s).",
        },
        "vi": {
            # --- Menu chính ---
            "menu_title": "PikPak Downloader",
            "menu_dev": "Phát triển bởi SakerLy",
            "menu_1": "Đăng nhập tài khoản PikPak",
            "menu_2": "Tải file từ link PikPak",
            "menu_3": "Cài đặt (Proxy, Luồng, Thư mục)",
            "menu_4": "Quản lý Cache",
            "menu_5": "Xem cấu hình",
            "menu_6": "Quản lý tài khoản phụ",
            "menu_0": "Thoát",
            "back": "Quay lại",
            "prompt_choice": "Chọn chức năng",
            "press_enter": "Nhấn Enter để tiếp tục...",
            "saved": "✓ Đã lưu!",
            "on": "BẬT",
            "off": "TẮT",
            "toggle_off_q": "➜ Tắt đi?",
            "toggle_on_q": "➜ Bật lên?",
            "pool_line": "⚡ Pool: {n} tài khoản (~{speed} MB/s tối đa)",
            "pool_startup": "✓ Pool: {n} tài khoản đang hoạt động (~{speed} MB/s tối đa)",
            "pool_tip": "Mẹo: Thêm tài khoản phụ ([6]) để tăng tốc độ tải",
            # --- Đăng nhập ---
            "login_header": "ĐĂNG NHẬP PIKPAK",
            "login_user": "Email / SĐT / Username",
            "login_pass": "Mật khẩu",
            "login_wait": "Đang đăng nhập...",
            "login_fail": "✖ Đăng nhập thất bại! Kiểm tra lại thông tin.",
            "login_success": "✓ Đăng nhập thành công!",
            "token_missing": "✖ Chưa có Refresh Token. Hãy đăng nhập trước.",
            # --- Cài đặt ---
            "set_header": "CÀI ĐẶT HỆ THỐNG",
            "set_proxy": "Cấu hình Proxy",
            "set_threads": "Số luồng mỗi file",
            "set_adv": "Nâng cao (File đồng thời, Thư mục, Timeout)",
            "set_lang": "Đổi Ngôn Ngữ / Change Language",
            "set_lang_short": "Ngôn ngữ",
            "set_prem": "Chế độ Premium Transfer",
            "set_cache": "Cache",
            "proxy_status": "Trạng thái Proxy",
            "proxy_toggle": "Bật/Tắt Proxy?",
            "thread_prompt": "Số luồng tải mỗi file (Mặc định 8)",
            "prem_status": "Chế độ Premium",
            "prem_toggle": "Dùng tài khoản Premium để tải TẤT CẢ file?",
            "save_success": "✓ Đã lưu cài đặt!",
            "worker_prompt": "Số file tải cùng lúc",
            "path_prompt": "Thư mục lưu file",
            "timeout_prompt": "Timeout (giây)",
            "cache_prompt": "Sử dụng Cache?",
            # --- Proxy ---
            "px_header": "PROXY",
            "px_status": "Trạng thái:",
            "px_address": "Địa chỉ:",
            "px_opt_toggle": "Bật / Tắt Proxy",
            "px_opt_edit": "Chỉnh loại / host / port / tài khoản",
            "px_opt_paste": "Dán chuỗi proxy (vd: socks5://user:pass@host:port)",
            "px_opt_test": "Kiểm tra proxy ngay",
            "px_hint_auth": "── Để trống Username nếu proxy không cần đăng nhập ──",
            "px_type_label": "Loại proxy:",
            "px_type_http": "http   — phổ biến nhất, nhưng nhiều proxy free KHÔNG hỗ trợ HTTPS",
            "px_type_https": "https  — proxy server dùng SSL",
            "px_type_socks4": "socks4 — đời cũ, không hỗ trợ mật khẩu",
            "px_type_socks5": "socks5 — hỗ trợ cả HTTP lẫn HTTPS, ổn định nhất (khuyên dùng)",
            "px_type": "Loại",
            "px_host": "Host / IP",
            "px_port": "Port",
            "px_user": "Username (Enter nếu không có)",
            "px_pass": "Mật khẩu",
            "px_port_invalid": "✖ Port không hợp lệ (1-65535)",
            "px_warn_http": "⚠ Proxy HTTP free thường không tải được từ PikPak (HTTPS). Nếu lỗi hãy thử socks5.",
            "px_paste_prompt": "Proxy",
            "px_paste_formats": "Hỗ trợ: loại://user:pass@host:port · user:pass@host:port · host:port:user:pass · host:port",
            "px_paste_invalid": "✖ Không đọc được chuỗi proxy này",
            "px_enable_q": "Bật proxy luôn?",
            "px_testing": "Đang kiểm tra HTTP → HTTPS → PikPak...",
            "px_test_ok": "✓ Proxy hoạt động tốt",
            "px_test_fail": "✖ Proxy không dùng được",
            "px_fix_title": "Hướng xử lý:",
            "px_fix_1": "1. Đổi loại proxy sang [bold]socks5[/bold] (hỗ trợ HTTPS tốt nhất)",
            "px_fix_2": "2. Kiểm tra proxy còn sống, host/port/user/mật khẩu đã đúng chưa",
            "px_fix_3": "3. Proxy HTTP free thường không tunnel được HTTPS",
            "px_not_configured": "Proxy chưa cấu hình hoặc đang tắt",
            "px_err_auth": "Proxy từ chối username / mật khẩu",
            "px_err_timeout": "Proxy phản hồi quá lâu (timeout)",
            "px_err_refused": "Không kết nối được tới proxy (sai host/port hoặc proxy đã chết)",
            "px_err_tunnel": "Proxy không tunnel được HTTPS — hãy thử socks5",
            # --- Luồng / premium / nâng cao ---
            "thr_header": "SỐ LUỒNG TẢI",
            "thr_current": "Luồng/file hiện tại:",
            "thr_est": "Tốc độ ước tính:",
            "thr_max": "tối đa",
            "thr_reco": "Khuyến nghị:",
            "thr_reco_val": "8–16 cho tốc độ tốt nhất",
            "thr_saved": "Luồng: {n} conn (~{speed} MB/s tối đa)",
            "prem_header": "CHẾ ĐỘ PREMIUM TRANSFER",
            "prem_on_desc": "Lưu TẤT CẢ file lên cloud trước khi tải",
            "prem_off_desc": "Chỉ lưu video/zip/rar/iso lên cloud (mặc định)",
            "prem_when_on": "Khi BẬT:",
            "prem_when_off": "Khi TẮT:",
            "adv_header": "CÀI ĐẶT NÂNG CAO",
            "adv_workers": "File đồng thời:",
            "adv_path": "Thư mục tải:",
            "adv_timeout": "Timeout:",
            "adv_saved": "✓ Đã lưu toàn bộ!",
            "path_invalid": "✖ Không tạo được thư mục này, giữ nguyên thư mục cũ",
            # --- Cache / cấu hình ---
            "cache_info": "THÔNG TIN CACHE",
            "cache_files": "Số lượng file",
            "cache_size": "Dung lượng",
            "cache_clear": "Xóa toàn bộ cache?",
            "cache_cleared": "✓ Đã dọn dẹp cache!",
            "cfg_path": "Thư mục tải:",
            "cfg_workers": "File đồng thời:",
            "cfg_timeout": "Timeout:",
            "cfg_lang": "Ngôn ngữ:",
            "cfg_proxy": "Proxy:",
            "cfg_prem": "Chế độ Premium:",
            "cfg_threads": "Luồng/file:",
            "cfg_accounts": "Tài khoản:",
            "cfg_log": "Mức log:",
            "cfg_log_file": "File log:",
            "log_prompt": "Mức log (DEBUG = rất chi tiết, dùng khi báo lỗi)",
            # --- Tải xuống ---
            "input_link": "Nhập Link PikPak",
            "input_link_hint": "(nhiều link thì cách nhau bằng dấu phẩy)",
            "input_pwd": "Mật khẩu (nếu có)",
            "analyzing": "➜ Đang phân tích cấu trúc thư mục...",
            "dl_analyzing": "➜ Đang phân tích:",
            "dl_load_fail": "✖ Bỏ qua (không tải được thông tin link)",
            "link_invalid": "✖ URL không hợp lệ!",
            "no_files": "✖ Không tìm thấy file trong liên kết này.",
            "link_info": "THÔNG TIN LINK",
            "links_ok": "Link hợp lệ",
            "total_files": "Tổng số file",
            "total_size": "Tổng dung lượng",
            "dl_opt_1": "Tải tất cả",
            "dl_opt_2": "Chọn file để tải",
            "dl_opt_0": "Hủy",
            "file_list": "DANH SÁCH FILE",
            "more_files": "... còn {n} file nữa",
            "dl_pick": "👉 Nhập ID (vd: 1-3,5)",
            "dl_starting": "⬇ Đang tải {n} file từ {m} link cùng lúc",
            "dl_cancel_hint": "Nhấn [bold]Q[/bold] để hủy",
            "dl_cancelling": "⛔ ĐANG HỦY...",
            "dl_cancelled": "⛔ Đã hủy – các file còn lại được bỏ qua.",
            "dl_cancel_title": "ĐÃ HỦY",
            "dl_cancel_by_user": "⛔ NGƯỜI DÙNG ĐÃ HỦY",
            "dl_cancel_done": "Hoàn tất trước khi hủy",
            "dl_cancel_left": "Đã dừng / chưa bắt đầu",
            "dl_result": "KẾT QUẢ",
            "dl_complete": "HOÀN TẤT!",
            "dl_success": "Thành công",
            "dl_failed": "Thất bại",
            "dl_skip": "Đã có (bỏ qua)",
            "dl_error": "Lỗi",
            "dl_retry_q": "⚠ Có {n} file bị lỗi. Thử lại?",
            "dl_auto_continue": "✓ Xong! Tự tiếp tục sau {n}s... (Enter để bỏ qua)",
            "err_disk_full": "✖ LỖI HỆ THỐNG: Ổ cứng đầy! Đã dừng tải.",
            # --- Cập nhật ---
            "update_check": "Đang kiểm tra bản cập nhật...",
            "update_found": "🚀 CÓ BẢN CẬP NHẬT MỚI",
            "update_ask_web": "Mở trình duyệt để tải ngay?",
            "update_ask_src": "Tự động cập nhật mã nguồn ngay?",
            "update_done": "✓ Cập nhật thành công! Đang khởi động lại...",
            "update_fail": "✖ Lỗi cập nhật",
            # --- Ngôn ngữ ---
            "lang_select": "Chọn ngôn ngữ / Select Language",
            "lang_en": "Tiếng Anh (English)",
            "lang_vi": "Tiếng Việt",
            "lang_set": "✓ Đã chuyển sang Tiếng Việt",
            # --- Bảng tiến trình ---
            "global_stats": "THỐNG KÊ TIẾN TRÌNH",
            "status_restore": "Đang lưu vào Cloud...",
            "status_check": "Đang kiểm tra...",
            "status_getlink": "Lấy link...",
            "status_dl": "Đang tải...",
            "status_clean": "Đang xóa...",
            # --- Tài khoản phụ ---
            "acc_header": "TÀI KHOẢN PHỤ",
            "acc_info": "Mỗi tài khoản phụ thêm ~11 MB/s vào tổng tốc độ.",
            "acc_list": "Danh sách tài khoản",
            "acc_add": "Thêm tài khoản phụ",
            "acc_remove": "Xóa tài khoản phụ",
            "acc_remove_prompt": "Số thứ tự cần xóa (1-{n})",
            "acc_test": "Kiểm tra tất cả tài khoản",
            "acc_none": "Chưa có tài khoản phụ nào.",
            "acc_added": "✓ Đã thêm tài khoản!",
            "acc_removed": "✓ Đã xóa tài khoản!",
            "acc_invalid": "✖ Số thứ tự không hợp lệ.",
            "acc_duplicate": "✖ Tài khoản này đã có trong danh sách.",
            "acc_relogin": "Đăng nhập lại tài khoản phụ",
            "acc_relogin_prompt": "Số thứ tự cần đăng nhập lại (1-{n})",
            "acc_relogin_done": "✓ Đã đăng nhập lại tài khoản #{n}!",
            "acc_relogin_needed": "hết phiên đăng nhập, cần đăng nhập lại",
            "acc_col_status": "Trạng thái",
            "acc_col_user": "User ID",
            "acc_st_ok": "✓ Hoạt động",
            "acc_st_relogin": "✖ Cần đăng nhập lại",
            "acc_st_error": "✖ Lỗi",
            "acc_st_unknown": "?",
            "warn_main_relogin": "⚠ Tài khoản chính đã hết phiên đăng nhập (PikPak thu hồi token). Chọn [1] để đăng nhập lại.",
            "warn_extra_relogin": "⚠ Tài khoản phụ #{n}: hết phiên đăng nhập. Vào [6] → [4] để đăng nhập lại.",
            "warn_acc_error": "⚠ Tài khoản {label} không kết nối được: {err}",
            "acc_pool_size": "Tài khoản đang hoạt động",
            "acc_pool_now": "Pool hiện có: {n} tài khoản (~{speed} MB/s tối đa)",
            "acc_active": "Hoạt động: {n} / {total} tài khoản",
            "acc_speed": "Tốc độ tối đa ước tính: ~{speed} MB/s",
            # --- Dọn cloud ---
            "gc_start": "🧹 Đang dọn file tạm còn sót trên cloud...",
            "gc_done": "✓ Đã dọn dẹp cloud xong!",
            "gc_freed": "➜ {acc}: đã xóa {n} file tạm.",
        },
    }

    @classmethod
    def get(cls, key, **kwargs):
        lang = Config.LANGUAGE
        text = cls.STRINGS.get(lang, cls.STRINGS["en"]).get(
            key, cls.STRINGS["en"].get(key, key)
        )
        if kwargs:
            try:
                return text.format(**kwargs)
            except (KeyError, IndexError, ValueError):
                return text
        return text


class Config:
    BASE_DIR = BASE_DIR
    CONFIG_FILE = BASE_DIR / "config.json"
    LOG_DIR = BASE_DIR / "logs"

    REFRESH_TOKEN = ""
    DEVICE_ID = ""
    MAIN_USER_ID = ""
    CAPTCHA_TOKEN = ""
    LANGUAGE = "en"
    USE_PROXY = False
    PROXY_TYPE = "http"
    PROXY_HOST = ""
    PROXY_PORT = ""
    PROXY_USERNAME = ""
    PROXY_PASSWORD = ""
    FORCE_PREMIUM_MODE = False
    MAX_WORKERS = 3
    DOWNLOAD_PATH_STR = "downloads"
    TIMEOUT = 30
    USE_CACHE = True
    CONCURRENT_THREADS = 8
    EXTRA_ACCOUNTS: list = []
    LOG_LEVEL = "INFO"

    # True nếu config.json không đọc được: khi đó KHÔNG ghi đè để khỏi mất token
    _LOAD_FAILED = False

    @classmethod
    def get_download_dir(cls):
        p = Path(os.path.expanduser(cls.DOWNLOAD_PATH_STR or "downloads"))
        return p if p.is_absolute() else cls.BASE_DIR / p

    @staticmethod
    def _int(value, default, lo=None, hi=None):
        try:
            v = int(value)
        except (TypeError, ValueError):
            return default
        if lo is not None:
            v = max(lo, v)
        if hi is not None:
            v = min(hi, v)
        return v

    @classmethod
    def load_config(cls):
        if cls.CONFIG_FILE.exists():
            try:
                with open(cls.CONFIG_FILE, "r", encoding="utf-8") as f:
                    d = json.load(f)
                if not isinstance(d, dict):
                    raise ValueError("config root is not an object")
            except Exception:
                # File hỏng: sao lưu lại một lần rồi dùng giá trị mặc định
                if not cls._LOAD_FAILED:
                    try:
                        shutil.copy2(
                            cls.CONFIG_FILE, cls.CONFIG_FILE.with_suffix(".json.bak")
                        )
                    except Exception:
                        pass
                cls._LOAD_FAILED = True
                d = {}
            else:
                cls._LOAD_FAILED = False

            cls.REFRESH_TOKEN = d.get("refresh_token", "") or ""
            cls.DEVICE_ID = d.get("device_id", "") or ""
            cls.MAIN_USER_ID = d.get("main_user_id", "") or ""
            cls.CAPTCHA_TOKEN = d.get("captcha_token", "") or ""
            cls.MAX_WORKERS = cls._int(d.get("max_workers"), 3, lo=1, hi=32)
            cls.DOWNLOAD_PATH_STR = d.get("download_path", "downloads") or "downloads"
            cls.TIMEOUT = cls._int(d.get("timeout"), 30, lo=5, hi=600)
            cls.USE_CACHE = bool(d.get("use_cache", True))
            lang = d.get("language", "en")
            cls.LANGUAGE = lang if lang in Language.STRINGS else "en"
            cls.USE_PROXY = bool(d.get("use_proxy", False))
            ptype = str(d.get("proxy_type", "http") or "http").lower()
            cls.PROXY_TYPE = (
                ptype if ptype in ("http", "https", "socks4", "socks5") else "http"
            )
            cls.PROXY_HOST = str(d.get("proxy_host", "") or "")
            cls.PROXY_PORT = str(d.get("proxy_port", "") or "")
            cls.PROXY_USERNAME = str(d.get("proxy_username", "") or "")
            cls.PROXY_PASSWORD = str(d.get("proxy_password", "") or "")
            cls.CONCURRENT_THREADS = cls._int(
                d.get("concurrent_threads"), 8, lo=1, hi=64
            )
            cls.FORCE_PREMIUM_MODE = bool(d.get("force_premium_mode", False))
            accs = d.get("extra_accounts", [])
            cls.EXTRA_ACCOUNTS = [a for a in accs if isinstance(a, dict)] if isinstance(accs, list) else []
            cls.LOG_LEVEL = normalize_level(d.get("log_level"))
            set_log_level(cls.LOG_LEVEL)
        if not cls.DEVICE_ID:
            cls.DEVICE_ID = uuid.uuid4().hex

    @classmethod
    def save_config(cls):
        try:
            data = {
                "refresh_token": cls.REFRESH_TOKEN,
                "device_id": cls.DEVICE_ID,
                "main_user_id": cls.MAIN_USER_ID,
                "captcha_token": cls.CAPTCHA_TOKEN,
                "max_workers": cls.MAX_WORKERS,
                "download_path": cls.DOWNLOAD_PATH_STR,
                "timeout": cls.TIMEOUT,
                "use_cache": cls.USE_CACHE,
                "language": cls.LANGUAGE,
                "use_proxy": cls.USE_PROXY,
                "proxy_type": cls.PROXY_TYPE,
                "proxy_host": cls.PROXY_HOST,
                "proxy_port": cls.PROXY_PORT,
                "proxy_username": cls.PROXY_USERNAME,
                "proxy_password": cls.PROXY_PASSWORD,
                "concurrent_threads": cls.CONCURRENT_THREADS,
                "force_premium_mode": cls.FORCE_PREMIUM_MODE,
                "extra_accounts": cls.EXTRA_ACCOUNTS,
                "log_level": cls.LOG_LEVEL,
            }
            # Ghi ra file tạm rồi thay thế, tránh config.json bị cụt khi tắt ngang
            tmp = cls.CONFIG_FILE.with_suffix(".json.tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            os.replace(tmp, cls.CONFIG_FILE)
            cls._LOAD_FAILED = False
            return True
        except Exception:
            return False

    @classmethod
    def migrate_config(cls):
        if not cls.CONFIG_FILE.exists():
            return
        cls.load_config()
        if not cls._LOAD_FAILED:
            cls.save_config()

    @classmethod
    def setup_dirs(cls):
        try:
            cls.get_download_dir().mkdir(parents=True, exist_ok=True)
            return True
        except Exception:
            return False
