# 📦 PikPakDownloader

**PikPakDownloader** là công cụ hỗ trợ tải file/folder từ dịch vụ **PikPak** một cách nhanh chóng, tự động và tiện lợi.  
Phần mềm hỗ trợ chạy đa nền tảng và có thể build thành file `.exe` để sử dụng trên Windows.

---

## 🚀 Tính năng chính

- 🔗 Lấy link tải trực tiếp (direct download) từ PikPak  
- 📂 Xem & tải cả file lẫn thư mục trong Share Link  
- 🔐 Tự động đăng nhập, refresh token (nếu có tài khoản)  
- ⚙️ Không cần đăng nhập nếu dùng Share URL  
- 📁 Hỗ trợ tải hàng loạt  
- 🖥️ Chạy được trên:  
  - Windows (EXE)  
  - Android (Termux)  
- 🚫 Không cần cài app PikPak

---

## 🛠 Cài đặt & chạy bằng Python

### 1. Cài Python 3.11
Tải tại: [https://www.python.org/downloads/](https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe)

### 2. Cài thư viện cần thiết

```bash
pip install requests
```
### 3. Chạy chương trình
```bash
python pikpak_downloader.py
```
