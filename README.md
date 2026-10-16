# PikPak Downloader

Công cụ dòng lệnh giúp tải file/thư mục từ link chia sẻ PikPak về máy bằng chính tài khoản của bạn — không cần cài app PikPak.

Chạy được trên **Windows**, **Android** (qua Termux) và **iOS/iPadOS** (qua iSH).

---

## Mục lục

- [Tính năng](#tính-năng)
- [Yêu cầu](#yêu-cầu)
- [Cài đặt trên Windows](#cài-đặt-trên-windows)
- [Cài đặt trên Android (Termux)](#cài-đặt-trên-android-termux)
- [Cài đặt trên iOS / iPadOS (iSH)](#cài-đặt-trên-ios--ipados-ish)
- [Hướng dẫn sử dụng](#hướng-dẫn-sử-dụng)
- [Cấu hình config.json](#cấu-hình-configjson)
- [Cập nhật phiên bản mới](#cập-nhật-phiên-bản-mới)
- [Lỗi thường gặp](#lỗi-thường-gặp)
- [Dành cho người phát triển](#dành-cho-người-phát-triển)
- [Miễn trừ trách nhiệm — Disclaimer](#miễn-trừ-trách-nhiệm--disclaimer)

---

## Tính năng

- Đăng nhập và tải bằng chính tài khoản PikPak của bạn.
- Dán nhiều link cùng lúc (cách nhau bằng dấu phẩy), chọn file cần tải trong danh sách.
- Tải đa luồng; có thể thêm **tài khoản phụ** để tăng tốc độ.
- Với video, tool tự lưu vào cloud của bạn, tải xong sẽ tự xóa để không tốn dung lượng.
- Hỗ trợ proxy **http / https / socks4 / socks5** (có đăng nhập), dán nhanh chuỗi proxy và tự kiểm tra kết nối tới PikPak.
- Đổi thư mục lưu, số luồng, timeout.
- Giao diện tiếng Việt và tiếng Anh.
- Tốc độ phụ thuộc vào mạng của bạn (đã test ổn định từ 10 Mb/s trở lên).

---

## Yêu cầu

- **Python 3.11 trở lên**
- Các thư viện trong [requirements.txt](requirements.txt): `aiohttp`, `aiohttp-socks`, `aiofiles`, `requests`, `rich`
  (lần chạy đầu tool sẽ tự cài nếu thiếu, nhưng nên cài trước theo hướng dẫn bên dưới)

---

## Cài đặt trên Windows

### Bước 1 — Cài Python

1. Tải Python tại: https://www.python.org/downloads/
2. Mở file cài đặt, **tích vào ô `Add python.exe to PATH`** ở màn hình đầu tiên, rồi bấm **Install Now**.
3. Kiểm tra: mở **Command Prompt** (hoặc PowerShell) và gõ:
   ```bat
   python --version
   ```
   Nếu hiện `Python 3.11.x` trở lên là được.

### Bước 2 — Tải mã nguồn

**Cách 1 (đơn giản):** tải file ZIP tại
https://github.com/SakerLy/SupportDLPikPak/archive/refs/heads/main.zip
rồi giải nén ra một thư mục, ví dụ `D:\PikPakTool`.

**Cách 2 (dùng Git):**
```bat
git clone https://github.com/SakerLy/SupportDLPikPak.git
cd SupportDLPikPak
```

### Bước 3 — Cài thư viện

Mở thư mục vừa giải nén, bấm vào thanh địa chỉ của File Explorer, gõ `cmd` rồi Enter để mở Command Prompt ngay tại thư mục đó. Sau đó chạy:

```bat
python -m pip install -r requirements.txt
```

### Bước 4 — Chạy tool

```bat
python main.py
```

> 💡 Nên dùng **Windows Terminal** (có sẵn trên Windows 11) để giao diện hiển thị màu và khung viền đẹp nhất.
>
> 💡 Nếu trang [Releases](https://github.com/SakerLy/SupportDLPikPak/releases) có bản `.exe`, bạn có thể tải về chạy trực tiếp mà không cần cài Python.

---

## Cài đặt trên Android (Termux)

### Bước 1 — Cài Termux

Tải Termux từ **F-Droid** (https://f-droid.org/packages/com.termux/) hoặc **GitHub** (https://github.com/termux/termux-app/releases).

> ⚠️ Không dùng bản Termux trên Google Play — bản đó đã cũ và hay lỗi khi cài gói.

### Bước 2 — Cài Python và công cụ cần thiết

Mở Termux và chạy lần lượt:

```bash
pkg update && pkg upgrade -y
pkg install -y python git build-essential
```

### Bước 3 — Cấp quyền truy cập bộ nhớ

```bash
termux-setup-storage
```

Bấm **Cho phép** khi Android hỏi. Sau bước này, bộ nhớ máy nằm ở `~/storage/shared` (tương ứng `/storage/emulated/0`).

### Bước 4 — Tải mã nguồn và cài thư viện

```bash
git clone https://github.com/SakerLy/SupportDLPikPak.git
cd SupportDLPikPak
pip install -r requirements.txt
```

### Bước 5 — Chạy tool

```bash
python main.py
```

### Mẹo cho Android

- **Lưu file ra bộ nhớ máy** để xem được trong app Thư viện / Quản lý file: vào menu **Settings → Advanced** và đặt thư mục tải là
  ```
  /storage/emulated/0/Download/PikPak
  ```
- **Tránh bị Android tắt khi tải lâu:** chạy `termux-wake-lock` trước khi tải (hoặc kéo thanh thông báo của Termux và bấm *Acquire wakelock*), đồng thời tắt tối ưu pin cho Termux.
- Lần sau mở lại tool:
  ```bash
  cd ~/SupportDLPikPak && python main.py
  ```

---

## Cài đặt trên iOS / iPadOS (iSH)

iOS không cho chạy Python trực tiếp, nên ta dùng **iSH** — một môi trường Linux (Alpine) chạy trong app.

> ⚠️ iSH giả lập CPU nên chạy chậm hơn nhiều so với máy tính hay Android. Tốc độ tải trên iPhone/iPad sẽ thấp hơn — phù hợp cho file nhỏ hoặc khi không có máy tính.

### Bước 1 — Cài iSH

Tải **iSH Shell** miễn phí trên App Store: https://apps.apple.com/app/ish-shell/id1436902243

### Bước 2 — Cài Python và thư viện

Mở iSH và chạy:

```sh
apk update
apk add python3 py3-pip git py3-aiohttp py3-aiofiles py3-requests py3-rich
pip install --break-system-packages aiohttp-socks
```

### Bước 3 — Tải mã nguồn và chạy

```sh
git clone https://github.com/SakerLy/SupportDLPikPak.git
cd SupportDLPikPak
python3 main.py
```

### Mẹo cho iOS

- **Để file tải về hiện trong app Tệp (Files):** chạy lệnh sau, rồi chọn một thư mục trong app Tệp (ví dụ *On My iPhone → Downloads*):
  ```sh
  mkdir -p /mnt/ios
  mount -t ios . /mnt/ios
  ```
  Sau đó vào **Settings → Advanced** trong tool và đặt thư mục tải là `/mnt/ios`.
- iOS có thể tạm dừng iSH khi bạn thoát ra màn hình chính. Giữ iSH mở trong lúc tải (có thể bật *Guided Access* hoặc tắt khóa màn hình tự động).
- Lần sau mở lại tool:
  ```sh
  cd ~/SupportDLPikPak && python3 main.py
  ```

---

## Hướng dẫn sử dụng

Khi chạy, tool hiện menu chính:

| Phím | Chức năng |
|------|-----------|
| `1` | Đăng nhập tài khoản PikPak (email / số điện thoại / username + mật khẩu) |
| `2` | Tải từ link — dán một hoặc nhiều link, cách nhau bằng dấu phẩy |
| `3` | Cài đặt: proxy, số luồng, thư mục tải, timeout, ngôn ngữ, Premium Transfer Mode |
| `4` | Quản lý cache |
| `5` | Xem cấu hình hiện tại |
| `6` | Quản lý tài khoản phụ (thêm tài khoản để tăng tốc độ tải) |
| `0` | Thoát |

**Lần đầu sử dụng:**

1. Chọn `3` → đổi ngôn ngữ sang tiếng Việt nếu muốn.
2. Chọn `1` để đăng nhập.
3. Chọn `2`, dán link PikPak và chọn file muốn tải.

File tải về mặc định nằm trong thư mục `downloads` cạnh `main.py`. Tên file có ký tự Windows không cho phép (`: ? * " < > |`) sẽ được tự đổi thành `_`.

**Dùng proxy:** vào `3` → `1`, chọn **[3] Dán chuỗi proxy** và dán theo một trong các dạng:

```
socks5://user:pass@host:port
http://host:port
user:pass@host:port
host:port:user:pass
```

Tool sẽ lưu và tự kiểm tra 3 bước **HTTP → HTTPS → PikPak**. Proxy áp dụng cho cả đăng nhập, gọi API PikPak và tải file (riêng kiểm tra bản cập nhật GitHub thì không); nên dùng `socks5` vì ổn định nhất.

**Các file tool tự tạo:**

- `config.json` — lưu cài đặt và **token đăng nhập**. Không chia sẻ file này cho người khác.
- `logs/` — nhật ký chạy: mỗi lần mở tool là một file riêng theo ngày giờ, ví dụ `pikpak_tool_2026-09-24_14-05-33.log` (giữ 30 lần chạy gần nhất). Khi báo lỗi, gửi kèm file của lần chạy bị lỗi.
- `.cache/` — bộ nhớ đệm, có thể xóa bằng menu `4`.
- `cloud_temp.json` — danh sách file tạm tool đã lưu lên cloud. Tool chỉ tự xóa những file có trong danh sách này, **không bao giờ đụng tới file riêng của bạn** trên PikPak.

---

## Cấu hình config.json

Hầu hết cài đặt đều chỉnh được trong menu `3`. Nếu muốn sửa tay, **tắt tool trước**, mở `config.json` (cạnh `main.py`) bằng Notepad hoặc VS Code, sửa rồi lưu lại.

| Khóa | Mặc định | Ý nghĩa |
|------|----------|---------|
| `language` | `"en"` | Ngôn ngữ giao diện: `"en"` hoặc `"vi"` |
| `download_path` | `"downloads"` | Thư mục lưu file. Đường dẫn tương đối tính từ thư mục tool; trên Windows dùng `/` thay cho `\`, ví dụ `"D:/Phim"` (hoặc viết đôi `"D:\\Phim"`) |
| `max_workers` | `3` | Số file tải cùng lúc (1–32) |
| `concurrent_threads` | `8` | Số luồng tối thiểu cho mỗi file (1–64); file lớn tự dùng nhiều hơn |
| `timeout` | `30` | Thời gian chờ mỗi request API, tính bằng giây (5–600) |
| `use_cache` | `true` | Lưu tạm danh sách file của link để lần sau mở nhanh hơn |
| `force_premium_mode` | `false` | `true`: lưu mọi file lên cloud trước khi tải (mặc định chỉ video/zip/rar/iso) |
| `use_proxy` | `false` | Bật / tắt proxy |
| `proxy_type` | `"http"` | `"http"`, `"https"`, `"socks4"` hoặc `"socks5"` |
| `proxy_host`, `proxy_port` | `""` | Địa chỉ và cổng proxy |
| `proxy_username`, `proxy_password` | `""` | Tài khoản proxy (để trống nếu không cần) |
| `log_level` | `"INFO"` | Mức chi tiết của log, xem bảng dưới |

> ⚠️ Không sửa tay các khóa `refresh_token`, `device_id`, `main_user_id`, `captcha_token`, `extra_accounts`: đây là dữ liệu đăng nhập do tool tự quản lý. Giá trị sai kiểu (vd. chữ thay vì số) sẽ được tự đưa về mặc định; nếu file bị hỏng hẳn, tool giữ nguyên file và tạo bản sao `config.json.bak`.

### Mức log (`log_level`)

Mỗi mức ghi chính nó **và mọi mức nặng hơn** (INFO ghi cả WARNING và ERROR). Có thể đổi trong menu `3` → `3` (Nâng cao), có hiệu lực ngay không cần mở lại tool.

| Mức | Ghi những gì | Khi nào dùng |
|-----|--------------|--------------|
| `DEBUG` | Mọi request HTTP, từng đoạn tải, từng lần thử lại, traceback đầy đủ | Khi gặp lỗi và cần gửi log cho tác giả. File log rất lớn, tải xong nhớ đổi lại `INFO` |
| `INFO` | Các mốc chính: đăng nhập, số tài khoản, bắt đầu / xong / bỏ qua từng file, dọn cloud, kết quả kiểm tra proxy | **Mặc định**, dùng hằng ngày |
| `WARNING` | Sự cố tool tự xử lý được: mất mạng tạm thời, thử lại, account đầy phải đổi account, chuyển sang tải 1 luồng | Chỉ muốn thấy bất thường |
| `ERROR` | Chỉ lỗi làm hỏng việc: file tải thất bại, ổ cứng đầy, token hết hạn, lỗi không mong muốn | Log gọn nhất |

Ví dụ:

```json
"log_level": "DEBUG",
```

---

## Cập nhật phiên bản mới

Tool tự kiểm tra bản mới khi khởi động. Để cập nhật thủ công:

- **Dùng Git** (Windows / Termux / iSH): trong thư mục tool, chạy `git pull`.
- **Dùng ZIP:** tải ZIP mới, giải nén đè lên thư mục cũ. Giữ lại file `config.json` để không phải đăng nhập lại.

---

## Lỗi thường gặp

| Lỗi | Cách xử lý |
|-----|-----------|
| `'python' is not recognized...` (Windows) | Cài lại Python và nhớ tích **Add python.exe to PATH**, hoặc dùng lệnh `py main.py`. |
| `Could not import modules` | Hãy chạy `main.py` từ đúng thư mục gốc của tool (thư mục chứa `main.py`). |
| Cài `aiohttp` bị lỗi trên Termux | Chạy `pkg install build-essential` rồi cài lại. |
| `No module named 'aiohttp_socks'` | Chạy `pip install aiohttp-socks` (hoặc `pip install -r requirements.txt`). |
| Proxy HTTP không tải được | Đổi loại proxy sang `socks5` trong **Settings → Proxy**, rồi chọn **Kiểm tra proxy**. |
| `config.json` bị hỏng | Tool giữ bản sao ở `config.json.bak` và không ghi đè file hỏng; sửa hoặc xóa file rồi đăng nhập lại. |
| Báo ổ cứng đầy | Giải phóng dung lượng hoặc đổi thư mục tải sang ổ khác. |
| Giao diện bị vỡ khung / lỗi font | Dùng Windows Terminal (Windows) hoặc thu nhỏ cỡ chữ / xoay ngang màn hình (điện thoại). |

---

## Dành cho người phát triển

```bash
python -m pip install -r requirements-dev.txt
python -m pytest
```

Test không cần mạng hay tài khoản PikPak: phần tải đa luồng được kiểm tra với một server HTTP cục bộ.

---

## Miễn trừ trách nhiệm — Disclaimer

Công cụ này không phải sản phẩm chính thức của PikPak.
Token, dữ liệu và tài khoản người dùng do bạn tự chịu trách nhiệm.
Công cụ không thay thế hoàn toàn gói Premium của PikPak. Một số tính năng như tốc độ cao, xử lý nhanh, dung lượng lưu trữ vẫn cần Premium.
Hãy sử dụng đúng luật và đúng điều khoản dịch vụ.

This tool is not an official PikPak product.
All tokens, data, and account usage are your own responsibility.
This tool does not fully replace PikPak Premium; features like speed boost, instant processing, and storage quota still require Premium.
Use responsibly and comply with legal and service rules.

---

Phát triển bởi **SakerLy** — xem giấy phép tại [LICENSE](LICENSE).
