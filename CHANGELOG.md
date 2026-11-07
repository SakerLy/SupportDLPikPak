# Changelog

## v0.1.1 — 2026-11-07

### Bảo mật

- **Bật lại kiểm tra chứng chỉ HTTPS khi đăng nhập và gọi API PikPak.** Trước đây tool không kiểm tra, nên một proxy hoặc mạng Wi-Fi công cộng xấu có thể đọc được mật khẩu và token của bạn.

### Sửa lỗi

- **Tải nhiều link có file trùng tên** (vd. hai link cùng có `01.mp4`) không còn ghi đè hoặc làm hỏng nhau: file thứ hai được đặt vào thư mục riêng. Dán cùng một link hai lần cũng không tải trùng nữa.
- **Lượt tải dài hơn 20 phút** không còn bị lỗi "Auth Fail" hay bắt đăng nhập lại do nhiều file cùng làm mới token một lúc.
- **Thư mục nhiều file** bị lỗi mạng khi đang đọc danh sách giờ báo lỗi, thay vì âm thầm bỏ sót file (và nhớ danh sách thiếu đó 30 phút).
- **Không còn xóa file đã có trên máy** khi tải lỗi hoặc bấm `Q` hủy; file cũ chỉ bị thay khi bản mới tải xong.
- **Mất điện giữa lúc tải** không còn để lại file hỏng bị coi là đã xong khi tải tiếp.
- Bảng tiến trình: các lỗi "No Link", "Auth Fail", "Err 403"... hiện màu đỏ; ô Queue không đếm file lỗi.

### Cải tiến

- **Tự cập nhật tải đúng bản release** thay vì bản đang phát triển, và không làm hỏng thư mục tool nếu cập nhật lỗi giữa chừng.
- Bỏ thư viện `requests` (không cần cài nữa).
- Tự dọn file cache cũ hơn 1 ngày khi mở tool.

## v0.1.0 — 2026-10-16

### Sửa lỗi

- **File tải về có thể bị hỏng mà không báo lỗi.** Khi server ngắt kết nối giữa chừng, phần dữ liệu còn thiếu vẫn bị coi là đã tải xong. Giờ tool kiểm tra đủ dung lượng từng đoạn và tự tải lại phần thiếu.
- **Tắt tool ngay lúc vừa bắt đầu tải** có thể khiến lần chạy sau coi file tạm (chưa có dữ liệu) là đã tải xong. Đã sửa.
- **Windows:** không còn lỗi khi thư mục tải đã có sẵn file cùng tên nhưng khác dung lượng.
- Thư mục tải tự chọn (vd. `D:/Phim`) giờ được tạo ngay khi mở tool, thay vì luôn tạo thư mục `downloads` mặc định.
- Dòng "Tự tiếp tục sau Ns..." sau khi tải xong giờ đếm ngược trên cùng một dòng, không in thành nhiều dòng nữa.

### Cải tiến

- **Proxy áp dụng cho cả đăng nhập và mọi thao tác với PikPak** (phân tích link, lưu lên cloud, lấy link tải), không chỉ riêng việc tải file. Hữu ích khi mạng của bạn bị PikPak chặn.
- **Tải nhiều file nhỏ nhanh hơn**: các file trong cùng một lượt tải dùng lại kết nối, không phải kết nối lại cho từng file. Khi dùng proxy, mỗi file ảnh 1–3 MB nhanh hơn khoảng 30%.
- **Mở tool nhanh hơn khi có nhiều tài khoản phụ**: các tài khoản được đăng nhập cùng lúc thay vì lần lượt.
- Giảm số request gửi tới PikPak khi lưu video lên cloud và khi lấy link tải, giúp ít bị giới hạn tốc độ hơn khi tải nhiều file.
- Nếu server không hỗ trợ tải đa luồng, tool chuyển ngay sang tải 1 luồng thay vì thử lại nhiều lần.

### Cập nhật

- `config.json` (cài đặt, đăng nhập) được giữ nguyên, không cần đăng nhập lại.
- Nếu cập nhật bằng cách giải nén ZIP đè lên thư mục cũ, có thể xóa thư mục `core` cũ trước khi giải nén cho gọn (không bắt buộc, file cũ không còn được dùng).

### Dành cho người phát triển

- Mã nguồn được chia lại theo chức năng: `core/download`, `core/pikpak`, `core/net`; giao diện tách thành nhiều file trong `ui/`; chuỗi giao diện chuyển sang `config/i18n.py`.
- Thêm bộ test (`python -m pytest`, cài bằng `requirements-dev.txt`), không cần mạng hay tài khoản PikPak.

---

### English summary

- **Fixed:** downloads could be silently corrupted when the server closed a connection early; a freshly started temp file could be treated as finished after an abrupt exit; Windows error when a same-named file already existed; custom download folder not created at startup; the "Continuing in Ns..." countdown printing multiple lines.
- **Improved:** proxy now covers login and all PikPak API calls; connections are reused across files (≈30% faster for small files over a proxy); extra accounts log in in parallel at startup; fewer API requests per file; immediate fallback to single-stream when the server doesn't support ranges.
- **Update notes:** `config.json` is kept, no re-login needed.
- **Developers:** code reorganized into `core/download`, `core/pikpak`, `core/net`; UI split into several modules; strings moved to `config/i18n.py`; added a pytest suite.
