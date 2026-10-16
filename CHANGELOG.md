# Changelog

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
