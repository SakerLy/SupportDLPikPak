import asyncio

from core.pikpak.garbage_collector import CloudGarbageCollector
from core.logger import logger

# Stripe đa account: PikPak giới hạn tốc độ theo account (~11 MB/s),
# nên file lớn được restore vào nhiều account để cộng dồn băng thông
STRIPE_MIN_SIZE = 300 * 1024 * 1024
MAX_STRIPE_ACCOUNTS = 3


class CloudRestoreMixin:
    @staticmethod
    def _is_storage_full(error: str) -> bool:
        e = (error or "").lower()
        if "file_space_not_enough" in e or "space_not_enough" in e:
            return True
        return any(w in e for w in ("space", "storage")) and any(
            k in e for k in ("enough", "full", "insufficient")
        )

    async def _run_cloud_gc(self, apis) -> bool:
        # Chỉ chạy 1 lần cho mỗi batch tải, bỏ qua các file đang được task khác sử dụng
        async with self._gc_lock:
            if self._gc_done:
                return False
            self._gc_done = True
            logger.info(
                "Tất cả account đầy dung lượng — chạy Cloud GC để giải phóng file rác..."
            )
            gc = CloudGarbageCollector(quiet=True)
            freed = 0
            for i, a in enumerate(apis):
                freed += await gc.clean_account(
                    a, f"#{i + 1}", exclude_ids=self.active_cloud_ids
                )
            if freed:
                self.full_accounts.clear()
                logger.info(f"Cloud GC giải phóng {freed} file. Thử restore lại...")
                return True
            logger.warning("Cloud GC không tìm thấy file rác nào để giải phóng.")
            return False

    async def _restore_with_failover(
        self, apis, name, size, share_id, src_file_id, pass_token
    ):
        for round_no in range(2):
            full_seen = False
            for idx, a in enumerate(apis):
                if self.cancel_event.is_set():
                    return None, None, False
                if id(a) in self.full_accounts:
                    full_seen = True
                    continue

                label = f"#{idx + 1}"
                if not await a.ensure_token():
                    logger.warning(f"Account {label}: xác thực thất bại, bỏ qua.")
                    continue

                # Xóa bản kẹt do tool tạo từ lần chạy trước (nếu có) để giải phóng chỗ.
                # File trùng tên mà KHÔNG do tool tạo là file của người dùng: giữ nguyên.
                preexisting = None
                stale = await a.wait_for_file(name, max_retries=1, expected_size=size)
                if stale and stale not in self.active_cloud_ids:
                    if a.is_temp_file(stale):
                        try:
                            await asyncio.wait_for(a.delete_file(stale), timeout=10)
                        except Exception:
                            pass
                    else:
                        preexisting = stale

                fid, error = await a.restore_and_poll(share_id, src_file_id, pass_token)

                if fid:
                    logger.info(
                        f"Đã lưu '{name}' vào Account {label}. Tiến hành lấy link tải."
                    )
                    return fid, a, True

                if self._is_storage_full(error):
                    self.full_accounts.add(id(a))
                    full_seen = True
                    logger.warning(
                        f"Account {label} đã đầy dung lượng. Chuyển sang account tiếp theo... ({error})"
                    )
                    continue

                fid = await a.wait_for_file(name, max_retries=8, expected_size=size)
                if fid:
                    owned = fid != preexisting
                    if owned:
                        a.track_temp_file(fid)
                    logger.info(
                        f"Restore báo lỗi nhưng tìm thấy file trên Account {label} (file của tool: {owned})."
                    )
                    return fid, a, owned
                logger.warning(
                    f"Account {label} lưu thất bại: {error}. Đang thử account khác..."
                )

            if not full_seen or round_no == 1:
                return None, None, False
            if not await self._run_cloud_gc(apis):
                return None, None, False
        return None, None, False

    async def _restore_replicas(
        self, apis, primary_api, share_id, src_file_id, pass_token
    ):
        # Restore thêm bản sao vào các account khác để stripe băng thông
        candidates = [
            a for a in apis if a is not primary_api and id(a) not in self.full_accounts
        ][: MAX_STRIPE_ACCOUNTS - 1]
        if not candidates:
            return []

        async def _one(a):
            try:
                if not await a.ensure_token():
                    return None
                fid, error = await a.restore_and_poll(share_id, src_file_id, pass_token)
                if fid:
                    return (a, fid)
                if self._is_storage_full(error):
                    self.full_accounts.add(id(a))
                return None
            except Exception:
                return None

        results = await asyncio.gather(*[_one(a) for a in candidates])
        return [r for r in results if r]
