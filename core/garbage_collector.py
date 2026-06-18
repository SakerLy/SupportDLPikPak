import asyncio
from core.account_pool import get_pool
from core.api import PikPakAPI
from core.utils import HttpClient
from config.settings import console
from core.logger import logger


class CloudGarbageCollector:
    def __init__(self, target_folder_name="PikPak_Temp_DL"):
        self.target_folder_name = target_folder_name
        self.batch_size = 50  # Số lượng ID tối đa cho một lần gọi API batchDelete
        self.max_concurrent = (
            3  # Giới hạn số luồng xóa đồng thời để tránh lỗi 429 Rate Limit
        )

    async def run(self):
        pool = get_pool()
        apis = pool.all_apis()

        if not apis:
            logger.info("Garbage Collector: Không có tài khoản nào sẵn sàng.")
            return

        console.print("\n[bold yellow]🧹 Đang khởi chạy Cloud Garbage Collector...[/]")

        tasks = []
        for i, api in enumerate(apis):
            acc_name = "MAIN" if i == 0 else f"EXTRA_{i}"
            tasks.append(self._clean_account(api, acc_name))

        await asyncio.gather(*tasks)
        console.print("[bold green]✓ Hoàn tất kiểm tra và dọn dẹp Cloud![/]\n")

    async def _clean_account(self, api: PikPakAPI, acc_name: str):
        try:
            # 1. Quét tìm các file mồ côi ở thư mục root
            code, data, _ = await HttpClient.request(
                "GET",
                f"{api.BASE_URL}/drive/v1/files",
                headers=api.headers,
                params={"parent_id": "root", "limit": 100, "with_audit": "false"},
            )

            if not data or "files" not in data:
                return

            # Chỉ lọc ra các file (không phải folder) chưa bị xóa vào thùng rác
            orphan_file_ids = [
                f["id"]
                for f in data["files"]
                if f.get("kind") == "drive#file" and not f.get("trashed", False)
            ]

            if not orphan_file_ids:
                return

            logger.info(
                f"[{acc_name}] Tìm thấy {len(orphan_file_ids)} file rác. Đang dọn dẹp..."
            )

            # 2. Chia nhỏ thành các batch để gọi hàm batchDelete
            batches = [
                orphan_file_ids[i : i + self.batch_size]
                for i in range(0, len(orphan_file_ids), self.batch_size)
            ]

            semaphore = asyncio.Semaphore(self.max_concurrent)

            async def _delete_batch(batch_ids):
                async with semaphore:
                    for attempt in range(3):  # Retry logic khi gặp HTTP 429
                        code, resp, _ = await HttpClient.request(
                            "POST",
                            f"{api.BASE_URL}/drive/v1/files:batchDelete",
                            headers=api.headers,
                            json_data={"ids": batch_ids},
                        )
                        if code == 200:
                            return True
                        elif code in (429, 503):
                            # Backoff tuyến tính nếu bị dính Rate Limit
                            await asyncio.sleep(2 * (attempt + 1))
                        else:
                            break
                    return False

            # Dùng asyncio.gather để gọi nhiều batch cùng lúc
            delete_tasks = [_delete_batch(b) for b in batches]
            results = await asyncio.gather(*delete_tasks)

            success_count = sum(len(b) for r, b in zip(results, batches) if r)
            if success_count > 0:
                console.print(
                    f"  [dim]➜ {acc_name}: Giải phóng thành công {success_count} file rác.[/dim]"
                )

        except Exception as e:
            logger.error(f"[{acc_name}] Lỗi Garbage Collector: {e}")
