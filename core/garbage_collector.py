import asyncio
from core.account_pool import get_pool
from core.api import PikPakAPI
from core.utils import HttpClient
from config.settings import console
from core.logger import logger


class CloudGarbageCollector:
    def __init__(self, target_folder_name="PikPak_Temp_DL", quiet=False):
        self.target_folder_name = target_folder_name
        self.quiet = quiet
        self.batch_size = 50
        self.max_concurrent = 3

    async def run(self, exclude_ids=None) -> int:
        pool = get_pool()
        apis = pool.all_apis()
        if not apis:
            logger.info("Garbage Collector: Không có tài khoản nào sẵn sàng.")
            return 0

        if not self.quiet:
            console.print(
                "\n[bold yellow]🧹 Đang khởi chạy Cloud Garbage Collector...[/]"
            )

        tasks = []
        for i, api in enumerate(apis):
            acc_name = "MAIN" if i == 0 else f"EXTRA_{i}"
            tasks.append(self.clean_account(api, acc_name, exclude_ids))
        results = await asyncio.gather(*tasks)

        if not self.quiet:
            console.print("[bold green]✓ Hoàn tất kiểm tra và dọn dẹp Cloud![/]\n")
        return sum(results)

    async def clean_account(
        self, api: PikPakAPI, acc_name: str, exclude_ids=None
    ) -> int:
        exclude_ids = exclude_ids or set()
        try:
            code, data, _ = await HttpClient.request(
                "GET",
                f"{api.BASE_URL}/drive/v1/files",
                headers=api.headers,
                params={"parent_id": "root", "limit": 100, "with_audit": "false"},
            )
            if not data or "files" not in data:
                return 0

            orphan_file_ids = [
                f["id"]
                for f in data["files"]
                if f.get("kind") == "drive#file"
                and not f.get("trashed", False)
                and f["id"] not in exclude_ids
            ]
            if not orphan_file_ids:
                return 0

            logger.info(
                f"[{acc_name}] Tìm thấy {len(orphan_file_ids)} file rác. Đang dọn dẹp..."
            )

            batches = [
                orphan_file_ids[i : i + self.batch_size]
                for i in range(0, len(orphan_file_ids), self.batch_size)
            ]
            semaphore = asyncio.Semaphore(self.max_concurrent)

            async def _delete_batch(batch_ids):
                async with semaphore:
                    for attempt in range(3):
                        code, resp, _ = await HttpClient.request(
                            "POST",
                            f"{api.BASE_URL}/drive/v1/files:batchDelete",
                            headers=api.headers,
                            json_data={"ids": batch_ids},
                        )
                        if code == 200:
                            return True
                        if code in (429, 503):
                            await asyncio.sleep(2 * (attempt + 1))
                        else:
                            break
                    return False

            results = await asyncio.gather(*[_delete_batch(b) for b in batches])
            success_count = sum(len(b) for r, b in zip(results, batches) if r)
            if success_count > 0:
                logger.info(
                    f"[{acc_name}] Giải phóng thành công {success_count} file rác."
                )
                if not self.quiet:
                    console.print(
                        f"  [dim]➜ {acc_name}: Giải phóng thành công {success_count} file rác.[/dim]"
                    )
            return success_count

        except Exception as e:
            logger.error(f"[{acc_name}] Lỗi Garbage Collector: {e}")
            return 0
