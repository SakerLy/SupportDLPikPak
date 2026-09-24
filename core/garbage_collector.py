import asyncio
from core.account_pool import get_pool
from core.api import PikPakAPI
from core.utils import HttpClient
from core import cloud_ledger
from config.settings import console, Language
from core.logger import logger


class CloudGarbageCollector:
    """Dọn các file tạm mà tool đã restore lên cloud nhưng chưa kịp xóa
    (tắt ngang, mất mạng...). Chỉ xóa file có trong cloud ledger — không bao giờ
    đụng tới file riêng của người dùng."""

    def __init__(self, quiet=False):
        self.quiet = quiet
        self.batch_size = 50
        self.max_concurrent = 3

    async def run(self, exclude_ids=None) -> int:
        pool = get_pool()
        apis = pool.all_apis()
        if not apis:
            logger.info("Garbage Collector: Không có tài khoản nào sẵn sàng.")
            return 0
        if not any(cloud_ledger.get(a.account_key) for a in apis):
            return 0

        if not self.quiet:
            console.print(f"\n[bold yellow]{Language.get('gc_start')}[/]")

        tasks = []
        for i, api in enumerate(apis):
            acc_name = "MAIN" if i == 0 else f"EXTRA_{i}"
            tasks.append(self.clean_account(api, acc_name, exclude_ids))
        results = await asyncio.gather(*tasks)

        if not self.quiet:
            console.print(f"[bold green]{Language.get('gc_done')}[/]\n")
        return sum(results)

    async def clean_account(
        self, api: PikPakAPI, acc_name: str, exclude_ids=None
    ) -> int:
        exclude_ids = exclude_ids or set()
        try:
            temp_ids = [
                i for i in cloud_ledger.get(api.account_key) if i not in exclude_ids
            ]
            if not temp_ids:
                return 0

            logger.info(
                f"[{acc_name}] Có {len(temp_ids)} file tạm của tool còn trên cloud. Đang dọn dẹp..."
            )

            batches = [
                temp_ids[i : i + self.batch_size]
                for i in range(0, len(temp_ids), self.batch_size)
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
                            cloud_ledger.remove(api.account_key, batch_ids)
                            return True
                        if code in (429, 503):
                            await asyncio.sleep(2 * (attempt + 1))
                            continue
                        if code in (400, 404):
                            # File đã bị xóa từ trước: bỏ khỏi sổ để khỏi thử mãi
                            cloud_ledger.remove(api.account_key, batch_ids)
                        break
                    return False

            results = await asyncio.gather(*[_delete_batch(b) for b in batches])
            success_count = sum(len(b) for r, b in zip(results, batches) if r)
            if success_count > 0:
                logger.info(
                    f"[{acc_name}] Đã xóa {success_count} file tạm."
                )
                if not self.quiet:
                    console.print(
                        f"  [dim]{Language.get('gc_freed', acc=acc_name, n=success_count)}[/dim]"
                    )
            return success_count

        except Exception as e:
            logger.error(f"[{acc_name}] Lỗi Garbage Collector: {e}")
            return 0
