import asyncio
import json
import os
import re

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from core.download.downloader import Downloader

DATA = os.urandom(5 * 1024 * 1024 + 123)


@pytest.fixture(autouse=True)
def fast_sleep(monkeypatch):
    real_sleep = asyncio.sleep

    async def _sleep(delay, *args, **kwargs):
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", _sleep)


def make_app(mode, stats):
    seen = set()

    async def handler(request):
        stats["requests"] += 1
        stats["peers"].add(request.transport.get_extra_info("peername"))
        if mode == "503":
            return web.Response(status=503)
        m = re.match(r"bytes=(\d+)-(\d+)", request.headers.get("Range", ""))
        if mode == "ignore_range" or not m:
            return web.Response(body=DATA, status=200)
        start, end = int(m.group(1)), int(m.group(2))
        body = DATA[start : end + 1]
        if mode == "short_once" and (start, end) not in seen:
            # Phản hồi "hợp lệ" nhưng thiếu nửa sau, kết thúc sạch (không lỗi kết nối)
            seen.add((start, end))
            body = body[: len(body) // 2]
        return web.Response(
            body=body,
            status=206,
            headers={"Content-Range": f"bytes {start}-{start + len(body) - 1}/{len(DATA)}"},
        )

    app = web.Application()
    app.router.add_get("/f", handler)
    return app


async def run_download(mode, path, num_conn=3, stats=None, times=1):
    stats = stats if stats is not None else {"requests": 0, "peers": set()}
    async with TestServer(make_app(mode, stats)) as server:
        url = str(server.make_url("/f"))
        dl = Downloader()
        dl.progress_data[1] = {"status": ""}

        async def fresh():
            return [url]

        try:
            ok = True
            for i in range(times):
                target = path.with_name(f"{path.name}.{i}") if times > 1 else path
                ok = await dl._multi_conn_download(
                    [url], {}, target, len(DATA), 1, num_conn, fresh
                ) and ok
            return ok
        finally:
            await dl.close_session()


def test_normal_download(tmp_path):
    p = tmp_path / "f.tmp"
    assert asyncio.run(run_download("ok", p))
    assert p.read_bytes() == DATA


def test_short_response_is_retried_not_marked_done(tmp_path):
    p = tmp_path / "f.tmp"
    assert asyncio.run(run_download("short_once", p))
    assert p.read_bytes() == DATA


def test_server_ignoring_range_gives_up_fast_without_corruption(tmp_path):
    p = tmp_path / "f.tmp"
    stats = {"requests": 0, "peers": set()}
    assert not asyncio.run(run_download("ignore_range", p, stats=stats))
    # Bỏ cuộc ngay để nhánh gọi chuyển sang tải 1 luồng, không thử lại hàng chục lần
    assert stats["requests"] <= 6
    ckpt = json.loads((tmp_path / "f.tmp.ckpt").read_text())
    for s, e in ckpt["completed_ranges"]:
        assert p.read_bytes()[s : e + 1] == DATA[s : e + 1]


def test_consecutive_files_reuse_connections(tmp_path):
    stats = {"requests": 0, "peers": set()}
    assert asyncio.run(run_download("ok", tmp_path / "f.tmp", num_conn=2, stats=stats, times=3))
    for i in range(3):
        assert (tmp_path / f"f.tmp.{i}").read_bytes() == DATA
    assert stats["requests"] >= 6
    assert len(stats["peers"]) <= 2


def test_checkpoint_exists_when_download_fails(tmp_path):
    p = tmp_path / "f.tmp"
    assert not asyncio.run(run_download("503", p, num_conn=2))
    assert p.stat().st_size == len(DATA)
    ckpt = json.loads((tmp_path / "f.tmp.ckpt").read_text())
    assert ckpt == {"total": len(DATA), "completed_ranges": []}


class FakeAPI:
    def __init__(self, url):
        self.url = url

    async def ensure_token(self):
        return True

    async def get_download_url(self, share_id, file_id, pass_token):
        return self.url


async def run_single_file(mode, stats):
    async with TestServer(make_app(mode, stats)) as server:
        dl = Downloader()
        file_data = {"name": "a.bin", "size": len(DATA), "id": "x", "path": "dir/a.bin"}
        try:
            return await dl.download_single_file(
                file_data, "s", "p", 1, api=FakeAPI(str(server.make_url("/f")))
            )
        finally:
            await dl.close_session()


@pytest.mark.parametrize("mode", ["ok", "ignore_range"])
def test_direct_download_without_head_probe(tmp_path, monkeypatch, mode):
    from config.settings import Config

    monkeypatch.setattr(Config, "DOWNLOAD_PATH_STR", str(tmp_path))
    stats = {"requests": 0, "peers": set()}
    assert asyncio.run(run_single_file(mode, stats))
    assert (tmp_path / "dir" / "a.bin").read_bytes() == DATA
    assert not list((tmp_path / "dir").glob(".*"))
