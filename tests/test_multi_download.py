import asyncio
import json
import os
import re

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from core.downloader import Downloader

DATA = os.urandom(5 * 1024 * 1024 + 123)


@pytest.fixture(autouse=True)
def fast_sleep(monkeypatch):
    real_sleep = asyncio.sleep

    async def _sleep(delay, *args, **kwargs):
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", _sleep)


def make_app(mode):
    seen = set()

    async def handler(request):
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


async def run_download(mode, path, num_conn=3):
    async with TestServer(make_app(mode)) as server:
        url = str(server.make_url("/f"))
        dl = Downloader()
        dl.progress_data[1] = {"status": ""}

        async def fresh():
            return [url]

        ok = await dl._multi_conn_download([url], {}, path, len(DATA), 1, num_conn, fresh)
        return ok


def test_normal_download(tmp_path):
    p = tmp_path / "f.tmp"
    assert asyncio.run(run_download("ok", p))
    assert p.read_bytes() == DATA


def test_short_response_is_retried_not_marked_done(tmp_path):
    p = tmp_path / "f.tmp"
    assert asyncio.run(run_download("short_once", p))
    assert p.read_bytes() == DATA


def test_server_ignoring_range_does_not_corrupt(tmp_path):
    p = tmp_path / "f.tmp"
    ok = asyncio.run(run_download("ignore_range", p))
    if ok:
        assert p.read_bytes() == DATA
    else:
        ckpt = json.loads((tmp_path / "f.tmp.ckpt").read_text())
        for s, e in ckpt["completed_ranges"]:
            assert p.read_bytes()[s : e + 1] == DATA[s : e + 1]


def test_checkpoint_exists_when_download_fails(tmp_path):
    p = tmp_path / "f.tmp"
    assert not asyncio.run(run_download("503", p, num_conn=2))
    assert p.stat().st_size == len(DATA)
    ckpt = json.loads((tmp_path / "f.tmp.ckpt").read_text())
    assert ckpt == {"total": len(DATA), "completed_ranges": []}
