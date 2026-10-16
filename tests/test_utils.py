import pytest

from config.settings import Config
from core import cloud_ledger
from core.downloader import Downloader, safe_name, safe_rel_dir
from core.proxy import parse_proxy_string
from core.utils import UpdateManager


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("phim: tập 1?.mp4", "phim_ tập 1_.mp4"),
        ("..", "_"),
        ("", "_"),
        ("CON.txt", "_CON.txt"),
        ("name. ", "name"),
    ],
)
def test_safe_name(raw, expected):
    assert safe_name(raw) == expected


def test_safe_name_limits_bytes_and_keeps_ext():
    out = safe_name("á" * 300 + ".mkv")
    assert len(out.encode("utf-8")) <= 200
    assert out.endswith(".mkv")


def test_safe_rel_dir_drops_filename_and_traversal():
    p = safe_rel_dir("../A/B:C/file.mp4")
    assert p.parts == ("_", "A", "B_C")


def test_gaps_and_merge():
    assert Downloader._merge_ranges([[10, 19], [0, 9], [30, 39]]) == [[0, 19], [30, 39]]
    assert Downloader._get_gaps(50, [[0, 9], [20, 29]]) == [[10, 19], [30, 49]]
    assert Downloader._get_gaps(10, []) == [[0, 9]]
    assert Downloader._get_gaps(10, [[0, 9]]) == []


@pytest.mark.parametrize(
    "text, expected",
    [
        ("socks5://u:p%40ss@1.2.3.4:1080", ("socks5", "1.2.3.4", "1080", "u", "p@ss")),
        ("socks5h://h:1", ("socks5", "h", "1", "", "")),
        ("u:p@ss@host:8080", (None, "host", "8080", "u", "p@ss")),
        ("host:3128", (None, "host", "3128", "", "")),
        ("host:3128:u:p", (None, "host", "3128", "u", "p")),
    ],
)
def test_parse_proxy_string(text, expected):
    d = parse_proxy_string(text)
    assert (d["type"], d["host"], d["port"], d["username"], d["password"]) == expected


@pytest.mark.parametrize("text", ["", "ftp://h:1", "host", "host:99999", "a:b:c"])
def test_parse_proxy_string_rejects(text):
    with pytest.raises(ValueError):
        parse_proxy_string(text)


@pytest.mark.parametrize(
    "remote, local, newer",
    [
        ("0.0.10", "0.0.9", True),
        ("0.1", "0.0.9", True),
        ("0.0.9", "0.0.9", False),
        ("0.0.8", "0.0.9", False),
        ("1.0.0-beta", "0.0.9", False),
    ],
)
def test_is_newer_version(remote, local, newer):
    assert UpdateManager.is_newer_version(remote, local) is newer


def test_config_int_clamps():
    assert Config._int("abc", 3, lo=1, hi=32) == 3
    assert Config._int(0, 3, lo=1, hi=32) == 1
    assert Config._int("100", 3, lo=1, hi=32) == 32


def test_cloud_ledger_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setattr(cloud_ledger, "LEDGER_FILE", tmp_path / "cloud_temp.json")
    cloud_ledger.add("dev1", "f1")
    cloud_ledger.add("dev1", "f1")
    cloud_ledger.add("dev1", "f2")
    assert cloud_ledger.get("dev1") == ["f1", "f2"]
    cloud_ledger.remove("dev1", "f1")
    assert cloud_ledger.contains("dev1", "f2")
    assert not cloud_ledger.contains("dev1", "f1")
    cloud_ledger.remove("dev1", ["f2"])
    assert cloud_ledger.get("dev1") == []
