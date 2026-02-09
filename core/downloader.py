import threading
import time
import os
import requests
import subprocess
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.console import Group
from rich.align import Align
from rich import box
from config.settings import Config, console, Language
from core.api import PikPakAPI, TreeBuilder

class Downloader:
    def __init__(self):
        self.api = PikPakAPI()
        self.tree_builder = TreeBuilder(self.api)
        self.progress_data = {} 
        self.monitor_active = False
        self.total_files_count = 0 
        self.total_batch_size = 0
        self.file_progress_locks = {}
        self.storage_lock = threading.Lock()

    @staticmethod
    def format_size(size):
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size < 1024: return f"{size:.2f} {unit}"
            size /= 1024
        return f"{size:.2f} PB"

    @staticmethod
    def format_time(seconds):
        if seconds < 0 or seconds > 86400 * 3: return "--:--"
        m, s = divmod(int(seconds), 60)
        h, m = divmod(m, 60)
        return f"{h:02d}:{m:02d}:{s:02d}" if h > 0 else f"{m:02d}:{s:02d}"

    def _natural_key(self, item):
        return [int(s) if s.isdigit() else s.lower() for s in re.split(r'(\d+)', item['name'])]

    def _recursive_sort(self, node):
        if 'files' in node: node['files'].sort(key=self._natural_key)
        if 'folders' in node:
            node['folders'].sort(key=self._natural_key)
            for folder in node['folders']: self._recursive_sort(folder)

    def get_tree_and_prepare(self, url, password):
        m = re.search(r"/s/([A-Za-z0-9_-]+)", url)
        if not m: console.print(f"[bold red]{Language.get('link_invalid')}[/]"); return None
        share_id = m.group(1)
        if not self.api.refresh_token(): return None
        files, ptoken = self.api.get_share_info(share_id, password)
        if not files: console.print(f"[bold red]{Language.get('no_files')}[/]"); return None
        console.print(f"[cyan]{Language.get('analyzing')}[/]")
        tree = self.tree_builder.build_tree(files, "", share_id, ptoken)
        self._recursive_sort(tree)
        return {"folders": tree["folders"], "files": tree["files"], "share_id": share_id, "pass_token": ptoken}

    def start_monitor(self, total_count, total_size_bytes):
        self.monitor_active = True
        self.total_files_count = total_count
        self.total_batch_size = total_size_bytes
        self.batch_start_time = time.time()

    def stop_monitor(self): self.monitor_active = False

    def generate_dashboard_table(self):
        all_threads = list(self.progress_data.values())
        done_count = sum(1 for p in all_threads if p['status'] == "Done")
        skipped_count = sum(1 for p in all_threads if p['status'] == "Skipped")
        display_list = [p for p in all_threads if p['status'] not in ["Done", "Skipped", Language.get('status_idm')]]
        total_speed = sum(p['speed'] for p in display_list)
        total_downloaded = sum(p.get('done_bytes', 0) for p in all_threads)
        remaining_bytes = max(0, self.total_batch_size - total_downloaded)
        eta_str = self.format_time(remaining_bytes / total_speed) if total_speed > 0 else "--:--"
        
        stats_grid = Table.grid(expand=True)
        stats_grid.add_column(justify="center", ratio=1)
        stats_grid.add_column(justify="center", ratio=1)
        stats_grid.add_column(justify="center", ratio=1)
        stats_grid.add_row(
            f"[bold cyan]Queue: {self.total_files_count - done_count - skipped_count}[/]",
            f"[bold green]Done: {done_count}[/] | [bold yellow]Skip: {skipped_count}[/]",
            f"[bold white]Speed: {self.format_size(total_speed)}/s[/] | [bold white]ETA: {eta_str}[/]"
        )
        panel_stats = Panel(stats_grid, style="blue", title=f"[bold]{Language.get('global_stats')}[/]")

        task_table = Table(box=box.SIMPLE, show_header=True, header_style="bold cyan", expand=True)
        task_table.add_column("ID", width=4)
        task_table.add_column("Filename", ratio=3)
        task_table.add_column("Progress", ratio=2)
        task_table.add_column("Speed", width=12, justify="right")
        task_table.add_column("ETA", width=10, justify="right")
        task_table.add_column("Status", width=10, justify="center")

        display_list.sort(key=lambda x: x['id'])

        for p in display_list:
            percent = p['percent']
            bar_color = "green" if percent == 100 else "cyan"
            if p['status'] in ["Error", "Failed", "Restore Fail", "Too Large"]: bar_color = "red"
            filled = int(20 * percent / 100)
            bar_str = f"[{bar_color}]{'━' * filled}[/][dim white]{'━' * (20 - filled)}[/]"
            status_style = "bold red" if p['status'] in ["Error", "Failed", "Restore Fail", "Too Large"] else "cyan"
            task_table.add_row(str(p['id']), p['name'], f"{bar_str} {percent:.0f}%", f"{self.format_size(p['speed'])}/s", self.format_time(p.get('eta', 0)), f"[{status_style}]{p['status']}[/]")
        return Group(panel_stats, task_table)

    def call_idm(self, url, filename, save_dir):
        if not os.path.exists(Config.IDM_PATH): return False
        try:
            subprocess.Popen([Config.IDM_PATH, '/d', url, '/p', str(save_dir.absolute()), '/f', filename, '/n', '/a', '/s'])
            return True
        except: return False
    
    def _download_segment(self, url, headers, file_path, start, end, thread_id, progress_lock, file_total_size, start_time):
        try:
            req_headers = headers.copy()
            req_headers['Range'] = f"bytes={start}-{end}"
            expected_length = end - start + 1
            downloaded_len = 0

            with requests.get(url, headers=req_headers, stream=True, timeout=60, verify=False, proxies=Config.get_proxy_dict()) as r:
                r.raise_for_status()
                with open(file_path, "r+b") as f:
                    f.seek(start)
                    for chunk in r.iter_content(chunk_size=1024*256):
                        if chunk:
                            f.write(chunk)
                            chunk_len = len(chunk)
                            downloaded_len += chunk_len
                            with progress_lock:
                                self.progress_data[thread_id]['done_bytes'] += chunk_len
                                downloaded = self.progress_data[thread_id]['done_bytes']
                                duration = time.time() - start_time
                                if duration > 0:
                                    speed = downloaded / duration
                                    percent = (downloaded / file_total_size) * 100
                                    eta = (file_total_size - downloaded) / speed
                                    self.progress_data[thread_id].update({'percent': percent, 'speed': speed, 'eta': eta})
            
            if downloaded_len != expected_length:
                return False

            return True
        except Exception: return False

    def download_single_file(self, file_data, share_id, pass_token, thread_id):
        name = file_data['name']
        real_total_size = int(file_data['size'])
        HEAVY_EXTS = ['.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.ts', '.iso', '.m4v', '.rar', '.zip', '.7z']
        is_heavy_file = any(name.lower().endswith(ext) for ext in HEAVY_EXTS)

        use_premium_transfer = is_heavy_file or Config.FORCE_PREMIUM_MODE

        self.progress_data[thread_id] = {'id': thread_id, 'name': name, 'percent': 0, 'speed': 0, 'status': "Init...", 'done_bytes': 0, 'total_bytes': real_total_size, 'eta': 0}
        
        save_dir = Config.get_download_dir() / Path(file_data['path']).parent
        save_dir.mkdir(parents=True, exist_ok=True)
        file_path = save_dir / name
        temp_file = file_path.parent / f".{file_path.name}.tmp"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36", "Referer": "https://mypikpak.com/", "Accept": "*/*", "Accept-Encoding": "identity", "Connection": "keep-alive"}
        
        # IMPORT RE Module locally as it might be missing in scope
        import re

        # --- CASE 1: HEAVY FILE (RESTORE -> SEARCH -> GET LINK -> MULTI-DL -> DELETE) ---
        if use_premium_transfer:
            if file_path.exists():
                if file_path.stat().st_size == real_total_size:
                    self.progress_data[thread_id].update({'percent': 100, 'speed': 0, 'status': "Skipped", 'done_bytes': real_total_size})
                    return True
                if file_path.stat().st_size > real_total_size: file_path.unlink()

            # Always lock to ensure sequential download (protect cloud storage)
            self.progress_data[thread_id]['status'] = "Waiting..."
            self.storage_lock.acquire()

            my_file_id = None
            download_success = False

            try:
                # 0. PRE-CLEANUP
                existing_id = self.api.wait_for_file(name, max_retries=1)
                if existing_id:
                    self.progress_data[thread_id]['status'] = "Cleaning Old..."
                    self.api.delete_file(existing_id)
                    time.sleep(1)

                # 1. Restore
                self.progress_data[thread_id]['status'] = Language.get('status_restore')
                my_file_id = self.api.restore_and_poll(share_id, file_data['id'], pass_token)
                
                # Fallback if task ID logic fails
                if not my_file_id:
                    self.progress_data[thread_id]['status'] = Language.get('status_check')
                    my_file_id = self.api.wait_for_file(name)
                
                if not my_file_id:
                    self.progress_data[thread_id]['status'] = "Restore Fail"
                    return False
                
                # 2. Get Link
                self.progress_data[thread_id]['status'] = Language.get('status_getlink')
                time.sleep(2) 
                download_url = self.api.get_user_file_url(my_file_id)
                if not download_url:
                    self.progress_data[thread_id]['status'] = "No Link"
                    self.api.delete_file(my_file_id)
                    return False

                # 3a. IDM Check (Disabled for Restore Mode to ensure cleanup)
                
                # 3b. Multi-Threaded DL (Internal)
                if not file_path.exists() or file_path.stat().st_size == 0:
                    try:
                        with open(file_path, "wb") as f: f.truncate(real_total_size)
                        num_threads = Config.CONCURRENT_THREADS
                        chunk_size = real_total_size // num_threads
                        futures = []
                        progress_lock = threading.Lock()
                        start_time = time.time()
                        self.progress_data[thread_id]['status'] = f"DL ({num_threads} threads)"

                        with ThreadPoolExecutor(max_workers=num_threads) as executor:
                            for i in range(num_threads):
                                start = i * chunk_size
                                end = start + chunk_size - 1 if i < num_threads - 1 else real_total_size - 1
                                futures.append(executor.submit(self._download_segment, download_url, headers, file_path, start, end, thread_id, progress_lock, real_total_size, start_time))
                            results = [f.result() for f in as_completed(futures)]
                        
                        if all(results) and file_path.stat().st_size == real_total_size:
                            self.progress_data[thread_id].update({'percent': 100, 'speed': 0, 'status': "Done", 'done_bytes': real_total_size})
                            download_success = True
                        else: 
                            self.progress_data[thread_id]['status'] = "Switch Single..."
                            if file_path.exists(): file_path.unlink()
                            self.progress_data[thread_id]['done_bytes'] = 0
                    except: 
                        self.progress_data[thread_id]['status'] = "Error"
                        if file_path.exists(): file_path.unlink()

                # 4. Fallback Resume
                if not download_success:
                    retry_count = 0
                    while True:
                        current_size = 0
                        if file_path.exists(): current_size = file_path.stat().st_size
                        if current_size >= real_total_size:
                            self.progress_data[thread_id].update({'percent': 100, 'speed': 0, 'status': "Done", 'done_bytes': real_total_size})
                            download_success = True
                            break
                        
                        mode = 'ab' if current_size > 0 else 'wb'
                        curr_headers = headers.copy()
                        if current_size > 0: curr_headers['Range'] = f"bytes={current_size}-"; self.progress_data[thread_id]['status'] = f"Resume {int(current_size/1024/1024)}MB"
                        
                        try:
                            with requests.get(download_url, headers=curr_headers, stream=True, timeout=30, verify=False, proxies=Config.get_proxy_dict()) as r:
                                if r.status_code in [403, 401]: self.progress_data[thread_id]['status'] = "Refresh Link"; download_url = self.api.get_user_file_url(my_file_id); time.sleep(1); continue
                                if r.status_code == 416: 
                                    if file_path.stat().st_size == real_total_size: download_success = True; break
                                    file_path.unlink(missing_ok=True); current_size = 0; continue
                                r.raise_for_status()
                                self.progress_data[thread_id]['status'] = Language.get('status_dl')
                                start_time = time.time(); downloaded_session = 0; last_ui_time = start_time
                                with open(file_path, mode) as f:
                                    for chunk in r.iter_content(chunk_size=4*1024*1024):
                                        if chunk:
                                            f.write(chunk); downloaded_session += len(chunk); total_down = current_size + downloaded_session
                                            if time.time() - last_ui_time >= 1:
                                                speed = downloaded_session / (time.time() - start_time); percent = (total_down / real_total_size) * 100
                                                eta = (real_total_size - total_down) / speed if speed > 0 else 0
                                                self.progress_data[thread_id].update({'percent': percent, 'speed': speed, 'done_bytes': total_down, 'eta': eta}); last_ui_time = time.time()
                            if file_path.stat().st_size < real_total_size: raise Exception("Incomplete")
                            else: download_success = True; break
                        except: 
                            retry_count += 1
                            self.progress_data[thread_id]['status'] = f"Retry {retry_count}"
                            time.sleep(2)
                            if retry_count > 10:
                                self.progress_data[thread_id]['status'] = "Failed"
                                if file_path.exists(): file_path.unlink()
                                if temp_file.exists(): temp_file.unlink()
                                if my_file_id: self.api.delete_file(my_file_id)
                                break
            
            finally:
                if my_file_id:
                    self.progress_data[thread_id]['status'] = Language.get('status_clean')
                    self.api.delete_file(my_file_id)
                    if download_success:
                        self.progress_data[thread_id]['status'] = "Done"
                
                self.storage_lock.release()
            
            return download_success

        # --- CASE 2: DIRECT DOWNLOAD (LIGHT FILES) ---
        else:
            download_url = self.api.get_download_url(share_id, file_data['id'], pass_token)
            if not download_url: self.progress_data[thread_id]['status'] = "No URL"; return False
            
            if Config.USE_IDM and os.name == 'nt':
                 if self.call_idm(download_url, name, save_dir):
                    self.progress_data[thread_id].update({'percent': 100, 'status': Language.get('status_idm'), 'done_bytes': real_total_size})
                    return True

            try:
                if file_path.exists() and file_path.stat().st_size == real_total_size: self.progress_data[thread_id].update({'percent': 100, 'status': "Skipped"}); return True
                resume_pos = 0; mode = 'wb'
                if temp_file.exists():
                    resume_pos = temp_file.stat().st_size
                    if resume_pos < real_total_size: mode = 'ab'; headers['Range'] = f"bytes={resume_pos}-"; self.progress_data[thread_id]['status'] = "Resuming..."
                    elif resume_pos >= real_total_size: temp_file.rename(file_path); self.progress_data[thread_id].update({'percent': 100, 'status': "Done"}); return True

                r = requests.get(download_url, headers=headers, stream=True, timeout=Config.TIMEOUT, verify=False, proxies=Config.get_proxy_dict())
                if resume_pos > 0 and r.status_code == 200: resume_pos = 0; mode = 'wb'; temp_file.unlink(missing_ok=True)
                if r.status_code not in [200, 206]: 
                    if temp_file.exists(): temp_file.unlink()
                    if file_path.exists(): file_path.unlink()
                    self.progress_data[thread_id]['status'] = f"Err {r.status_code}"; return False
                
                done = resume_pos; start_time = time.time(); last_time = start_time; last_done = done
                with open(temp_file, mode) as f:
                    for chunk in r.iter_content(chunk_size=1024*64):
                        if chunk:
                            f.write(chunk); done += len(chunk)
                            if time.time() - last_time >= 0.5:
                                speed = (done - last_done) / (time.time() - last_time); percent = (done / real_total_size) * 100 if real_total_size else 0
                                eta = (real_total_size - done) / speed if speed > 0 else 0
                                self.progress_data[thread_id].update({'percent': percent, 'speed': speed, 'status': "DL Direct", 'done_bytes': done, 'eta': eta}); last_time = time.time(); last_done = done
                if temp_file.stat().st_size >= real_total_size: temp_file.rename(file_path); self.progress_data[thread_id].update({'percent': 100, 'status': "Done"}); return True
                return False
            except: 
                if temp_file.exists(): temp_file.unlink()
                if file_path.exists(): file_path.unlink()
                self.progress_data[thread_id]['status'] = "Error"; return False