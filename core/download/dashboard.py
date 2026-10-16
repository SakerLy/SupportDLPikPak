from rich import box
from rich.console import Group
from rich.markup import escape as rich_escape
from rich.panel import Panel
from rich.table import Table

from config.settings import Language

DONE_STATUS = "Done"
SKIP_STATUS = "Skipped"
GOOD_STATUSES = {DONE_STATUS, SKIP_STATUS}


class DashboardMixin:
    def generate_dashboard_table(self):
        all_threads = list(self.progress_data.values())
        done_count = sum(1 for p in all_threads if p["status"] == DONE_STATUS)
        skipped_count = sum(1 for p in all_threads if p["status"] == SKIP_STATUS)
        cancelled_count = sum(1 for p in all_threads if p["status"] == "Cancelled")

        display_list = [
            p
            for p in all_threads
            if p["status"] not in (*GOOD_STATUSES, "Cancelled", "Waiting")
        ]

        total_speed = sum(
            p.get("speed", 0)
            for p in display_list
            if "DL" in p["status"] or "Resuming" in p["status"]
        )
        total_downloaded = sum(p.get("done_bytes", 0) for p in all_threads)
        remaining = max(0, self.total_batch_size - total_downloaded)
        eta_str = (
            self.format_time(remaining / total_speed) if total_speed > 0 else "--:--"
        )

        cancel_hint = (
            f"[bold red] {Language.get('dl_cancelling')}[/]"
            if self.cancel_event.is_set()
            else f"  [dim]{Language.get('dl_cancel_hint')}[/]"
        )

        stats_grid = Table.grid(expand=True)
        stats_grid.add_column(justify="center", ratio=1)
        stats_grid.add_column(justify="center", ratio=1)
        stats_grid.add_column(justify="center", ratio=1)
        stats_grid.add_row(
            f"[bold cyan]Queue: {self.total_files_count - done_count - skipped_count - cancelled_count}[/]",
            f"[bold green]Done: {done_count}[/] | [bold yellow]Skip: {skipped_count}[/]"
            + (f" | [bold red]Cancel: {cancelled_count}[/]" if cancelled_count else ""),
            f"[bold white]Speed: {self.format_size(total_speed)}/s | ETA: {eta_str}[/]",
        )
        panel_stats = Panel(
            Group(stats_grid, cancel_hint),
            style="blue",
            title=f"[bold]{Language.get('global_stats')}[/]",
        )

        task_table = Table(
            box=box.SIMPLE, show_header=True, header_style="bold cyan", expand=True
        )
        task_table.add_column("ID", width=4)
        task_table.add_column("Filename", ratio=3)
        task_table.add_column("Progress", ratio=2)
        task_table.add_column("Speed", width=12, justify="right")
        task_table.add_column("ETA", width=10, justify="right")
        task_table.add_column("Status", width=14, justify="center")

        for p in sorted(display_list, key=lambda x: x["id"]):
            pct = min(p.get("percent", 0), 100)
            bad = p["status"] in (
                "Error",
                "Failed",
                "Restore Fail",
                "Cancelled",
                "Cancelling...",
                "Disk Error",
                "Disk Full",
            )
            color = "red" if bad else ("green" if pct == 100 else "cyan")
            filled = int(20 * pct / 100)
            bar = f"[{color}]{'━' * filled}[/][dim white]{'━' * (20 - filled)}[/]"
            ss = (
                "bold red"
                if bad
                else ("bold yellow" if p["status"] == "Cancelling..." else "cyan")
            )
            task_table.add_row(
                str(p["id"]),
                rich_escape(p["name"]),
                f"{bar} {pct:.0f}%",
                f"{self.format_size(p.get('speed', 0))}/s",
                self.format_time(p.get("eta", 0)),
                f"[{ss}]{p['status']}[/]",
            )
        return Group(panel_stats, task_table)
