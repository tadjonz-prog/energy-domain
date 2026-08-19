"""
Energy Domain DataStream Direct — connection helper.

Python analog of the vendor's DBeaver/JDBC setup:
    jdbc:energy_domain://data-api.energydomain.com:443/energy_domain

Credentials + target are read from a gitignored .env file (never hard-coded).

Stack note: this venv pins pandas < 3 (currently 2.3.x). datastream_direct
0.1.4's type conversion (fetch_frame / _apply_type) predates pandas 3.0's
default string dtype and crashes on it ("Invalid value ... for dtype 'str'").
Pandas 2.x is the version the vendor library was built against. See
requirements.txt (pandas pinned) — do not bump pandas to 3.x here until the
vendor updates datastream_direct.
"""
import os
import datetime
from pathlib import Path

from dotenv import load_dotenv
from datastream_direct import connect, fetch_frame  # re-exported for convenience

# Load .env sitting next to this file
load_dotenv(Path(__file__).with_name(".env"))


def _require(name: str) -> str:
    val = os.getenv(name)
    if not val or val.startswith("REPLACE_WITH_"):
        raise RuntimeError(
            f"Missing credential: set {name} in {Path(__file__).with_name('.env')}"
        )
    return val


def get_connection():
    """Open a DataStream Direct connection using .env values."""
    return connect(
        username=_require("ED_USERNAME"),
        password=_require("ED_PASSWORD"),
        host=os.getenv("ED_HOST", "data-api.energydomain.com"),
        port=int(os.getenv("ED_PORT", "443")),
        database=os.getenv("ED_DATABASE", "energy_domain"),
    )


# --------------------------------------------------------------------------
# "Retry hourly until success" guard
#
# For unattended scheduling: run the job EVERY HOUR, but make each run a no-op
# once it has already succeeded for the current period. So a transient vendor
# outage (e.g. a 503 at 06:00) just means 07:00 tries again... until one run
# succeeds and stamps "done", after which the rest of that period's hourly
# firings exit instantly. Reboot-safe (state is a file), no long-sleeping
# process, no overlap.
#
# Driven by CLI flags (see parse_run_args): --once <key> --period day|week.
# key=None disables the guard (the script always runs), so plain manual runs
# are unchanged. State lives in data/state/<key>.txt (gitignored -> per-box).
# --------------------------------------------------------------------------
_STATE_DIR = Path(__file__).with_name("data") / "state"


def _period_id(period="day") -> str:
    """Period identifier: 'day' -> ISO date, 'week' -> ISO year-week."""
    today = datetime.date.today()
    if (period or "day").lower() == "week":
        y, w, _ = today.isocalendar()
        return f"{y}-W{w:02d}"
    return today.isoformat()


def already_succeeded(key, period="day") -> bool:
    """True if `key` already succeeded for the current period. key falsy -> False."""
    if not key:
        return False
    try:
        return (_STATE_DIR / f"{key}.txt").read_text().strip() == _period_id(period)
    except FileNotFoundError:
        return False


def mark_succeeded(key, period="day") -> None:
    """Record a successful run for the current period (no-op if key is falsy)."""
    if not key:
        return
    _STATE_DIR.mkdir(parents=True, exist_ok=True)
    (_STATE_DIR / f"{key}.txt").write_text(_period_id(period))


def once_period_warning(key, period):
    """Seatbelt for the orthogonal --once/--period flags: the key is a free-form
    label, so a name like 'rigs_daily' paired with '--period week' is legal but
    almost certainly a typo. Return a warning string on a name/period mismatch
    (by the _daily/_weekly naming convention), else None. Does not block the run."""
    if not key:
        return None
    k = key.lower()
    if k.endswith("_daily") and period != "day":
        return f"--once {key} (name says daily) but --period {period} — mismatch?"
    if (k.endswith("_weekly") or k.endswith("_week")) and period != "week":
        return f"--once {key} (name says weekly) but --period {period} — mismatch?"
    return None


# --------------------------------------------------------------------------
# Report date (DATE_PROD / filename) — optionally pinned to a weekday
#
# Default: today (the run date). But a WEEKLY report whose retry may not land
# until Saturday must still stamp the FRIDAY it belongs to, or Zoho's
# Friday-ending weekly pivots break. The --friday flag (see parse_run_args)
# passes anchor='friday' and the report date snaps back to the most recent
# occurrence of that weekday on-or-before today:
#     Fri run   -> that Friday
#     Sat retry -> the day before (that same Friday)
#     Sun retry -> still that Friday
# anchor=None (daily runs) stamps today, as before.
# --------------------------------------------------------------------------
_WEEKDAYS = {
    "monday": 0, "mon": 0, "tuesday": 1, "tue": 1, "wednesday": 2, "wed": 2,
    "thursday": 3, "thu": 3, "friday": 4, "fri": 4, "saturday": 5, "sat": 5,
    "sunday": 6, "sun": 6,
}


def resolve_report_date(anchor=None):
    """Date to stamp as DATE_PROD and use in the filename. anchor=None -> today;
    a weekday name (e.g. 'friday') -> most recent that-weekday on-or-before today."""
    today = datetime.date.today()
    if anchor:
        target = _WEEKDAYS.get(str(anchor).strip().lower())
        if target is not None:
            days_back = (today.weekday() - target) % 7
            return today - datetime.timedelta(days=days_back)
    return today


# --------------------------------------------------------------------------
# Cross-platform run logging
#
# Each run appends ONE structured line to data/logs/<report>.log — identical on
# every box regardless of launcher (cron / systemd / Task Scheduler / manual).
# Timestamps are stamped in America/Denver via zoneinfo so the abbreviation is a
# consistent MDT/MST on Linux AND Windows (a bare %Z renders the long
# "Mountain Daylight Time" on Windows but "MDT" on Linux — this avoids that).
# Line format:
#   YYYY-MM-DD HH:MM:SS MDT | <source> | <report> | <OK|SKIP|FAIL> | <detail> | <dur>
# data/ is gitignored -> logs are per-box, never committed.
# --------------------------------------------------------------------------
_LOG_DIR = Path(__file__).with_name("data") / "logs"


def report_source() -> str:
    """Which box this ran on ('spark' / 'prod2' / 'winpc' ...), for logs and the
    email subject. Honors REPORT_SOURCE in .env; else derives from the hostname."""
    tag = os.getenv("REPORT_SOURCE")
    if tag:
        return tag.strip()
    import socket
    h = socket.gethostname().lower()
    if "prod2" in h:
        return "prod2"
    if "spark" in h:
        return "spark"
    return h


def run_period_id(period="day") -> str:
    """Public accessor for the current guard period id (used in SKIP log lines)."""
    return _period_id(period)


def email_sent_note(recips):
    """Compact 'email=' log value confirming delivery + who to, e.g.
    'sent(2: tad.jones,zoho-import)'. Uses local-parts (before @) to stay short
    and keep the full address domain out of the log."""
    locals_ = [r.split("@")[0] for r in recips]
    return f"sent({len(locals_)}: {','.join(locals_)})"


def _log_timestamp() -> str:
    try:
        from zoneinfo import ZoneInfo
        now = datetime.datetime.now(ZoneInfo("America/Denver"))
    except Exception:
        now = datetime.datetime.now().astimezone()
    return now.strftime("%Y-%m-%d %H:%M:%S %Z")


def log_run(report: str, outcome: str, detail: str = "", seconds=None) -> None:
    """Append one outcome line to data/logs/<report>.log. Never raises."""
    dur = f"{seconds:.1f}s" if seconds is not None else "-"
    line = (f"{_log_timestamp()} | {report_source():<6} | {report:<7} | "
            f"{outcome:<4} | {detail} | {dur}\n")
    try:
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        with open(_LOG_DIR / f"{report}.log", "a", newline="\n") as f:
            f.write(line)
    except OSError:
        pass  # logging must never crash a run


def log_error(report: str) -> None:
    """Append the current exception's FULL traceback to data/logs/<report>.errors.log.
    Call from inside an `except` block. Keeps the concise FAIL line in the main
    <report>.log scannable while preserving the stack trace on every platform
    (Task Scheduler discards stderr, so winpc would otherwise lose it). Never raises."""
    import traceback
    block = (f"\n===== {_log_timestamp()} | {report_source()} | {report} | FAIL =====\n"
             f"{traceback.format_exc()}")
    try:
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        with open(_LOG_DIR / f"{report}.errors.log", "a", newline="\n") as f:
            f.write(block)
    except OSError:
        pass  # logging must never crash a run


# --------------------------------------------------------------------------
# "Ready to import" signal (handshake to the Zoho Analytics import process)
#
# On FULL success (file written + email sent) a --import-ready run writes an
# atomic JSON manifest to data/ready/<report>_<report_date>.json. Because it is
# written last and via os.replace (atomic), the importer never sees a partial
# file, and the manifest only appears once a good report exists — whether that
# is Friday evening or a Saturday retry. report_date is the pinned Friday, so the
# importer keys off Friday regardless of when the run actually succeeded. The
# sha256 lets the importer verify the .txt is intact before ingesting.
# See deploy/IMPORT.md for the full contract.
# --------------------------------------------------------------------------
_READY_DIR = Path(__file__).with_name("data") / "ready"


def write_ready_manifest(report, report_date, file_path, rows, emailed) -> None:
    """Atomically publish the 'ok to import' manifest for a completed report.
    Never raises (signalling must not crash a run)."""
    import json
    import hashlib
    try:
        path = Path(file_path)
        blob = path.read_bytes()
        try:
            from zoneinfo import ZoneInfo
            now = datetime.datetime.now(ZoneInfo("America/Denver"))
        except Exception:
            now = datetime.datetime.now().astimezone()
        manifest = {
            "report": report,
            "report_date": report_date.isoformat(),
            "file": path.name,
            "rows": rows,
            "bytes": len(blob),
            "sha256": hashlib.sha256(blob).hexdigest(),
            "source": report_source(),
            "emailed": bool(emailed),
            "generated_at": now.isoformat(timespec="seconds"),
            "status": "ready",
        }
        _READY_DIR.mkdir(parents=True, exist_ok=True)
        out = _READY_DIR / f"{report}_{report_date.isoformat()}.json"
        tmp = out.with_name(out.name + ".tmp")
        tmp.write_text(json.dumps(manifest, indent=2) + "\n")
        os.replace(tmp, out)   # atomic publish
    except OSError:
        pass


# --------------------------------------------------------------------------
# Shared CLI for both report runners
#
# Flags are the primary interface (portable across cron / systemd / Task
# Scheduler — no inline env-var syntax that varies by platform). Each flag
# falls back to its legacy env var when absent, so existing env-var-based
# schedules keep working during the transition to flags.
# --------------------------------------------------------------------------
def parse_run_args(argv=None):
    import argparse
    p = argparse.ArgumentParser(description="Energy Domain report runner")
    p.add_argument("--email", action="store_true",
                   help="also email the report (default: write file only)")
    p.add_argument("--friday", action="store_true",
                   help="stamp DATE_PROD (and the permits window) to the most recent "
                        "Friday — for weekly reports whose retry may slip to Saturday")
    p.add_argument("--once", metavar="KEY", default=None,
                   help="retry-until-success guard state key (e.g. rigs_daily, "
                        "rigs_weekly); omit to always run")
    p.add_argument("--period", choices=["day", "week"], default=None,
                   help="guard period for --once (default: day)")
    p.add_argument("--to", metavar="EMAILS", default=None,
                   help="override recipients, comma-separated (default: RIGS_EMAIL_TO from .env)")
    p.add_argument("--import-ready", action="store_true", dest="import_ready",
                   help="on full success, publish data/ready/<report>_<date>.json for "
                        "the Zoho import process (see deploy/IMPORT.md)")
    args = p.parse_args(argv)
    # Flag-first, env-fallback (keeps legacy env-var schedules working):
    args.anchor = "friday" if args.friday else os.getenv("REPORT_DATE_ANCHOR")
    args.once = args.once or os.getenv("RUN_ONCE_KEY")
    args.period = args.period or os.getenv("RUN_ONCE_PERIOD") or "day"
    return args


__all__ = [
    "get_connection", "connect", "fetch_frame",
    "already_succeeded", "mark_succeeded", "resolve_report_date",
    "report_source", "run_period_id", "log_run", "log_error", "parse_run_args",
    "once_period_warning", "write_ready_manifest", "email_sent_note",
]
