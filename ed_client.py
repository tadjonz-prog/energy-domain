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
# Opt in per schedule via environment variables on the cron/task line:
#   RUN_ONCE_KEY=rigs_daily     # state file name; distinguishes daily vs weekly
#   RUN_ONCE_PERIOD=day|week    # what "already done" means (default: day)
# If RUN_ONCE_KEY is unset the guard is disabled and the script always runs
# (so plain manual runs and the existing weekly single-shot lines are unchanged).
#
# State lives in data/state/<key>.txt (data/ is gitignored -> per-box, not shared).
# --------------------------------------------------------------------------
_STATE_DIR = Path(__file__).with_name("data") / "state"


def _period_id() -> str:
    """Identifier for the current period, per RUN_ONCE_PERIOD."""
    today = datetime.date.today()
    if os.getenv("RUN_ONCE_PERIOD", "day").lower() == "week":
        y, w, _ = today.isocalendar()
        return f"{y}-W{w:02d}"
    return today.isoformat()


def _guard_path():
    """State file for the active RUN_ONCE_KEY, or None if guarding is disabled."""
    key = os.getenv("RUN_ONCE_KEY")
    return (_STATE_DIR / f"{key}.txt") if key else None


def already_succeeded() -> bool:
    """True if this key has already succeeded for the current period."""
    path = _guard_path()
    if path is None:
        return False
    try:
        return path.read_text().strip() == _period_id()
    except FileNotFoundError:
        return False


def mark_succeeded() -> None:
    """Record a successful run for the current period (no-op if guard disabled)."""
    path = _guard_path()
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_period_id())


# --------------------------------------------------------------------------
# Report date (DATE_PROD / filename) — optionally pinned to a weekday
#
# Default: today (the run date). But a WEEKLY report whose retry may not land
# until Saturday must still stamp the FRIDAY it belongs to, or Zoho's
# Friday-ending weekly pivots break. Set REPORT_DATE_ANCHOR to a weekday name
# (e.g. "friday") and the report date snaps back to the most recent occurrence
# of that weekday on-or-before today:
#     Fri run   -> that Friday
#     Sat retry -> the day before (that same Friday)
#     Sun retry -> still that Friday
# Leave REPORT_DATE_ANCHOR unset for daily runs (stamps today, as before).
# --------------------------------------------------------------------------
_WEEKDAYS = {
    "monday": 0, "mon": 0, "tuesday": 1, "tue": 1, "wednesday": 2, "wed": 2,
    "thursday": 3, "thu": 3, "friday": 4, "fri": 4, "saturday": 5, "sat": 5,
    "sunday": 6, "sun": 6,
}


def resolve_report_date():
    """Date to stamp as DATE_PROD and use in the filename (see note above)."""
    today = datetime.date.today()
    anchor = os.getenv("REPORT_DATE_ANCHOR")
    if anchor:
        target = _WEEKDAYS.get(anchor.strip().lower())
        if target is not None:
            days_back = (today.weekday() - target) % 7
            return today - datetime.timedelta(days=days_back)
    return today


__all__ = [
    "get_connection", "connect", "fetch_frame",
    "already_succeeded", "mark_succeeded", "resolve_report_date",
]
