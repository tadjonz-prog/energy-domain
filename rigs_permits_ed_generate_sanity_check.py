#!/bin/sh
''''exec "$(dirname "$0")/venv/bin/python3" "$0" "$@" # '''
"""
rigs_permits_ed_generate_sanity_check.py

Ad-hoc PREFLIGHT / unit test for the Friday rigs & permits generators. Run it by
hand anytime; it is NOT scheduled and stays entirely OUT of the real pipeline:

  * checks the cron schedule has the expected weekly report lines (cron boxes)
  * checks the Energy Domain connection (raw cursor)
  * BUILDS both reports for real (writes today's .txt via build_report())
  * emails each built file to the OPERATOR ONLY, subject-marked "[SANITY TEST]",
    plus a PASS/FAIL summary email
  * prints + logs the verdict to data/logs/sanity.log

It never: writes an import manifest, touches the --once retry guard, or emails
the zoho import address. Pure unit test of ED's calls + the file build.

Usage:
  ./rigs_permits_ed_generate_sanity_check.py                 # check + build + email operator
  ./rigs_permits_ed_generate_sanity_check.py --to me@x.com   # send test emails elsewhere
  ./rigs_permits_ed_generate_sanity_check.py --no-email      # check + build only, no emails
"""
import argparse
import subprocess
import time

from ed_client import get_connection, send_email, log_run, log_error

DEFAULT_TO = "tad.jones@columbineco.com"   # operator; never the zoho import address
SANITY_PREFIX = "[SANITY TEST] "

# Expected report invocations in the crontab: (label, script, required flag fragments)
EXPECTED_CRON = [
    ("weekly rigs",    "rigs_ed.py",
     ["--email", "--friday", "--once rigs_weekly", "--period week", "--import-ready"]),
    ("weekly permits", "permits_ed.py",
     ["--email", "--friday", "--once permits_weekly", "--period week", "--import-ready"]),
]


def check_cron():
    """Return (status, [notes]). status in {'OK','FAIL','SKIP'}. SKIP = no crontab
    on this box (e.g. spark uses systemd, winpc uses Task Scheduler)."""
    try:
        r = subprocess.run(["crontab", "-l"], capture_output=True, text=True, timeout=10)
    except Exception as e:
        return "FAIL", [f"crontab -l error: {e}"]
    if r.returncode != 0:
        return "SKIP", ["no crontab on this box (non-cron scheduler?) — check skipped"]
    active = [ln for ln in r.stdout.splitlines()
              if ln.strip() and not ln.lstrip().startswith("#")]
    notes, ok = [], True
    for label, script, frags in EXPECTED_CRON:
        hits = [ln for ln in active if script in ln and all(f in ln for f in frags)]
        if hits:
            notes.append(f"{label}: OK ({len(hits)} line(s))")
        else:
            ok = False
            notes.append(f"{label}: MISSING (need: {' '.join(frags)})")
    return ("OK" if ok else "FAIL"), notes


def check_connection():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT state_abbr FROM well_combined LIMIT 1")
        cur.fetchall()
        return "connection: OK"
    finally:
        conn.close()


def build_and_email(kind, to, do_email):
    """Real build of one report (no guard, no manifest) + optional test email."""
    if kind == "rigs":
        from rigs_ed import build_report, email_report
    else:
        from permits_ed import build_report, email_report
    t0 = time.monotonic()
    out_path, n, summary = build_report()          # writes today's .txt
    dt = time.monotonic() - t0
    note = f"{kind}: OK  {n} rows, {out_path.stat().st_size:,} bytes, {dt:.1f}s"
    if do_email:
        recips = email_report(out_path, summary, to=to, subject_prefix=SANITY_PREFIX)
        note += f", emailed->{','.join(r.split('@')[0] for r in recips)}"
    else:
        note += ", email skipped"
    return note


def main():
    p = argparse.ArgumentParser(
        description="Ad-hoc sanity check for the ED rigs/permits generators")
    p.add_argument("--to", default=DEFAULT_TO,
                   help=f"recipient for the test emails (default {DEFAULT_TO}; never zoho)")
    p.add_argument("--no-email", action="store_true", help="build only; send no emails")
    args = p.parse_args()
    do_email = not args.no_email

    lines, ok = [], True

    cron_status, cron_notes = check_cron()
    if cron_status == "FAIL":
        ok = False
    lines.append(f"cron schedule: {cron_status}")
    lines += [f"  {n}" for n in cron_notes]

    try:
        lines.append(check_connection())
    except Exception as e:
        ok = False
        lines.append(f"connection: FAIL  {type(e).__name__}: {e}")
        log_error("sanity")

    for kind in ("rigs", "permits"):
        try:
            lines.append(build_and_email(kind, args.to, do_email))
        except Exception as e:
            ok = False
            lines.append(f"{kind}: FAIL  {type(e).__name__}: {e}")
            log_error("sanity")

    verdict = "PASS" if ok else "FAIL"
    body = (f"ED rigs/permits generator sanity check — {verdict}\n\n"
            + "\n".join(lines)
            + "\n\n(unit test only — no manifest, no retry-guard, not sent to zoho)\n")
    print(body)
    log_run("sanity", verdict, f"cron={cron_status.lower()}")
    if do_email:
        try:
            send_email(f"{SANITY_PREFIX}ED Generators Sanity Check — {verdict}",
                       body, to=args.to)
        except Exception as e:
            print(f"(summary email failed: {type(e).__name__}: {e})")

    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
