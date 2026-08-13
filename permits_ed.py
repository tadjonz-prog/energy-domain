#!/bin/sh
''''exec "$(dirname "$0")/venv/bin/python3" "$0" "$@" # '''
"""
Permits report — Energy Domain edition.

Port of permits.py (Enverus DeveloperAPIv3) to the Energy Domain DataStream
Direct source. The OUTPUT CONTRACT is preserved so it drops into the same
downstream (Zoho Reports) pipeline:
  * 79-column pipe-delimited header (identical to permits.py)
  * full-width data rows (permits.py wrote all 79 columns)
  * filename  data/Permits_ED_YYYY-MM-DD.txt  (ED_ prefix distinguishes it from
    the parallel Enverus Permits_YYYY-MM-DD.txt during the bake-off)
  * DATE_PROD = today (MM/DD/YYYY); DATE_APRV reformatted to MM/DD/YYYY
  * optional email delivery (opt-in; creds from .env, never hard-coded)

ONLY the data-retrieval layer changed: Enverus API -> Energy Domain SQL.

SCOPE: rolling "last 6 weeks" window on permit_date (today back WINDOW_START_DAYS_AGO
days). well_combined has 4.6M rows, so an unfiltered pull is impractical; the window
is set via WINDOW_START/END_DAYS_AGO below. The email stats bin the approved dates
into fixed 7-day (weekly) buckets across that window — 42 days / 7 = 6 buckets.

Report column  <-  Energy Domain field  (per the operator-supplied permits SQL)
  DATE_PROD    <-  today (MM/DD/YYYY)      [SQL had NULL; original fills today]
  STATE_NAME   <-  state_abbr
  DIST_NAME    <-  rrc_district_number  (TX RRC district; blank non-TX)
  COUNTY_SL    <-  county
  WELLNAM_SL   <-  well_name
  WELL_ORNT    <-  drill_type
  FIELD_1      <-  field
  API_NUM      <-  well_api
  WELL_CLASS   <-  well_product           [note: rigs_ed used well_type]
  WORK_TYPE    <-  (blank per SQL NULL)
  DATE_POST    <-  wad_updated_on (MM/DD/YYYY)  [Enverus UpdatedDate equivalent]
  DATE_APRV    <-  permit_date (MM/DD/YYYY)
  P_T_D        <-  measured_depth  (permit total depth; sparse — ~23% populated)
  WELL_LONG    <-  longitude
  WELL_LAT     <-  latitude
  OPER_NAME    <-  operator_name
  OPER_CONT/PHONE/ADDR/CITY/STATE/ZIP  <-  (blank per SQL NULL)
All other columns emitted empty, matching permits.py's layout.
"""
import os
import datetime
from pathlib import Path

from ed_client import (
    get_connection, already_succeeded, mark_succeeded, resolve_report_date,
    report_source, run_period_id, log_run, log_error, parse_run_args,
    once_period_warning,
)

DELIM = "|"

# Rolling "last 6 weeks" window on permit_date. Change these two numbers to
# widen/shift the window; the weekly stat buckets follow WINDOW_START_DAYS_AGO.
WINDOW_START_DAYS_AGO = 42   # oldest permit_date included (exclusive lower bound)
WINDOW_END_DAYS_AGO   = 0    # newest permit_date included (inclusive; 0 = today)
BUCKET_WEEK_DAYS      = 7     # width of each date-approved stat bucket (1 week)

# 79-column header — identical to permits.py
HEADERS = [
    "DATE_PROD", "MAST_ID", "WELL_ID", "STATE_NAME", "DIST_NAME", "COUNTY_SL",
    "WELLNAM_SL", "WELLNUM_SL", "LEGAL_SL", "SEC_SL", "TWN_SL", "TWNDIR_SL",
    "RNG_SL", "RNGDIR_SL", "FTG1_SL", "FTG1DIR_SL", "FTG1REF_SL", "FTG2_SL",
    "FTG2DIR_SL", "FTG2REF_SL", "QTRSEC_SL", "FTGCOR_SL", "CRTRQD1_SL",
    "CRTRQD2_SL", "ABSTNO_SL", "BLKNAM_SL", "BLKNUM_SL", "SRVYNAM_SL",
    "SRVYNUM_SL", "WELL_ORNT", "COUNTY_BH", "LEGAL_BH", "SEC_BH", "TWN_BH",
    "TWNDIR_BH", "RNG_BH", "RNGDIR_BH", "FTG1_BH", "FTG1DIR_BH", "FTG1REF_BH",
    "FTG2_BH", "FTG2DIR_BH", "FTG2REF_BH", "QTRSEC_BH", "FTGCOR_BH", "CRTRQD1_BH",
    "CRTRQD2_BH", "ABSTNO_BH", "BLKNAM_BH", "BLKNUM_BH", "SRVYNAM_BH",
    "SRVYNUM_BH", "FIELD_1", "FIELD_2", "FIELD_3", "FIELD_4", "FIELD_5",
    "API_NUM", "PERM_NUM", "WELL_PURP", "WELL_CLASS", "WORK_TYPE", "H2S_GAS",
    "DATE_POST", "DATE_APRV", "WELL_PROX", "P_T_D", "ADD_INFO", "RECRD_STAT",
    "MAP_REF", "WELL_LONG", "WELL_LAT", "OPER_NAME", "OPER_CONT", "OPER_PHONE",
    "OPER_ADDR", "OPER_CITY", "OPER_STATE", "OPER_ZIP",
]
assert len(HEADERS) == 79, len(HEADERS)

# Real columns only (bare NULL literals crash the driver's type parser, so the
# SQL's NULL-mapped columns are simply emitted blank in Python instead).
def _build_query(report_date):
    """Permits query with the 42-day window anchored on report_date instead of
    the server's CURRENT_DATE. For a weekly run this is the pinned Friday, so a
    Saturday retry still pulls the SAME [Friday-42 .. Friday] window (keeping
    Zoho's Friday-ending pivots stable); for daily runs report_date is today,
    identical to the old CURRENT_DATE behaviour. The date is our own ISO string,
    injected as a Trino DATE literal (no user input, no injection risk)."""
    ref = report_date.isoformat()
    return f"""
SELECT
    state_abbr,
    rrc_district_number,
    county,
    well_name,
    drill_type,
    field,
    well_api,
    well_product,
    permit_date,
    wad_updated_on,
    measured_depth,
    longitude,
    latitude,
    operator_name
FROM well_combined
WHERE permit_date >  DATE '{ref}' - INTERVAL '{WINDOW_START_DAYS_AGO}' DAY
  AND permit_date <= DATE '{ref}' - INTERVAL '{WINDOW_END_DAYS_AGO}' DAY
"""

OUT_DIR = Path(__file__).with_name("data")


def _mdy(v):
    if v is None:
        return ""
    s = str(v)
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return f"{s[5:7]}/{s[8:10]}/{s[0:4]}"
    return s


def _txt(v):
    return "" if v is None else str(v)


def _wellnum(name):
    """Best-effort well number split from the well name.

    Energy Domain has no separate well-number field (Enverus did), so we derive
    it as the trailing whitespace-delimited token of the well name (e.g.
    "MORTON 19A" -> "19A"). The well name is left FULLY intact, so the number is
    repeated in WELLNUM_SL. Multi-token numbers (e.g. "... 23 NXH") capture only
    the last token; refine here if a smarter rule is wanted.
    """
    if not name:
        return ""
    parts = str(name).split()
    return parts[-1] if parts else ""


def _bucket_meta(as_of, window_days=WINDOW_START_DAYS_AGO, week=BUCKET_WEEK_DAYS):
    """Weekly-bucket layout: (nbuckets, column_labels, span_notes). 42/7 = 6.
    Buckets run most-recent-first: bucket 0 = [0, week) days ago, measured from
    `as_of` (the report date — the pinned Friday for weekly runs), so the labels
    match the data window rather than the actual run day."""
    nbuckets = max(1, -(-window_days // week))          # ceil(window / week)
    cols, spans = [], []
    for i in range(nbuckets):
        lo, hi = i * week, (i + 1) * week               # [lo, hi) days ago
        cols.append(f"{lo}-{hi}d")
        newest = as_of - datetime.timedelta(days=lo)
        oldest = as_of - datetime.timedelta(days=hi - 1)
        spans.append(f"{lo}-{hi}d ago = {oldest:%m/%d}–{newest:%m/%d}")
    return nbuckets, cols, spans


def _week_index(d, as_of, nbuckets, week=BUCKET_WEEK_DAYS):
    """Weekly bucket index for an approved date (0 = most recent week), relative
    to `as_of` (the report date)."""
    i = (as_of - d).days // week
    return 0 if i < 0 else (nbuckets - 1 if i >= nbuckets else i)


def _summary(total, records, as_of):
    """Plain-text stats: total + a per-state table whose columns are the state
    total followed by the 6 weekly Date-Approved bucket counts.

    records is a list of (state_abbr, approved_date_or_None) — one per report row.
    Buckets are measured from `as_of` (the report date / pinned Friday).
    """
    nbuckets, cols, spans = _bucket_meta(as_of)

    per = {}                                             # state -> [total, w0, w1, ...]
    for st, d in records:
        row = per.setdefault(st, [0] * (nbuckets + 1))
        row[0] += 1
        if d:
            row[1 + _week_index(d, as_of, nbuckets)] += 1

    W = 8                                                # column width
    header = f"  {'State':<8}{'Total':>{W}}  " + "  ".join(f"{c:>{W}}" for c in cols)
    lines = [f"Total Permits: {total}", "",
             "Permits by State — state total + weekly Date-Approved buckets (days ago):",
             header]
    grand = [0] * (nbuckets + 1)
    for st, row in sorted(per.items(), key=lambda kv: (-kv[1][0], kv[0])):
        lines.append(f"  {st:<8}{row[0]:>{W}}  " + "  ".join(f"{v:>{W}}" for v in row[1:]))
        for j in range(nbuckets + 1):
            grand[j] += row[j]
    lines.append(f"  {'TOTAL':<8}{grand[0]:>{W}}  " + "  ".join(f"{v:>{W}}" for v in grand[1:]))
    lines += ["", "Bucket spans:"] + [f"  {s}" for s in spans]
    return "\n".join(lines)


def build_report(anchor=None):
    OUT_DIR.mkdir(exist_ok=True)
    report_date = resolve_report_date(anchor)   # today, or pinned weekday (--friday)
    dateprod = report_date.strftime("%m/%d/%Y")
    out_path = OUT_DIR / f"Permits_ED_{report_date.strftime('%Y-%m-%d')}.txt"

    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute(_build_query(report_date)); cur.fetchall()
        rows = list(cur.rows)
        idx = {name: i for i, name in enumerate(cur.headers)}
    finally:
        conn.close()

    n = 0
    with open(out_path, "w", newline="\n") as f:  # LF on every OS (Windows-safe)
        f.write(DELIM.join(HEADERS) + "\n")
        for r in rows:
            def g(col):
                return r[idx[col]]

            cols = [""] * len(HEADERS)
            cols[HEADERS.index("DATE_PROD")]  = dateprod
            cols[HEADERS.index("STATE_NAME")] = _txt(g("state_abbr"))
            cols[HEADERS.index("DIST_NAME")]  = _txt(g("rrc_district_number"))
            cols[HEADERS.index("COUNTY_SL")]  = _txt(g("county"))
            cols[HEADERS.index("WELLNAM_SL")] = _txt(g("well_name"))
            cols[HEADERS.index("WELLNUM_SL")] = _wellnum(g("well_name"))
            cols[HEADERS.index("WELL_ORNT")]  = _txt(g("drill_type"))
            cols[HEADERS.index("FIELD_1")]    = _txt(g("field"))
            cols[HEADERS.index("API_NUM")]    = _txt(g("well_api"))
            cols[HEADERS.index("WELL_CLASS")] = _txt(g("well_product"))
            cols[HEADERS.index("DATE_POST")]  = _mdy(g("wad_updated_on"))
            # WORK_TYPE blank (SQL NULL)
            cols[HEADERS.index("DATE_APRV")]  = _mdy(g("permit_date"))
            cols[HEADERS.index("P_T_D")]      = _txt(g("measured_depth"))
            cols[HEADERS.index("WELL_LONG")]  = _txt(g("longitude"))
            cols[HEADERS.index("WELL_LAT")]   = _txt(g("latitude"))
            cols[HEADERS.index("OPER_NAME")]  = _txt(g("operator_name"))
            # OPER_CONT/PHONE/ADDR/CITY/STATE/ZIP blank (SQL NULL)

            f.write(DELIM.join(cols) + "\n")   # full 79-column width
            n += 1

    # email-body stats: per-state table with weekly Date-Approved buckets
    records = []
    for r in rows:
        st = _txt(r[idx["state_abbr"]]) or "(blank)"
        pd = r[idx["permit_date"]]
        d = None
        if pd:
            try:
                d = datetime.date.fromisoformat(str(pd)[:10])
            except ValueError:
                d = None
        records.append((st, d))

    return out_path, n, _summary(n, records, report_date)


def email_report(path, body="", to=None, report_date=None):
    """Opt-in email delivery. Recipients: --to override, else RIGS_EMAIL_TO from
    .env. Subject date follows the report date (--friday-aware)."""
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders

    sender = os.getenv("GMAIL_USER")
    app_pw = os.getenv("GMAIL_APP_PW")
    recips_src = to if to else os.getenv("RIGS_EMAIL_TO", "")
    recips = [x.strip() for x in recips_src.split(",") if x.strip()]
    if not (sender and app_pw and recips):
        raise RuntimeError("Set GMAIL_USER, GMAIL_APP_PW, and recipients "
                           "(--to or RIGS_EMAIL_TO in .env) to email.")

    dateprod = (report_date or datetime.date.today()).strftime("%m/%d/%Y")
    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = ", ".join(recips)
    msg["Subject"] = "Permits Report - Energy Domain for " + dateprod + f" -{report_source()}"
    if body:
        # Send the stats as BOTH plain text and an HTML <pre> block. Mail clients
        # render text/plain in a proportional font (columns misalign); the <pre>
        # HTML part forces monospace so the table lines up. multipart/alternative
        # lets each client pick the richer part it supports.
        import html as _html
        alt = MIMEMultipart("alternative")
        alt.attach(MIMEText(body + "\n", "plain"))
        alt.attach(MIMEText(
            f'<pre style="font-family:\'Courier New\',Consolas,monospace;'
            f'font-size:13px;margin:0">{_html.escape(body)}</pre>', "html"))
        msg.attach(alt)
    with open(path, "rb") as fh:
        obj = MIMEBase("application", "octet-stream")
        obj.set_payload(fh.read())
    encoders.encode_base64(obj)
    obj.add_header("Content-Disposition", f"attachment; filename= {path.name}")
    msg.attach(obj)

    s = smtplib.SMTP("smtp.gmail.com", 587)
    s.starttls(); s.login(sender, app_pw)
    s.sendmail(sender, recips, msg.as_string()); s.quit()


def main():
    import sys, time
    args = parse_run_args()
    warn = once_period_warning(args.once, args.period)
    if warn:
        print(f"WARN: {warn}", file=sys.stderr)
        log_run("permits", "WARN", warn)
    if already_succeeded(args.once, args.period):
        print(f"[{args.once}] already succeeded this period — skipping (nothing to do).")
        log_run("permits", "SKIP", f"already succeeded {run_period_id(args.period)}")
        return
    report_date = resolve_report_date(args.anchor)
    t0 = time.monotonic()
    try:
        out_path, n, summary = build_report(args.anchor)
        print(f"Wrote {n} rows -> {out_path}  ({out_path.stat().st_size:,} bytes)")
        print(f"  window: permit_date last {WINDOW_START_DAYS_AGO} days "
              f"({WINDOW_START_DAYS_AGO}-{WINDOW_END_DAYS_AGO} days ago)")
        print(summary)
        if args.email:
            email_report(out_path, summary, to=args.to, report_date=report_date)
            print("Email sent.")
        else:
            print("(file only — run  ./permits_ed.py --email  to also send it)")
        mark_succeeded(args.once, args.period)  # only after full success (incl. email)
    except Exception as e:
        log_run("permits", "FAIL", f"{type(e).__name__}: {e}", time.monotonic() - t0)
        log_error("permits")   # full traceback -> data/logs/permits.errors.log
        raise
    log_run("permits", "OK",
            f"rows={n} date_prod={report_date:%m/%d/%Y} "
            f"email={'sent' if args.email else 'no'}", time.monotonic() - t0)


if __name__ == "__main__":
    main()
