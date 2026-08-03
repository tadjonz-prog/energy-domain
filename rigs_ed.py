#!/bin/sh
''''exec "$(dirname "$0")/venv/bin/python3" "$0" "$@" # '''
"""
Rigs report — Energy Domain edition.

Port of rigs.py (Enverus DeveloperAPIv3) to the Energy Domain DataStream Direct
source. The OUTPUT CONTRACT is preserved byte-for-byte so this drops straight
into the same downstream (Zoho Reports) pipeline:
  * 52-column pipe-delimited header (identical to rigs.py)
  * data rows emitted at full 52-column width (OPER_STATE/OPER_ZIP as trailing
    empties) so every row matches the header
  * filename  data/Rigs_ED_MM-DD-YYYY.txt  (ED_ prefix distinguishes it from the
    parallel Enverus Rigs_MM-DD-YYYY.txt during the bake-off)
  * DATE_PROD = today as MM/DD/YYYY; DATE_APRV/DATE_SPUD reformatted to MM/DD/YYYY
  * optional email delivery (opt-in; creds from .env, never hard-coded)

ONLY the data-retrieval layer changed: Enverus API  ->  Energy Domain SQL
(well_combined JOIN current_rigs ON well_api).

Report column  <-  Energy Domain field
  DATE_PROD    <-  today (MM/DD/YYYY)
  STATE_NAME   <-  wc.state_abbr
  DIST_NAME    <-  wc.rrc_district_number  (TX RRC district; blank for non-TX,
                   matching the original's TX-only district population)
  COUNTY_SL    <-  wc.county
  WELLNAM_SL   <-  wc.well_name          (Enverus LeaseName equivalent)
  WELL_ORNT    <-  wc.drill_type         (Enverus Trajectory)
  FIELD_1      <-  wc.field
  API_NUM      <-  wc.well_api           (Enverus API_UWI)
  WELL_CLASS   <-  wc.well_type          (Enverus StateWellType)
  DATE_APRV    <-  wc.permit_date        (MM/DD/YYYY)
  DATE_SPUD    <-  wc.spud_date          (MM/DD/YYYY)
  DRIL_NAME    <-  cr.driller            (Enverus ContractorName)
  RIG_NUM      <-  (COMMENTED OUT — pending vendor; Enverus RigName_Number)
  P_T_D        <-  wc.measured_depth
  WELL_DEPTH   <-  wc.true_vertical_depth  (NOTE: rigs.py used measured depth here)
  MEAS_DEPTH   <-  wc.measured_depth
  OPER_NAME    <-  cr.operator_name
  OPER_CITY    <-  (blank — no Energy Domain source)
All other columns emitted empty, matching rigs.py's layout.
"""
import os
import datetime
from pathlib import Path

from ed_client import get_connection

DELIM = "|"

# 52-column header — identical to rigs.py
HEADERS = [
    "DATE_PROD", "MAST_ID", "WELL_ID", "NEW_WELL", "ORPH_WELL", "STATE_NAME",
    "DIST_NAME", "COUNTY_SL", "WELLNAM_SL", "WELLNUM_SL", "LEGAL_SL", "WELL_ORNT",
    "COUNTY_BH", "LEGAL_BH", "FIELD_1", "FIELD_2", "FIELD_3", "FIELD_4", "FIELD_5",
    "API_NUM", "PERM_NUM", "WELL_PURP", "JOB_NUM", "WELL_CLASS", "WORK_TYPE", "H2S_GAS",
    "DATE_APRV", "DATE_SPUD", "DATE_RELS", "WELL_PROX", "DRIL_NAME", "DRIL_CONT",
    "DRIL_PHONE", "DRIL_ADDR", "DRIL_CITY", "DRIL_STATE", "DRIL_ZIP", "RIG_NUM",
    "DRIL_TERM", "P_T_D", "WELL_DEPTH", "MEAS_DEPTH", "ADD_INFO", "RECRD_STAT",
    "WELL_ACTV", "OPER_NAME", "OPER_CONT", "OPER_PHONE", "OPER_ADDR", "OPER_CITY",
    "OPER_STATE", "OPER_ZIP",
]
# Emit the FULL 52-column width every row (OPER_STATE/OPER_ZIP as trailing
# empties). rigs.py truncated rows at OPER_CITY (50 fields); we keep rows the
# same width as the header for consistency, even though those two have no data.

# Data-retrieval layer: Energy Domain SQL (replaces the Enverus d3.query call)
QUERY = """
SELECT
    wc.state_abbr,
    wc.rrc_district_number,
    wc.county,
    wc.well_name,
    wc.drill_type,
    wc.field,
    wc.well_api,
    wc.well_type,
    wc.permit_date,
    wc.spud_date,
    cr.driller,
    wc.measured_depth,
    wc.true_vertical_depth,
    cr.operator_name
FROM well_combined wc
JOIN current_rigs cr ON wc.well_api = cr.well_api
"""

OUT_DIR = Path(__file__).with_name("data")


def _mdy(v):
    """ISO date string (YYYY-MM-DD) -> MM/DD/YYYY; blank if empty. Mirrors rigs.py."""
    if v is None:
        return ""
    s = str(v)
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return f"{s[5:7]}/{s[8:10]}/{s[0:4]}"
    return s


def _txt(v):
    """Blank for null (rigs.py wrote the literal 'None'; blank is cleaner for Zoho)."""
    return "" if v is None else str(v)


def _wellnum(name):
    """Best-effort well number split from the well name.

    Energy Domain has no separate well-number field (Enverus did), so we derive
    it as the trailing whitespace-delimited token of the well name (e.g.
    "MORTON 19A" -> "19A"). The well name is left FULLY intact, so the number is
    repeated in WELLNUM_SL. Multi-token numbers capture only the last token.
    """
    if not name:
        return ""
    parts = str(name).split()
    return parts[-1] if parts else ""


def _summary(total, state_counts):
    """Plain-text stats for the email body / log: total + per-state counts."""
    lines = [f"Total Rigs: {total}", "", "Rigs by State:"]
    for st, c in sorted(state_counts.items(), key=lambda kv: (-kv[1], kv[0])):
        lines.append(f"  {st:<6} {c}")
    return "\n".join(lines)


def build_report():
    OUT_DIR.mkdir(exist_ok=True)
    report_date = datetime.date.today()
    dateprod = report_date.strftime("%m/%d/%Y")
    out_path = OUT_DIR / f"Rigs_ED_{report_date.strftime('%m-%d-%Y')}.txt"

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(QUERY)
        cur.fetchall()
        rows = list(cur.rows)
        idx = {name: i for i, name in enumerate(cur.headers)}
    finally:
        conn.close()

    n = 0
    with open(out_path, "w") as f:
        f.write(DELIM.join(HEADERS) + "\n")  # full 52-col header
        for r in rows:
            def g(col):
                return r[idx[col]]

            cols = [""] * len(HEADERS)
            cols[HEADERS.index("DATE_PROD")]  = dateprod
            cols[HEADERS.index("STATE_NAME")] = _txt(g("state_abbr"))
            cols[HEADERS.index("DIST_NAME")]   = _txt(g("rrc_district_number"))
            cols[HEADERS.index("COUNTY_SL")]  = _txt(g("county"))
            cols[HEADERS.index("WELLNAM_SL")] = _txt(g("well_name"))
            cols[HEADERS.index("WELLNUM_SL")] = _wellnum(g("well_name"))
            cols[HEADERS.index("WELL_ORNT")]  = _txt(g("drill_type"))
            cols[HEADERS.index("FIELD_1")]    = _txt(g("field"))
            cols[HEADERS.index("API_NUM")]    = _txt(g("well_api"))
            cols[HEADERS.index("WELL_CLASS")] = _txt(g("well_type"))
            cols[HEADERS.index("DATE_APRV")]  = _mdy(g("permit_date"))
            cols[HEADERS.index("DATE_SPUD")]  = _mdy(g("spud_date"))
            cols[HEADERS.index("DRIL_NAME")]  = _txt(g("driller"))
            # RIG_NUM commented out pending vendor:
            # cols[HEADERS.index("RIG_NUM")]  = _txt(g("<vendor rig number field>"))
            cols[HEADERS.index("P_T_D")]      = _txt(g("measured_depth"))
            cols[HEADERS.index("WELL_DEPTH")] = _txt(g("true_vertical_depth"))
            cols[HEADERS.index("MEAS_DEPTH")] = _txt(g("measured_depth"))
            cols[HEADERS.index("OPER_NAME")]  = _txt(g("operator_name"))
            # OPER_CITY left blank (no Energy Domain source)

            f.write(DELIM.join(cols) + "\n")  # full 52-column width
            n += 1

    # email-body stats: total + rigs-by-state
    state_counts = {}
    for r in rows:
        st = _txt(r[idx["state_abbr"]]) or "(blank)"
        state_counts[st] = state_counts.get(st, 0) + 1

    return out_path, n, _summary(n, state_counts)


def email_report(path, body=""):
    """Opt-in email delivery (rigs.py port). Creds + recipients from .env."""
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders

    sender = os.getenv("GMAIL_USER")
    app_pw = os.getenv("GMAIL_APP_PW")
    recips = [x.strip() for x in os.getenv("RIGS_EMAIL_TO", "").split(",") if x.strip()]
    if not (sender and app_pw and recips):
        raise RuntimeError("Set GMAIL_USER, GMAIL_APP_PW, RIGS_EMAIL_TO in .env to email.")

    dateprod = datetime.date.today().strftime("%m/%d/%Y")
    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = ", ".join(recips)
    msg["Subject"] = "Rigs Report - Energy Domain for " + dateprod
    if body:
        msg.attach(MIMEText(body + "\n", "plain"))
    with open(path, "rb") as fh:
        obj = MIMEBase("application", "octet-stream")
        obj.set_payload(fh.read())
    encoders.encode_base64(obj)
    obj.add_header("Content-Disposition", f"attachment; filename= {path.name}")
    msg.attach(obj)

    s = smtplib.SMTP("smtp.gmail.com", 587)
    s.starttls()
    s.login(sender, app_pw)
    s.sendmail(sender, recips, msg.as_string())
    s.quit()


def main():
    import sys
    out_path, n, summary = build_report()
    print(f"Wrote {n} rows -> {out_path}  ({out_path.stat().st_size:,} bytes)")
    print(summary)
    if "--email" in sys.argv:
        email_report(out_path, summary)
        print("Email sent.")
    else:
        print("(file only — run  ./rigs_ed.py --email  to also send it)")


if __name__ == "__main__":
    main()
