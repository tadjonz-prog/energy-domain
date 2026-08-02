#!/home/tadjonz/energy-domain/venv/bin/python
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

SCOPE: permits.py queried permits ApprovedDate in a rolling 35-42-day-ago window
(a "6 weeks back" report). well_combined has 4.6M rows, so an unfiltered pull is
impractical; we keep the original's window via WINDOW_START/END_DAYS_AGO below.

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

from ed_client import get_connection

DELIM = "|"

# Rolling window matching permits.py (ApprovedDate 35-42 days ago). Change these
# two numbers to widen/shift the window.
WINDOW_START_DAYS_AGO = 42   # oldest permit_date included (exclusive lower bound)
WINDOW_END_DAYS_AGO   = 35   # newest permit_date included (inclusive upper bound)

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
QUERY = f"""
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
WHERE permit_date >  CURRENT_DATE - INTERVAL '{WINDOW_START_DAYS_AGO}' DAY
  AND permit_date <= CURRENT_DATE - INTERVAL '{WINDOW_END_DAYS_AGO}' DAY
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


def build_report():
    OUT_DIR.mkdir(exist_ok=True)
    report_date = datetime.date.today()
    dateprod = report_date.strftime("%m/%d/%Y")
    out_path = OUT_DIR / f"Permits_ED_{report_date.strftime('%Y-%m-%d')}.txt"

    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute(QUERY); cur.fetchall()
        rows = list(cur.rows)
        idx = {name: i for i, name in enumerate(cur.headers)}
    finally:
        conn.close()

    n = 0
    with open(out_path, "w") as f:
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

    return out_path, n


def email_report(path):
    """Opt-in email delivery. Creds + recipients from .env."""
    import smtplib
    from email.mime.multipart import MIMEMultipart
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
    msg["Subject"] = "Permits Report - Energy Domain for " + dateprod
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
    import sys
    out_path, n = build_report()
    print(f"Wrote {n} rows -> {out_path}  ({out_path.stat().st_size:,} bytes)")
    print(f"  window: permit_date {WINDOW_START_DAYS_AGO}-{WINDOW_END_DAYS_AGO} days ago")
    if "--email" in sys.argv:
        email_report(out_path)
        print("Email sent.")
    else:
        print("(file only — run  ./permits_ed.py --email  to also send it)")


if __name__ == "__main__":
    main()
