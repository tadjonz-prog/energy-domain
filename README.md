# Energy Domain — rig & permit reports

Python replacements for the legacy Enverus `rigs.py` / `permits.py` report
generators. **Only the data source changed** — Enverus `DeveloperAPIv3` out,
Energy Domain **DataStream Direct** (JDBC analog) in — while the pipe-delimited
output format is preserved byte-for-byte so the files drop straight into the
existing Zoho Reports pipeline.

Connection is the Python analog of the vendor's DBeaver/JDBC setup:
`jdbc:energy_domain://data-api.energydomain.com:443/energy_domain`.

## Layout

| File | Purpose |
|---|---|
| `ed_client.py` | Connection helper — `get_connection()` + `fetch_frame`, creds from `.env` |
| `rigs_ed.py` | Rigs report → `data/Rigs_ED_MM-DD-YYYY.txt` (52-col, active-rig wells) |
| `permits_ed.py` | Permits report → `data/Permits_ED_YYYY-MM-DD.txt` (79-col, 35–42 day permit window) |
| `rig_report.py` | Ad-hoc CSV/Parquet export of the rig join (exploration helper) |
| `test_connection.py` | Connection smoke test |
| `requirements.txt` | Pinned deps (`pandas<3` — see note below) |
| `.env.example` | Template for the gitignored `.env` |
| `systemd/` | `--user` service + timer units (daily runs on the spark dev box) |

## Setup

```bash
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
cp .env.example .env      # then fill in credentials (see below)
```

`.env` (gitignored, never committed) holds:

```
ED_USERNAME= / ED_PASSWORD=        # DataStream Direct login
ED_HOST=data-api.energydomain.com  # ED_PORT=443  ED_DATABASE=energy_domain
GMAIL_USER= / GMAIL_APP_PW=        # Gmail app password for email delivery
RIGS_EMAIL_TO=                     # comma-separated recipient(s) for both reports
```

## Run

```bash
./rigs_ed.py               # write the rigs file
./rigs_ed.py --email       # write + email it
./permits_ed.py            # write the permits file
./permits_ed.py --email    # write + email it
```

Output lands in `data/` (gitignored). Filenames carry an `ED_` prefix so they
never collide with the parallel Enverus files during the bake-off.

## Daily schedule (spark dev box)

`--user` systemd timers run both reports every morning (Mountain time) and email
`RIGS_EMAIL_TO`:

```bash
cp systemd/spark-*.{service,timer} ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now spark-rigs-ed.timer spark-permits-ed.timer
```

Current cadence: rigs 06:00 MT, permits 06:05 MT (`Persistent=true`, needs
`loginctl enable-linger`).

## Notes

- **`pandas` is pinned `<3`**: `datastream-direct` 0.1.4's `fetch_frame` predates
  pandas 3.0's default string dtype and crashes on it. Do not bump to 3.x until
  the vendor updates the library.
- **Bare `NULL` in SQL crashes the driver's type parser** — columns with no ED
  source are emitted as blanks in Python rather than selected as `NULL AS x`.
- The scripts use a host-agnostic `#!/usr/bin/env python3` shebang, but they
  must run under **this project's venv** (they need `datastream-direct`, pandas,
  etc.). Invoke via the venv explicitly — `./venv/bin/python rigs_ed.py` — or via
  the deploy wrapper / systemd unit that does so. A bare `./rigs_ed.py` would use
  system Python and fail on imports.
- `RIG_NUM` (rigs) is intentionally left blank pending a vendor rig-number field.
