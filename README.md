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
| `permits_ed.py` | Permits report → `data/Permits_ED_YYYY-MM-DD.txt` (79-col, last-42-day / 6-week permit window) |
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

## Retry-until-success scheduling

The Energy Domain API occasionally returns transient `503`s (server busy /
maintenance); a single scheduled run that lands during one fails with
`too many 503 error responses`. To survive that, run the job **hourly** and let
the code skip once it has already succeeded, so a failed 06:00 run just retries
at 07:00… until one goes through.

Opt in with two environment variables on the cron/task line:

| Variable | Meaning |
|---|---|
| `RUN_ONCE_KEY` | state-file name (e.g. `rigs_daily`); omit to disable the guard |
| `RUN_ONCE_PERIOD` | `day` (default) or `week` — what "already done" resets on |

On success the script writes `data/state/<key>.txt` with the current period id;
subsequent hourly runs in the same period exit immediately as a no-op. If the
run fails (vendor 503, email error), nothing is written, so the next hour tries
again. State is per-box (`data/` is gitignored); with `RUN_ONCE_KEY` unset the
scripts always run, so manual runs and single-shot schedules are unchanged.

Example prod-2 crontab (daily rigs, retry hourly 06:00–22:00 MT until it sends):

```cron
0 6-22 * * * RIGS_EMAIL_TO=tad.jones@columbineco.com \
  RUN_ONCE_KEY=rigs_daily RUN_ONCE_PERIOD=day \
  /root/energy-domain/run.sh rigs_ed.py --email \
  >> /root/energy-domain/cron_rigs_daily.log 2>&1
```

## Notes

- **`pandas` is pinned `<3`**: `datastream-direct` 0.1.4's `fetch_frame` predates
  pandas 3.0's default string dtype and crashes on it. Do not bump to 3.x until
  the vendor updates the library.
- **Bare `NULL` in SQL crashes the driver's type parser** — columns with no ED
  source are emitted as blanks in Python rather than selected as `NULL AS x`.
- **Host-agnostic shebang.** `rigs_ed.py` / `permits_ed.py` use a shell/Python
  re-exec polyglot as their first two lines:

  ```
  #!/bin/sh
  ''''exec "$(dirname "$0")/venv/bin/python3" "$0" "$@" # '''
  ```

  `/bin/sh` runs line 2, which re-execs the script under the **venv sitting next
  to it** (`$(dirname "$0")/venv/bin/python3`) — so `./rigs_ed.py` works on any
  host regardless of the absolute path (spark `/home/tadjonz/...`, prod-2
  `/root/...`). To Python that line is just a harmless string literal, so the
  file is still valid Python. (`run.sh` / systemd units invoke the venv directly
  and don't rely on this.)
- `RIG_NUM` (rigs) is intentionally left blank pending a vendor rig-number field.
