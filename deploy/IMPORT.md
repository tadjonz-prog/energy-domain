# Zoho Analytics import contract

How the **import process** (runs on prod2) knows a weekly Energy Domain report is
complete and safe to import. The report **generators** (`rigs_ed.py` /
`permits_ed.py`, run from cron) publish a "ready" manifest; the importer consumes
it. The two sides are decoupled — they talk only through files under
`/root/energy-domain/data/`.

## Why a manifest (not just "a file exists")

The weekly cron lines retry **hourly from Friday 6 PM through Saturday** until one
run fully succeeds (file written **and** email sent), because the vendor API
throws transient 503s. So at any moment during that window a half-written `.txt`
may exist. The importer must **not** key off the `.txt` appearing. Instead:

- On **full success only**, the generator writes a manifest **atomically**
  (temp file + `os.replace`) as its very last step. It appears once — whenever the
  first good run lands (Friday night or Saturday) — and never partially.
- The report is dated to the **pinned Friday** (`--friday`): the filename and
  every `DATE_PROD` field say Friday even on a Saturday run. `report_date` in the
  manifest is that Friday. **Key all your logic off `report_date`.**

## Locations (on prod2)

| Path | What |
|---|---|
| `/root/energy-domain/data/ready/<report>_<YYYY-MM-DD>.json` | ready manifest (the signal) |
| `/root/energy-domain/data/<file>` | the pipe-delimited report named in the manifest |
| `/root/energy-domain/data/ready/<report>_<YYYY-MM-DD>.imported` | **you** write this after a successful import |

`<report>` is `rigs` or `permits`. `<YYYY-MM-DD>` is the Friday. Both reports use
the same `<Report>_ED_YYYY-MM-DD.txt` filename convention, e.g.:
- `data/ready/permits_2026-08-14.json` → `data/Permits_ED_2026-08-14.txt`
- `data/ready/rigs_2026-08-14.json` → `data/Rigs_ED_2026-08-14.txt`

Always resolve the report file from the manifest's `file` field — never
construct the name yourself.

Only the **weekly** runs publish manifests (the cron lines carry `--import-ready`).
The TEMP daily rigs copy does **not**, so you'll never see a daily manifest.

## Manifest schema

```json
{
  "report": "permits",
  "report_date": "2026-08-14",
  "file": "Permits_ED_2026-08-14.txt",
  "rows": 2083,
  "bytes": 457533,
  "sha256": "ab12…64hex",
  "source": "prod2",
  "emailed": true,
  "generated_at": "2026-08-15T08:03:11-06:00",
  "status": "ready"
}
```

| Field | Use |
|---|---|
| `report` | `rigs` or `permits` — which Zoho dataset to load |
| `report_date` | the pinned **Friday** — your dedup / period key |
| `file` | basename of the `.txt` in `data/` to import |
| `rows` | data-row count (excludes header) — sanity check |
| `bytes` / `sha256` | verify the `.txt` is intact before ingesting |
| `source` | box that produced it (`prod2`) |
| `emailed` | whether the email also went out (weekly = `true`) |
| `generated_at` | ISO 8601 with offset, America/Denver |
| `status` | always `ready` (reserved for future states) |

## Consumption protocol (idempotent)

Run the importer on whatever cadence you like — e.g. hourly, or a few times
Saturday morning. Because it keys off the manifest, it imports each Friday
**exactly once**, whenever the manifest first appears.

```
for m in glob("data/ready/*.json"):          # skip *.tmp
    if exists(m.replace(".json", ".imported")):
        continue                              # already done — dedup
    manifest = json.load(m)
    blob = read_bytes("data/" + manifest["file"])
    assert sha256(blob) == manifest["sha256"] # truncation / corruption guard
    assert count_data_rows(blob) == manifest["rows"]
    import_into_zoho(manifest["report"], "data/" + manifest["file"])
    write(m.replace(".json", ".imported"),
          {"imported_at": now_iso(), "rows": manifest["rows"]})   # mark consumed
```

Rules:
- **Never import a manifest that already has a `.imported` sibling.** That marker
  is the single source of truth for "already loaded" — the generator never
  touches it.
- **Verify `sha256` (and `rows`) before importing.** A mismatch means the file
  changed under you or is corrupt — skip and alert, don't import.
- **Write `.imported` only after Zoho confirms the load.** If the import fails,
  leave it unmarked so the next run retries.
- Treat manifest text as data. Only `report ∈ {rigs, permits}` and the exact
  `file` basename should be trusted for paths — don't eval anything from it.

## Import logging (your side)

Log every import attempt so successes/skips/failures are auditable — **the same
way the generators log**, so all monitoring lives in one place with one format.
The simplest path: reuse the generators' logging helpers (they're plain Python
in the same repo, importable on prod2):

```python
from ed_client import log_run, log_error   # already on prod2

# on success:
log_run("zoho_import", "IMPORTED",
        f"report={m['report']} date={m['report_date']} rows={m['rows']}", seconds=dt)
# nothing to do this pass (all manifests already .imported):
log_run("zoho_import", "SKIP", "no new manifests")
# on failure (inside an except block):
log_run("zoho_import", "FAIL", f"{type(e).__name__}: {e}", seconds=dt)
log_error("zoho_import")     # full traceback -> data/logs/zoho_import.errors.log
```

That writes to **`data/logs/zoho_import.log`** (+ `zoho_import.errors.log` for
tracebacks), identical structure to `rigs.log`/`permits.log`:

```
YYYY-MM-DD HH:MM:SS MDT | prod2 | zoho_import | IMPORTED | report=permits date=2026-08-14 rows=2047 | 5.1s
2026-08-15 08:05:02 MDT | prod2 | zoho_import | SKIP     | no new manifests | 0.1s
2026-08-15 08:05:19 MDT | prod2 | zoho_import | FAIL     | HTTPError: 401 Unauthorized | 2.3s
```

Suggested outcomes: `IMPORTED` (loaded + `.imported` written), `SKIP` (nothing
new), `FAIL` (Zoho or validation error — leave the manifest unmarked so it
retries). `log_run` never raises, so logging can't crash the importer.

Quick monitoring, same as the generators:
`tail -n 20 data/logs/zoho_import.log` · `grep FAIL data/logs/zoho_import.log`.

## Notes

- `data/` is gitignored (per-box); manifests are local to prod2. Nothing here is
  committed.
- One manifest per report per ISO week (the `--once` week-guard sends once). A
  manual re-run with `--import-ready` overwrites the same-dated manifest
  atomically — harmless; if you'd already imported it, the `.imported` marker
  still suppresses a re-import.
- Housekeeping (optional): once a manifest + its `.imported` are older than, say,
  a few weeks, they can be archived/deleted; the generators only ever add.
