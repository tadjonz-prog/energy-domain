# Rigs & Permits Generator — output specification

What this pipeline **produces**. Scope stops at "the file is complete and a
manifest says so." Reading the manifest and importing the data into Zoho
Analytics is a **separate process** with its own doc (`Import.md`, owned by the
import side); nothing about consuming these outputs belongs here.

## Reports

| Script | Output file | Format |
|---|---|---|
| `rigs_ed.py` | `data/Rigs_ED_YYYY-MM-DD.txt` | 52-column pipe-delimited, active-rig wells |
| `permits_ed.py` | `data/Permits_ED_YYYY-MM-DD.txt` | 79-column pipe-delimited, last-42-day permit window |

- Emailed when run with `--email` (weekly production runs → tad.jones + zoho).
- `--friday` pins `DATE_PROD`, the filename, and the permits window to the most
  recent Friday, so a Saturday retry still yields the Friday-dated report.

## The "ready" manifest — the completion signal

When a `--import-ready` run **fully succeeds** (file written, and emailed if
`--email`), the generator publishes one manifest:

```
data/ready/<report>_<YYYY-MM-DD>.json      # <report> = rigs | permits ; date = the Friday
```

Example — `data/ready/permits_2026-08-14.json`:
```json
{
  "report": "permits",
  "report_date": "2026-08-14",
  "file": "Permits_ED_2026-08-14.txt",
  "rows": 2047,
  "bytes": 450423,
  "sha256": "89f78f…64hex",
  "source": "prod2",
  "emailed": true,
  "generated_at": "2026-08-15T08:03:11-06:00",
  "status": "ready"
}
```

| Field | Meaning |
|---|---|
| `report` | `rigs` or `permits` |
| `report_date` | the pinned **Friday** (matches the filename and every `DATE_PROD`) |
| `file` | basename of the `.txt` in `data/` |
| `rows` | data-row count (excludes header) |
| `bytes` / `sha256` | size + checksum of that exact `.txt` |
| `source` | box that produced it (e.g. `prod2`) |
| `emailed` | whether the email also went out |
| `generated_at` | ISO 8601 with offset, America/Denver |
| `status` | always `ready` |

## Guarantees (what a consumer can rely on)

- **The manifest appears only after the `.txt` is complete** — written last, via
  temp-file + atomic `os.replace`, so it is never seen partially written.
- **It appears only once a good report exists** — a failed/retrying run writes
  nothing, so the manifest shows up whenever the first success lands (Friday
  evening or a Saturday retry), keyed to the Friday either way.
- **`sha256` / `rows` / `bytes` describe that exact file** — a consumer can
  verify integrity before use.
- **One manifest per report per ISO week** (the weekly `--once` guard sends once).
- **The generator only ever ADDS files under `data/` and `data/ready/`.** It
  never creates, reads, or deletes any `.imported` marker or import log — those
  belong entirely to the import side.

## Where these live

`data/` is gitignored and per-box; on prod2 that's `/root/energy-domain/data/`.
The generator's own run logs are `data/logs/rigs.log` / `permits.log` (report
creation only — not import activity).
