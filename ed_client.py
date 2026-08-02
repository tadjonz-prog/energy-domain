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


__all__ = ["get_connection", "connect", "fetch_frame"]
