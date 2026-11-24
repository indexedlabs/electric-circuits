import json
import os
from pathlib import Path

from data import IMPORT_ACCOUNT, LARGE_ACCOUNT, load_data


folder = Path(__file__).parent / "results/fixture"
data = load_data(folder / "columns.json")
url = os.environ["STALL_REPRO_PG_URL"]
seed = data.seed(url)
imported = data.calendar_import(url, lambda connection, transaction: None)
large = data.delete_account(url, LARGE_ACCOUNT, lambda connection, transaction: None)
small = data.delete_account(url, IMPORT_ACCOUNT, lambda connection, transaction: None)
assert sum(entry["rows"] for entry in imported) == 13081
assert sum(entry["rows"] for entry in large) == 29793
(folder / "workload-smoke.json").write_text(json.dumps({"seed": seed, "import": imported, "large_delete": large, "small_delete": small, "engine_running": False}, indent=2))
print("PostgreSQL-only smoke: 13081 import changes, 29793 large deletion changes; not engine performance evidence")
