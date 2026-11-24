import argparse
import json
from pathlib import Path


parser = argparse.ArgumentParser()
parser.add_argument("result", type=Path)
arguments = parser.parse_args()
result = json.loads(arguments.result.read_text())
assert result["completed"], "workload must complete before batching can be claimed"
calls = result["wall_profile"]["delta"]["membership"]["calls"]
transactions = 6 if result["workload"] == "import" else 1
bound = 44 * transactions + result["probe"]["live"]["attempts"] + 16
assert calls <= bound, f"membership called {calls} times; per-table-per-commit bound is {bound}, including probe commits"
print(f"membership batching contract passed: {calls} calls <= {bound}")
