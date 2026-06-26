#!/usr/bin/env python3
"""Pin Lakekeeper's vended (STS) table credentials to a fixed, short validity.

The access-control-simple example already creates its warehouse with
``sts-enabled: true`` (credential vending on). This script additionally pins
``sts-token-validity-seconds`` in the warehouse storage-profile so vended table
credentials expire quickly, which is what we want when testing credential
refresh.

MinIO enforces a hard 900-second (15 minute) minimum on STS AssumeRole, so 900
is the shortest validity that actually works against the example's MinIO backend.

The edit is idempotent: running it repeatedly (e.g. on every ``make lakekeeper``)
is a no-op once the validity is set, and it works on an already-cloned repo.
"""
import json
import os
import sys

DEFAULT_NOTEBOOK = ".catalogs/lakekeeper/examples/access-control-simple/notebooks/02-Create-Warehouse.ipynb"
STS_VALIDITY_SECONDS = 900


def patch_notebook(nb_path: str) -> int:
    if not os.path.exists(nb_path):
        print(f"[lakekeeper-sts] notebook not found: {nb_path}", file=sys.stderr)
        return 1

    with open(nb_path) as f:
        nb = json.load(f)

    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        src = "".join(cell.get("source", []))
        if "/v1/warehouse" not in src or "storage-profile" not in src:
            continue

        if "sts-token-validity-seconds" in src:
            print("[lakekeeper-sts] sts-token-validity-seconds already set; nothing to do")
            return 0

        # Enable vending (if it somehow isn't) and pin the validity to the MinIO floor.
        injected = f'"sts-enabled": True,\n        "sts-token-validity-seconds": {STS_VALIDITY_SECONDS}'
        if '"sts-enabled": True' in src:
            src = src.replace('"sts-enabled": True', injected, 1)
        elif '"sts-enabled": False' in src:
            src = src.replace('"sts-enabled": False', injected, 1)
        else:
            print("[lakekeeper-sts] could not find 'sts-enabled' in the storage-profile", file=sys.stderr)
            return 1

        cell["source"] = src
        with open(nb_path, "w") as f:
            json.dump(nb, f, indent=1, ensure_ascii=False)
            f.write("\n")
        print(f"[lakekeeper-sts] set sts-token-validity-seconds={STS_VALIDITY_SECONDS} in {nb_path}")
        return 0

    print("[lakekeeper-sts] warehouse-creation cell not found", file=sys.stderr)
    return 1


if __name__ == "__main__":
    nb = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_NOTEBOOK
    sys.exit(patch_notebook(nb))
