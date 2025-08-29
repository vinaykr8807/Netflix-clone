# Uses only standard libs + numpy if available. Reads a CSV path (local) and prints a mock training summary.

import csv
import os
from collections import defaultdict
from typing import Dict, List, Tuple

try:
    import numpy as np
except Exception:
    np = None

def read_interactions(csv_path: str) -> Dict[str, List[Tuple[str, float]]]:
    sessions: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
    with open(csv_path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            uid = row.get("user_id") or ""
            iid = row.get("item_id") or ""
            val = float(row.get("value") or 1.0)
            sessions[uid].append((iid, val))
    return sessions

def train_simple_popularity(sessions: Dict[str, List[Tuple[str, float]]]):
    pop: Dict[str, float] = defaultdict(float)
    for _, items in sessions.items():
        for iid, v in items:
            pop[iid] += v
    # normalize
    if np:
        vals = np.array(list(pop.values()))
        denom = vals.max() if vals.size > 0 else 1.0
        for k in pop:
            pop[k] = (pop[k] / denom) if denom else 0.0
    return pop

def main():
    csv_path = os.environ.get("INTERACTIONS_CSV", "interactions.csv")
    if not os.path.exists(csv_path):
        print("[v0] CSV not found:", csv_path)
        return
    sessions = read_interactions(csv_path)
    print("[v0] Users:", len(sessions))
    pop = train_simple_popularity(sessions)
    top = sorted(pop.items(), key=lambda x: x[1], reverse=True)[:10]
    print("[v0] Top-10 items (popularity):")
    for iid, score in top:
        print(iid, round(score, 4))

if __name__ == "__main__":
    main()
