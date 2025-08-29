"""
Minimal training loop placeholder.
- Builds simple item popularity per user weighted by trust as a stand-in for a full Transformer (keeps dependencies light).
- Saves numpy arrays representing item scores per user for fast prediction.
Outputs: scripts/output/item_index.npy, scripts/output/user_item_scores.npy
"""

import os
import sys
import numpy as np
import pandas as pd
from collections import defaultdict

OUT_DIR = os.path.join("scripts","output")
os.makedirs(OUT_DIR, exist_ok=True)

INTERACTIONS_CSV = os.path.join(OUT_DIR, "interaction_log_processed.csv")
TRUST_NPY = os.path.join(OUT_DIR, "trust_matrix.npy")
TRUST_USERS_NPY = os.path.join(OUT_DIR, "trust_users.npy")

def main():
    try:
        df = pd.read_csv(INTERACTIONS_CSV, parse_dates=["ts"])
        trust = np.load(TRUST_NPY)
        trust_users = np.load(TRUST_USERS_NPY)
    except Exception as e:
        print("[v0] Error loading inputs:", e)
        sys.exit(1)

    # Map item ids to indices
    item_ids = df["movie_id"].astype(int).unique()
    item_ids.sort()
    item_index = {mid: i for i, mid in enumerate(item_ids)}
    np.save(os.path.join(OUT_DIR, "item_index.npy"), item_ids)

    # Build base user->item value sums
    by_user = defaultdict(lambda: np.zeros(len(item_ids), dtype=np.float32))
    for row in df.itertuples(index=False):
        u = int(row.user_id)
        i = item_index.get(int(row.movie_id))
        if i is not None:
            by_user[u][i] += float(row.value)

    # Align users to trust matrix order
    user_index = {int(u): idx for idx, u in enumerate(trust_users)}

    # For each user, aggregate neighbors' vectors with trust weights
    user_item_scores = np.zeros((len(trust_users), len(item_ids)), dtype=np.float32)
    for u, row_idx in user_index.items():
        # own vector
        base = by_user.get(u, np.zeros(len(item_ids), dtype=np.float32))
        # weighted neighbors
        weights = trust[row_idx]  # shape (n_users,)
        agg = np.zeros_like(base)
        for v, w in enumerate(weights):
            if w <= 0: 
                continue
            vu = int(trust_users[v])
            agg += w * by_user.get(vu, np.zeros(len(item_ids), dtype=np.float32))
        user_item_scores[row_idx] = base + agg

    np.save(os.path.join(OUT_DIR, "user_item_scores.npy"), user_item_scores)
    print("[v0] Saved user_item_scores.npy and item_index.npy")

if __name__ == "__main__":
    main()
