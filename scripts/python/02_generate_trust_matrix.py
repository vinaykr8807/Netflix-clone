"""
Generate a synthetic trust matrix based on users in processed interactions.
- Values in [0, 1], threshold < 0.7 -> 0 for sparsity
- Row-normalize to sum to 1 (avoid div by zero)
Outputs: scripts/output/trust_matrix.npy
"""

import os
import sys
import numpy as np
import pandas as pd

OUT_DIR = os.path.join("scripts","output")
os.makedirs(OUT_DIR, exist_ok=True)
INTERACTIONS_CSV = os.path.join(OUT_DIR, "interaction_log_processed.csv")

def main():
    try:
        df = pd.read_csv(INTERACTIONS_CSV, parse_dates=["ts"])
    except Exception as e:
        print("[v0] Failed to load interactions CSV:", e)
        sys.exit(1)

    users = df["user_id"].dropna().unique()
    users = users.astype(int)
    n = len(users)
    print(f"[v0] Unique users: {n}")
    if n == 0:
        print("[v0] No users found.")
        sys.exit(1)

    rng = np.random.default_rng(42)
    mat = rng.random((n, n), dtype=np.float32)

    # Sparsify
    mat[mat < 0.7] = 0.0
    # Remove self-trust to avoid trivial bias, then re-add small diag to keep row-sum > 0
    np.fill_diagonal(mat, 0.0)

    # Row-normalize
    row_sums = mat.sum(axis=1, keepdims=True)
    # If a row is all zeros, make it uniform
    zero_rows = (row_sums == 0).flatten()
    if zero_rows.any():
        mat[zero_rows, :] = 1.0 / n
        row_sums = mat.sum(axis=1, keepdims=True)
    mat = mat / row_sums

    np.save(os.path.join(OUT_DIR, "trust_matrix.npy"), mat)
    np.save(os.path.join(OUT_DIR, "trust_users.npy"), users)  # keep mapping order
    print("[v0] Saved trust_matrix.npy and trust_users.npy")

if __name__ == "__main__":
    main()
