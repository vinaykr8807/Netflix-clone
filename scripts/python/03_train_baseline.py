# Outputs scripts/output/movie_scores.csv with movie_id,score.

import os, csv

STATS = "scripts/output/movie_stats.csv"
OUTPUT = "scripts/output/movie_scores.csv"

# Hyperparameters for Bayesian average
M = 10            # prior weight (number of "virtual" votes)
C = 3.5           # prior mean (MovieLens-ish)

def main():
    if not os.path.exists(STATS):
        raise FileNotFoundError(f"{STATS} not found. Run 02_movie_stats.py first.")

    scores = []
    with open(STATS, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            mid = row["movie_id"]
            c = float(row["count"])
            avg = float(row["avg"])
            score = (c * avg + M * C) / (c + M)
            scores.append((mid, score))

    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["movie_id", "score"])
        for mid, s in scores:
            w.writerow([mid, f"{s:.6f}"])

    print(f"[v0] Wrote {len(scores)} scores -> {OUTPUT}")

if __name__ == "__main__":
    main()
