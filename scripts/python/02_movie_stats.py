import os, csv, io, math
from collections import defaultdict

INPUT = "scripts/output/interaction_log_processed.csv"
OUTPUT = "scripts/output/movie_stats.csv"

def main():
    if not os.path.exists(INPUT):
        raise FileNotFoundError(f"{INPUT} not found. Run 01_ingest_supabase.py first.")

    counts = defaultdict(int)
    sums = defaultdict(float)

    with open(INPUT, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                rating = float(row["rating"])
            except:
                continue
            mid = row["movie_id"]
            counts[mid] += 1
            sums[mid] += rating

    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["movie_id", "count", "avg"])
        for mid, c in counts.items():
            avg = (sums[mid] / c) if c else 0.0
            w.writerow([mid, c, f"{avg:.6f}"])

    print(f"[v0] Wrote stats for {len(counts)} movies -> {OUTPUT}")

if __name__ == "__main__":
    main()
