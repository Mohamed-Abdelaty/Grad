import json

results_file = 'D:/Configuration-Driven Resilient Big Data Pipeline Platform/resilient-big-data-platform/docs/evaluation/results/batch_1m_results.json'
with open(results_file, 'r', encoding='utf-8') as f:
    results = json.load(f)

print("=== RECOMPUTED THROUGHPUT (Formula: rows_in / duration_seconds) ===")
print(f"{'Shape':<20} {'Input Rows':>12} {'Output Rows':>12} {'Duration (s)':>14} {'Throughput (rows_in/s)':>24}")
print("-" * 86)

for shape, data in results.items():
    rows_in = data.get('input_rows', 1000000)
    rows_out = data.get('output_rows', 0)
    dur = data.get('duration_seconds', 0.0)
    tput = round(rows_in / dur, 2) if dur > 0 else 0.0
    data['throughput_rows_in_per_sec'] = tput
    print(f"{shape:<20} {rows_in:>12,} {rows_out:>12,} {dur:>14.2f} {tput:>24.2f}")

with open(results_file, 'w', encoding='utf-8') as f:
    json.dump(results, f, indent=2)
