from pathlib import Path
import json

lines = Path("pw_result.json").read_text(encoding="utf-16", errors="ignore").splitlines()

# Find the line that has "{" before "config"
start_line = None
for i in range(len(lines)):
    if '"config":' in lines[i]:
        # line before it is "{"
        start_line = i - 1
        break

print(f"JSON starts at line {start_line+1}: {lines[start_line]}")
json_str = "\n".join(lines[start_line:])
data = json.loads(json_str)

print("Config loaded successfully!")
print(f"Stats: {data.get('stats')}")

failed = []
def walk_suites(suite):
    for spec in suite.get('specs', []):
        for test in spec.get('tests', []):
            for res in test.get('results', []):
                if res.get('status') in ('failed', 'timedOut'):
                    err = res.get('error', {}).get('message', '') or ''
                    failed.append((spec.get('file'), spec.get('title'), err, res.get('status')))
    for s in suite.get('suites', []):
        walk_suites(s)

walk_suites(data)
print(f"Total failing tests found: {len(failed)}")
for i, (f, t, e, st) in enumerate(failed, 1):
    print(f"\n--- Failure {i} [{st}] ---")
    print(f"File: {f}")
    print(f"Test: {t}")
    print("Error (first 6 lines):")
    lines = [line.strip() for line in e.splitlines() if line.strip()][:6]
    for l in lines:
        print(f"  {l}")
