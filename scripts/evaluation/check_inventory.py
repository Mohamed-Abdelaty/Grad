"""Artifact inventory checker for Phase 0."""
import json
from pathlib import Path

def get_inventory():
    runs = []
    # Control runs (5)
    for i in range(1, 6):
        runs.append(('control', 'control', f'eval-control-r{i}', Path('docs/evaluation/raw/control') / f'eval-control-r{i}'))

    # Kafka stop: pilot-v3 r1, r2 + campaign r3, r4, r5
    runs.append(('fault', 'kafka-stop', 'eval-kafka-stop-r1', Path('docs/evaluation/raw/pilot-v3/eval-kafka-stop-r1')))
    runs.append(('fault', 'kafka-stop', 'eval-kafka-stop-r2', Path('docs/evaluation/raw/pilot-v3/eval-kafka-stop-r2')))
    for i in range(3, 6):
        runs.append(('fault', 'kafka-stop', f'eval-kafka-stop-r{i}', Path('docs/evaluation/raw/campaign/kafka-stop') / f'eval-kafka-stop-r{i}'))

    # Other 4 fault exps (5 each = 20)
    for exp in ['kafka-restart', 'spark-worker-stop', 'spark-worker-restart', 'datanode-stop']:
        for i in range(1, 6):
            runs.append(('fault', exp, f'eval-{exp}-r{i}', Path('docs/evaluation/raw/campaign') / exp / f'eval-{exp}-r{i}'))

    rows = []
    for group, exp, name, p in runs:
        rep = (p / 'report.json').exists()
        env = (p / 'environment.json').exists()
        phys = (p / 'physical_metrics.json').exists()
        child = (p / 'children.json').exists()
        
        # Check files inside dir
        files = [f.name for f in p.iterdir()] if p.exists() else []
        has_events = any('event' in f.lower() for f in files)
        has_stderr = any('stderr' in f.lower() or 'log' in f.lower() for f in files)
        has_docker = any('docker' in f.lower() for f in files)
        
        # Check if contents of environment.json or report.json contain embedded items
        embedded_docker = False
        embedded_offsets = False
        embedded_hdfs = False
        if env:
            try:
                env_data = json.loads((p / 'environment.json').read_text())
                embedded_docker = 'docker_stats_start' in env_data
                if 'physical_metrics' in env_data:
                    embedded_hdfs = 'file_count' in env_data['physical_metrics']
            except Exception:
                pass
        if rep:
            try:
                rep_data = json.loads((p / 'report.json').read_text())
                embedded_offsets = 'kafka_offsets' in rep_data or 'telemetry' in rep_data
            except Exception:
                pass

        rows.append({
            'run': name,
            'exp': exp,
            'group': group,
            'report_json': rep,
            'environment_json': env,
            'physical_metrics_json': phys,
            'children_json': child,
            'pipeline_run_events_dump': has_events,
            'driver_stderr_dump': has_stderr,
            'docker_events_dump': has_docker,
            'embedded_docker_stats': embedded_docker,
            'embedded_hdfs_metrics': embedded_hdfs,
        })
    return rows

if __name__ == '__main__':
    rows = get_inventory()
    header = f"{'Run':<33} | {'report':<6} | {'env':<6} | {'phys':<6} | {'child':<6} | {'events_file':<11} | {'stderr_file':<11} | {'docker_file':<11}"
    print(header)
    print('-' * len(header))
    for r in rows:
        print(f"{r['run']:<33} | {str(r['report_json']):<6} | {str(r['environment_json']):<6} | {str(r['physical_metrics_json']):<6} | {str(r['children_json']):<6} | {str(r['pipeline_run_events_dump']):<11} | {str(r['driver_stderr_dump']):<11} | {str(r['docker_events_dump']):<11}")
