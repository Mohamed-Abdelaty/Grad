import yaml
import time
import json
from app.reliability.experiment_models import ExperimentModel
from app.reliability.experiment_runner import ExperimentRunner
from app.database.session import SyncSessionLocal

exp_dict = {
    'experiment': {
        'name': 'real-evidence-run-kafka-stop'
    },
    'target': {
        'service': 'kafka'
    },
    'fault': {
        'type': 'container_stop',
        'duration': 20
    },
    'validation': {
        'expected_data_loss': 0,
        'max_recovery_time': 120.0
    },
    'repetitions': 2
}

pipe_yaml = """pipeline:
  name: sales-evidence-batch
  version: "1.0"
source:
  type: csv
  path: /app/sample-data/sales_300k.csv
schema:
  - { name: transaction_id, type: string }
  - { name: customer_id, type: string }
  - { name: product_id, type: string }
  - { name: amount, type: double }
  - { name: timestamp, type: timestamp }
processing:
  operations:
    - { type: remove_nulls }
    - { type: remove_duplicates }
    - { type: filter, condition: "amount > 0" }
streaming:
  enabled: false
messaging:
  broker: kafka
  topic: sales-evidence-batch
processing_engine:
  type: spark
storage:
  type: hdfs
  path: /data/sales_evidence_batch
monitoring:
  enabled: true
"""

print('Validating ExperimentModel...')
model = ExperimentModel(**exp_dict)
print('Running experiment in DB session...')
t0 = time.time()
with SyncSessionLocal() as db:
    runner = ExperimentRunner(db)
    report = runner.run_experiment(model, associated_pipeline_yaml=pipe_yaml)
    print('Finished in', round(time.time() - t0, 2), 's')
    print('Parent Report ID:', report.get('report_id') or report.get('id'))
    print('Parent Run ID:', report.get('run_id'))
    print('Parent Result:', report.get('result'))
    print('Comparison Table:\n', json.dumps(report.get('comparison_table'), indent=2))
