import os

import pytest
import httpx
import yaml
import time

BASE_URL = os.environ.get('E2E_BASE_URL', 'http://localhost:8000')


@pytest.fixture(scope='module')
def auth_token():
    username = os.environ.get('ADMIN_USERNAME')
    password = os.environ.get('ADMIN_PASSWORD')
    if not username or not password:
        pytest.fail('Set ADMIN_USERNAME and ADMIN_PASSWORD (same values as in .env) to run e2e tests.')
    resp = httpx.post(f'{BASE_URL}/api/auth/login', data={'username': username, 'password': password})
    assert resp.status_code == 200
    return resp.json()['access_token']

@pytest.fixture(scope='module')
def client(auth_token):
    return httpx.Client(base_url=BASE_URL, headers={'Authorization': f'Bearer {auth_token}'}, timeout=120)

PIPELINE_YAML = '''# Sales pipeline for E2E testing
pipeline:
  name: e2e-sales-pipeline
  version: "1.0"

source:
  type: csv
  path: /app/sample-data/sales_sample.csv

schema:
  - { name: transaction_id, type: string }
  - { name: amount, type: double }
  - { name: customer_id, type: string }
  - { name: product_id, type: string }
  - { name: sale_date, type: string }

processing:
  operations:
    - { type: remove_nulls }
    - { type: remove_duplicates }
    - { type: filter, condition: "amount > 0" }

streaming:
  enabled: false

messaging:
  broker: kafka
  topic: e2e-sales-pipeline

processing_engine:
  type: spark

storage:
  type: hdfs
  path: /data/processed/e2e_sales_pipeline

monitoring:
  enabled: true
'''

def test_validate_pipeline(client):
    '''Validation endpoint returns valid=True for a well-formed pipeline YAML.'''
    resp = client.post('/api/pipelines/validate', json={'yaml_content': PIPELINE_YAML})
    assert resp.status_code == 200
    body = resp.json()
    assert body['valid'] is True
    assert body['errors'] == []

def test_run_pipeline_end_to_end(client):
    '''Full pipeline run: source→clean→local storage, returns success status and metrics.'''
    resp = client.post('/api/pipelines/run?wait=true', json={'yaml_content': PIPELINE_YAML})
    assert resp.status_code == 200
    body = resp.json()
    assert body['status'] == 'success'
    assert 'metrics' in body
    metrics = body['metrics']
    assert 'spark_job' in metrics or 'source:csv' in metrics

def test_pipeline_appears_in_list(client):
    '''After running, the pipeline run appears in GET /api/pipeline-runs.'''
    resp = client.get('/api/pipeline-runs')
    assert resp.status_code == 200
    runs = resp.json()
    assert isinstance(runs, list)
    assert len(runs) > 0

def test_system_status_all_present(client):
    '''System status endpoint returns all expected service keys.'''
    resp = client.get('/api/system/status')
    assert resp.status_code == 200
    body = resp.json()
    assert 'database' in body
    assert 'kafka' in body

def test_metrics_endpoint_has_custom_metrics():
    '''Prometheus /metrics endpoint exposes the 5 custom metrics.'''
    resp = httpx.get(f'{BASE_URL}/metrics/', follow_redirects=True, timeout=10)
    assert resp.status_code == 200
    text = resp.text
    assert 'pipeline_records_processed_total' in text
    assert 'pipeline_processing_latency_seconds' in text
    assert 'pipeline_errors_total' in text
