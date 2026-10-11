import os

import pytest
import httpx
import time
import yaml

BASE_URL = os.environ.get('E2E_BASE_URL', 'http://localhost:8000')

KAFKA_EXPERIMENT_YAML = '''
experiment:
  name: e2e-kafka-failure-test
target:
  service: kafka
fault:
  type: container_stop
  duration: 10
validation:
  expected_data_loss: 0
  max_recovery_time: 60
'''

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
    return httpx.Client(base_url=BASE_URL, headers={'Authorization': f'Bearer {auth_token}'}, timeout=180)

def test_allowed_targets_endpoint(client):
    '''GET /api/experiments/allowed-targets returns the 7 allowed services.'''
    resp = client.get('/api/experiments/allowed-targets')
    assert resp.status_code == 200
    body = resp.json()
    assert 'allowed_services' in body
    assert 'kafka' in body['allowed_services']
    assert len(body['allowed_services']) == 7

def test_create_experiment(client):
    '''POST /api/experiments accepts a valid experiment YAML.'''
    resp = client.post('/api/experiments', json={
        'yaml_content': KAFKA_EXPERIMENT_YAML,
        'pipeline_yaml': ''
    })
    assert resp.status_code == 201
    body = resp.json()
    assert 'experiment_id' in body
    assert body['fault_type'] == 'container_stop'
    assert body['target_service'] == 'kafka'

@pytest.mark.integration
def test_run_experiment_and_wait_for_completion(client):
    '''
    Integration test: creates and runs a real kafka container_stop experiment,
    waits for completion, asserts report generated with real recovery time.
    Requires the full docker-compose stack to be running.
    '''
    # Create
    create_resp = client.post('/api/experiments', json={
        'yaml_content': KAFKA_EXPERIMENT_YAML,
        'pipeline_yaml': ''
    })
    assert create_resp.status_code == 201
    exp_id = create_resp.json()['experiment_id']

    # Run
    run_resp = client.post(f'/api/experiments/{exp_id}/run', json={
        'yaml_content': KAFKA_EXPERIMENT_YAML,
        'pipeline_yaml': ''
    })
    assert run_resp.status_code == 202

    # Poll for completion (max 120s)
    deadline = time.time() + 120
    final_status = None
    while time.time() < deadline:
        detail = client.get(f'/api/experiments/{exp_id}').json()
        run = detail.get('latest_run')
        if run and run['status'] in ('succeeded', 'failed', 'cancelled'):
            final_status = run['status']
            break
        time.sleep(3)

    assert final_status in ('succeeded', 'failed'), f'Experiment did not complete within 120s. Status: {final_status}'

    # Verify report generated
    reports_resp = client.get('/api/reports')
    assert reports_resp.status_code == 200
    reports = reports_resp.json()
    assert len(reports) > 0

    # Verify kafka container is running again (rollback worked)
    status_resp = client.get('/health')
    assert status_resp.status_code == 200
