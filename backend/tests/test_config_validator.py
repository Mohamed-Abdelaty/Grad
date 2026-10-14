"""
tests/test_config_validator.py
-------------------------------
Tests for the Settings credential validator in app.core.config.

These tests isolate the validation logic without importing the full application
stack, so they have no dependency on confluent_kafka or asyncpg.
"""
import os
import pytest
from pydantic import ValidationError


def _make_settings(**overrides):
    """Create a fresh Settings instance with controlled env vars."""
    # Import here to avoid any module-level side-effects at collection time.
    from app.core.config import Settings  # noqa: PLC0415
    return Settings(**overrides)


class TestEmptyCredentials:
    def test_empty_password_allowed_in_dev(self, monkeypatch):
        monkeypatch.setenv('APP_ENV', 'development')
        s = _make_settings(ADMIN_USERNAME='admin', ADMIN_PASSWORD='')
        assert s.ADMIN_PASSWORD == ''

    def test_empty_password_rejected_in_production(self, monkeypatch):
        monkeypatch.setenv('APP_ENV', 'production')
        with pytest.raises(ValidationError, match='ADMIN_PASSWORD'):
            _make_settings(ADMIN_USERNAME='admin', ADMIN_PASSWORD='')

    def test_empty_username_rejected_in_production(self, monkeypatch):
        monkeypatch.setenv('APP_ENV', 'production')
        with pytest.raises(ValidationError, match='ADMIN_USERNAME'):
            _make_settings(ADMIN_USERNAME='', ADMIN_PASSWORD='strongpass123')

    def test_valid_credentials_accepted_in_production(self, monkeypatch):
        monkeypatch.setenv('APP_ENV', 'production')
        s = _make_settings(ADMIN_USERNAME='admin', ADMIN_PASSWORD='super-secure-99!')
        assert s.ADMIN_USERNAME == 'admin'


class TestPlaceholderPasswords:
    """Placeholder passwords must be rejected in every environment."""

    @pytest.mark.parametrize('placeholder', [
        'changeme',
        'changeme-dev',
        'change-me',
        'change-me-before-running',
        'admin123',
        'admin',
        'password',
        'secret',
        'letmein',
        'change_me',
        'CHANGE_ME',
        'CHANGEME',
    ])
    def test_placeholder_rejected_in_development(self, monkeypatch, placeholder):
        monkeypatch.setenv('APP_ENV', 'development')
        with pytest.raises(ValidationError, match='known placeholder'):
            _make_settings(ADMIN_USERNAME='admin', ADMIN_PASSWORD=placeholder)

    @pytest.mark.parametrize('placeholder', [
        'changeme-dev',
        'change-me-before-running',
    ])
    def test_placeholder_rejected_in_production(self, monkeypatch, placeholder):
        monkeypatch.setenv('APP_ENV', 'production')
        with pytest.raises(ValidationError, match='known placeholder'):
            _make_settings(ADMIN_USERNAME='admin', ADMIN_PASSWORD=placeholder)

    def test_strong_password_accepted_in_development(self, monkeypatch):
        monkeypatch.setenv('APP_ENV', 'development')
        s = _make_settings(ADMIN_USERNAME='admin', ADMIN_PASSWORD='custom-test-secret-123')
        assert s.ADMIN_PASSWORD == 'custom-test-secret-123'

    def test_placeholder_username_is_not_blocked(self, monkeypatch):
        """Only the password field has placeholder blocking; username does not."""
        monkeypatch.setenv('APP_ENV', 'development')
        # Should NOT raise — 'admin' is a placeholder password but a reasonable username.
        s = _make_settings(ADMIN_USERNAME='admin', ADMIN_PASSWORD='custom-test-secret-123')
        assert s.ADMIN_USERNAME == 'admin'
