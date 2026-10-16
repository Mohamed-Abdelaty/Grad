"""
tests/pipeline/test_validator.py
---------------------------------
Unit tests for :class:`~app.pipeline.validator.PipelineBusinessValidator`.

Each test class covers exactly one business rule in isolation.  Tests
construct minimal ``PipelineConfig`` objects using
:func:`PipelineConfig.model_validate` with a plain dict so they are
independent of the YAML parser.

No Docker, no database, and no network connections are required.

Rules under test
----------------
1. Streaming enabled without messaging → error on field "messaging"
2. Aggregate references non-existent column → error on field path
3. API source without url → error on field "source.url"
4. Relative storage path → error on field "storage.path"
5. Duplicate schema column names → error on field "schema"
6. Fully valid config → empty error list
"""

from __future__ import annotations

import pytest

from app.pipeline.config_models import PipelineConfig
from app.pipeline.validator import PipelineBusinessValidator

# ── Helper ─────────────────────────────────────────────────────────────────────


def make_config(overrides: dict | None = None) -> PipelineConfig:
    """
    Build a minimal, fully-valid :class:`PipelineConfig` from a base dict,
    then apply *overrides* (shallow merge at the top level).

    Using ``model_validate`` with a plain dict lets tests exercise the
    validator independently of the YAML parser.
    """
    base: dict = {
        "pipeline": {"name": "test-pipeline", "version": "1.0"},
        "source": {"type": "csv", "path": "/data/input.csv"},
        "schema": [
            {"name": "id", "type": "string"},
            {"name": "value", "type": "double"},
            {"name": "category", "type": "string"},
        ],
        "processing": {
            "operations": [{"type": "remove_nulls"}],
        },
        "streaming": {"enabled": False},
        "processing_engine": {"type": "spark"},
        "storage": {"type": "local", "path": "/data/output"},
        "monitoring": {"enabled": True},
    }
    if overrides:
        base.update(overrides)
    return PipelineConfig.model_validate(base)


@pytest.fixture(scope="module")
def validator() -> PipelineBusinessValidator:
    """Shared validator instance for all tests in this module."""
    return PipelineBusinessValidator()


# ── Rule 0: baseline ───────────────────────────────────────────────────────────


class TestValidConfigReturnsNoErrors:
    """A well-formed config must produce an empty error list."""

    def test_fully_valid_config_has_no_errors(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config()
        errors = validator.validate(config)
        assert errors == [], f"Expected no errors, got: {errors}"

    def test_valid_config_with_streaming_and_messaging(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config(
            {
                "streaming": {"enabled": True},
                "messaging": {
                    "broker": "kafka",
                    "topic": "test-events",
                    "partitions": 2,
                    "replication_factor": 1,
                },
            }
        )
        errors = validator.validate(config)
        assert errors == []

    def test_valid_config_with_api_source_and_url(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config(
            {
                "source": {
                    "type": "api",
                    "url": "https://api.example.com/data",
                    "method": "GET",
                },
            }
        )
        errors = validator.validate(config)
        assert errors == []

    def test_valid_aggregate_referencing_schema_columns(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config(
            {
                "processing": {
                    "operations": [
                        {
                            "type": "aggregate",
                            "group_by": "category",
                            "operation": "sum",
                            "field": "value",
                        }
                    ]
                }
            }
        )
        errors = validator.validate(config)
        assert errors == []


# ── Rule 1: streaming requires messaging ──────────────────────────────────────


class TestStreamingRequiresMessaging:
    """streaming.enabled=True without messaging must produce exactly one error."""

    def test_streaming_enabled_without_messaging_produces_error(
        self, validator: PipelineBusinessValidator
    ) -> None:
        # messaging is Optional[MessagingConfig] with default None
        config = make_config({"streaming": {"enabled": True}})
        assert config.messaging is None  # confirm precondition
        errors = validator.validate(config)
        assert len(errors) >= 1

    def test_streaming_error_field_is_messaging(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config({"streaming": {"enabled": True}})
        errors = validator.validate(config)
        fields = [e["field"] for e in errors]
        assert "messaging" in fields, (
            f"Expected 'messaging' in error fields, got: {fields}"
        )

    def test_streaming_error_message_is_informative(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config({"streaming": {"enabled": True}})
        errors = validator.validate(config)
        messaging_errors = [e for e in errors if e["field"] == "messaging"]
        assert len(messaging_errors) == 1
        msg = messaging_errors[0]["message"].lower()
        assert "streaming" in msg or "broker" in msg or "messaging" in msg

    def test_streaming_disabled_without_messaging_is_valid(
        self, validator: PipelineBusinessValidator
    ) -> None:
        """Batch pipelines don't need a message broker."""
        config = make_config({"streaming": {"enabled": False}})
        errors = validator.validate(config)
        streaming_errors = [e for e in errors if e["field"] == "messaging"]
        assert streaming_errors == []


# ── Rule 2: aggregate column cross-reference ──────────────────────────────────


class TestAggregateColumnReferences:
    """Aggregate operations must reference columns declared in schema."""

    def test_nonexistent_group_by_column_produces_error(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config(
            {
                "processing": {
                    "operations": [
                        {
                            "type": "aggregate",
                            "group_by": "nonexistent_column",
                            "operation": "sum",
                            "field": "value",
                        }
                    ]
                }
            }
        )
        errors = validator.validate(config)
        assert len(errors) >= 1

    def test_nonexistent_group_by_error_field_contains_group_by(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config(
            {
                "processing": {
                    "operations": [
                        {
                            "type": "aggregate",
                            "group_by": "ghost_column",
                            "operation": "count",
                            "field": "value",
                        }
                    ]
                }
            }
        )
        errors = validator.validate(config)
        fields = [e["field"] for e in errors]
        assert any("group_by" in f for f in fields), (
            f"Expected a 'group_by' field in error paths, got: {fields}"
        )

    def test_nonexistent_aggregate_field_produces_error(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config(
            {
                "processing": {
                    "operations": [
                        {
                            "type": "aggregate",
                            "group_by": "category",
                            "operation": "avg",
                            "field": "missing_column",
                        }
                    ]
                }
            }
        )
        errors = validator.validate(config)
        fields = [e["field"] for e in errors]
        assert any("field" in f for f in fields), (
            f"Expected an 'field' reference in error paths, got: {fields}"
        )

    def test_nonexistent_column_name_appears_in_message(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config(
            {
                "processing": {
                    "operations": [
                        {
                            "type": "aggregate",
                            "group_by": "bad_group",
                            "operation": "min",
                            "field": "value",
                        }
                    ]
                }
            }
        )
        errors = validator.validate(config)
        messages = " ".join(e["message"] for e in errors)
        assert "bad_group" in messages

    def test_both_group_by_and_field_missing_produces_two_errors(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config(
            {
                "processing": {
                    "operations": [
                        {
                            "type": "aggregate",
                            "group_by": "col_a",
                            "operation": "max",
                            "field": "col_b",
                        }
                    ]
                }
            }
        )
        errors = validator.validate(config)
        # Both col_a and col_b are missing from schema → 2 aggregate errors
        assert len(errors) >= 2

    def test_valid_aggregate_with_existing_columns_passes(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config(
            {
                "processing": {
                    "operations": [
                        {
                            "type": "aggregate",
                            "group_by": "id",        # exists in schema
                            "operation": "sum",
                            "field": "value",         # exists in schema
                        }
                    ]
                }
            }
        )
        errors = validator.validate(config)
        agg_errors = [
            e for e in errors
            if "group_by" in e["field"] or ("field" in e["field"] and "processing" in e["field"])
        ]
        assert agg_errors == []


# ── Rule 3: API source requires url ──────────────────────────────────────────


class TestApiSourceRequiresUrl:
    """source.type='api' without source.url must produce an error."""

    def test_api_source_without_url_produces_error(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config({"source": {"type": "api"}})
        errors = validator.validate(config)
        assert len(errors) >= 1

    def test_api_source_error_field_is_source_url(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config({"source": {"type": "api"}})
        errors = validator.validate(config)
        fields = [e["field"] for e in errors]
        assert "source.url" in fields, (
            f"Expected 'source.url' in error fields, got: {fields}"
        )

    def test_api_source_error_message_mentions_url(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config({"source": {"type": "api"}})
        errors = validator.validate(config)
        url_errors = [e for e in errors if e["field"] == "source.url"]
        assert len(url_errors) == 1
        assert "url" in url_errors[0]["message"].lower()

    def test_api_source_with_url_passes(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config(
            {"source": {"type": "api", "url": "https://api.example.com/v1/data"}}
        )
        errors = validator.validate(config)
        url_errors = [e for e in errors if e["field"] == "source.url"]
        assert url_errors == []

    def test_csv_source_without_url_passes(
        self, validator: PipelineBusinessValidator
    ) -> None:
        """CSV sources don't need a url — rule must not apply."""
        config = make_config({"source": {"type": "csv", "path": "/data/in.csv"}})
        errors = validator.validate(config)
        url_errors = [e for e in errors if e["field"] == "source.url"]
        assert url_errors == []

    def test_json_source_without_url_passes(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config({"source": {"type": "json", "path": "/data/in.json"}})
        errors = validator.validate(config)
        url_errors = [e for e in errors if e["field"] == "source.url"]
        assert url_errors == []


# ── Rule 4: storage path must be absolute ─────────────────────────────────────


class TestStoragePathMustBeAbsolute:
    """storage.path must start with '/' — relative paths must be rejected."""

    def test_relative_path_produces_error(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config({"storage": {"type": "local", "path": "relative/path"}})
        errors = validator.validate(config)
        assert len(errors) >= 1

    def test_relative_path_error_field_is_storage_path(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config({"storage": {"type": "local", "path": "data/output"}})
        errors = validator.validate(config)
        fields = [e["field"] for e in errors]
        assert "storage.path" in fields, (
            f"Expected 'storage.path' in error fields, got: {fields}"
        )

    def test_relative_path_error_message_mentions_absolute(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config({"storage": {"type": "hdfs", "path": "hdfs/user/data"}})
        errors = validator.validate(config)
        path_errors = [e for e in errors if e["field"] == "storage.path"]
        assert len(path_errors) == 1
        msg = path_errors[0]["message"].lower()
        assert "absolute" in msg or "required" in msg or "/" in msg

    def test_dot_slash_relative_path_produces_error(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config({"storage": {"type": "local", "path": "./output"}})
        errors = validator.validate(config)
        path_errors = [e for e in errors if e["field"] == "storage.path"]
        assert len(path_errors) == 1

    def test_absolute_path_passes(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config({"storage": {"type": "local", "path": "/absolute/path"}})
        errors = validator.validate(config)
        path_errors = [e for e in errors if e["field"] == "storage.path"]
        assert path_errors == []

    def test_root_path_passes(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config({"storage": {"type": "local", "path": "/"}})
        errors = validator.validate(config)
        path_errors = [e for e in errors if e["field"] == "storage.path"]
        assert path_errors == []


# ── Rule 5: no duplicate schema column names ──────────────────────────────────


class TestNoDuplicateSchemaColumns:
    """Each column name must appear exactly once in the schema list."""

    def test_duplicate_column_name_produces_error(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config(
            {
                "schema": [
                    {"name": "id", "type": "string"},
                    {"name": "id", "type": "integer"},  # duplicate!
                    {"name": "value", "type": "double"},
                ]
            }
        )
        errors = validator.validate(config)
        assert len(errors) >= 1

    def test_duplicate_error_field_is_schema(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config(
            {
                "schema": [
                    {"name": "amount", "type": "double"},
                    {"name": "amount", "type": "double"},
                ]
            }
        )
        errors = validator.validate(config)
        fields = [e["field"] for e in errors]
        assert "schema" in fields, (
            f"Expected 'schema' in error fields, got: {fields}"
        )

    def test_duplicate_column_name_appears_in_message(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config(
            {
                "schema": [
                    {"name": "customer_id", "type": "string"},
                    {"name": "customer_id", "type": "string"},
                    {"name": "value", "type": "double"},
                ]
            }
        )
        errors = validator.validate(config)
        schema_errors = [e for e in errors if e["field"] == "schema"]
        assert len(schema_errors) == 1
        assert "customer_id" in schema_errors[0]["message"]

    def test_multiple_duplicates_reported_in_one_error(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config(
            {
                "schema": [
                    {"name": "a", "type": "string"},
                    {"name": "b", "type": "double"},
                    {"name": "a", "type": "string"},  # dup
                    {"name": "b", "type": "double"},  # dup
                ]
            }
        )
        errors = validator.validate(config)
        schema_errors = [e for e in errors if e["field"] == "schema"]
        # A single error entry should list both duplicates
        assert len(schema_errors) == 1
        msg = schema_errors[0]["message"]
        assert "a" in msg and "b" in msg


class TestStreamingValidatorRules:
    """Tests for streaming operations and watermark validation."""

    def test_streaming_aggregate_disallowed(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config(
            {
                "streaming": {"enabled": True},
                "messaging": {
                    "broker": "kafka",
                    "topic": "events",
                    "partitions": 3,
                },
                "processing": {
                    "operations": [
                        {
                            "type": "aggregate",
                            "group_by": "category",
                            "operation": "sum",
                            "field": "value",
                        }
                    ]
                },
            }
        )
        errors = validator.validate(config)
        assert any(
            err["field"] == "processing.operations.0"
            and "Aggregate operations are not supported in streaming mode yet" in err["message"]
            for err in errors
        )

    def test_streaming_remove_duplicates_without_watermark_disallowed(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config(
            {
                "streaming": {"enabled": True},
                "messaging": {
                    "broker": "kafka",
                    "topic": "events",
                },
                "processing": {
                    "operations": [{"type": "remove_duplicates"}],
                },
            }
        )
        errors = validator.validate(config)
        assert any(
            err["field"] == "processing.operations.0"
            and "remove_duplicates in streaming mode requires streaming.watermark" in err["message"]
            for err in errors
        )

    def test_streaming_watermark_column_not_in_schema(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config(
            {
                "streaming": {
                    "enabled": True,
                    "watermark": {"column": "event_time", "delay_seconds": 15},
                },
                "messaging": {
                    "broker": "kafka",
                    "topic": "events",
                },
                "processing": {
                    "operations": [{"type": "remove_duplicates"}],
                },
            }
        )
        errors = validator.validate(config)
        assert any(
            err["field"] == "streaming.watermark.column"
            and "Watermark column 'event_time' is not declared in the schema" in err["message"]
            for err in errors
        )

    def test_streaming_watermark_column_not_timestamp(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config(
            {
                "schema": [
                    {"name": "id", "type": "string"},
                    {"name": "created_str", "type": "string"},
                ],
                "streaming": {
                    "enabled": True,
                    "watermark": {"column": "created_str", "delay_seconds": 15},
                },
                "messaging": {
                    "broker": "kafka",
                    "topic": "events",
                },
                "processing": {
                    "operations": [{"type": "remove_duplicates"}],
                },
            }
        )
        errors = validator.validate(config)
        assert any(
            err["field"] == "streaming.watermark.column"
            and "must have type 'timestamp'" in err["message"]
            for err in errors
        )

    def test_streaming_valid_with_watermark_and_dedup(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config(
            {
                "schema": [
                    {"name": "id", "type": "string"},
                    {"name": "timestamp", "type": "timestamp"},
                ],
                "streaming": {
                    "enabled": True,
                    "trigger_interval_seconds": 5,
                    "idle_timeout_seconds": 20,
                    "max_runtime_seconds": 300,
                    "starting_offsets": "earliest",
                    "watermark": {"column": "timestamp", "delay_seconds": 10},
                    "max_offsets_per_trigger": 1000,
                },
                "messaging": {
                    "broker": "kafka",
                    "topic": "events",
                    "partitions": 3,
                },
                "processing": {
                    "operations": [{"type": "remove_duplicates"}],
                },
            }
        )
        errors = validator.validate(config)
        assert errors == []


    def test_unique_column_names_pass(
        self, validator: PipelineBusinessValidator
    ) -> None:
        config = make_config(
            {
                "schema": [
                    {"name": "col_a", "type": "string"},
                    {"name": "col_b", "type": "double"},
                    {"name": "col_c", "type": "integer"},
                ]
            }
        )
        errors = validator.validate(config)
        schema_errors = [e for e in errors if e["field"] == "schema"]
        assert schema_errors == []


# ── Multiple rules triggered simultaneously ───────────────────────────────────


class TestMultipleRulesCanFailTogether:
    """All rules run in one pass — multiple violations are reported together."""

    def test_streaming_and_relative_path_both_reported(
        self, validator: PipelineBusinessValidator
    ) -> None:
        """Both rule 1 (streaming) and rule 4 (path) must appear in one call."""
        config = make_config(
            {
                "streaming": {"enabled": True},
                # messaging intentionally missing → rule 1 violation
                "storage": {"type": "local", "path": "not/absolute"},
                # relative path → rule 4 violation
            }
        )
        errors = validator.validate(config)
        fields = [e["field"] for e in errors]
        assert "messaging" in fields
        assert "storage.path" in fields

    def test_all_rules_can_fail_simultaneously(
        self, validator: PipelineBusinessValidator
    ) -> None:
        """A maximally broken config triggers rules 1, 2, 3, 4, and 5."""
        config = make_config(
            {
                "source": {"type": "api"},                  # rule 3: no url
                "schema": [
                    {"name": "x", "type": "string"},
                    {"name": "x", "type": "double"},        # rule 5: duplicate
                ],
                "processing": {
                    "operations": [
                        {
                            "type": "aggregate",
                            "group_by": "missing_col",      # rule 2: bad group_by
                            "operation": "sum",
                            "field": "also_missing",        # rule 2: bad field
                        }
                    ]
                },
                "streaming": {"enabled": True},             # rule 1: no messaging
                "storage": {"type": "local", "path": "relative"},  # rule 4: relative
            }
        )
        errors = validator.validate(config)
        # At minimum: rule1 + rule2(×2) + rule3 + rule4 + rule5 = 6 errors
        assert len(errors) >= 5
