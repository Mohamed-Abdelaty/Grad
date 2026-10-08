"""
alembic/env.py
--------------
Alembic migration environment.

Configured for *async* SQLAlchemy via asyncpg.  The DATABASE_URL is read
from the application's Settings object (never hard-coded).  All ORM models
are imported so auto-generate can detect schema changes automatically.

Migration version column
------------------------
The project uses human-readable revision IDs such as
``0002_persist_experiment_definitions`` (35 chars).  Alembic's default
``alembic_version`` table defines ``version_num`` as ``VARCHAR(32)``, which
is too short.  ``version_num_type`` is **not** a recognised
``context.configure()`` option — it is silently ignored.

The fix is ``_ensure_version_column_is_text()``, called inside
``do_run_migrations`` immediately before any migration runs.  It:

* On a **clean database**: does nothing — Alembic will ``CREATE TABLE``
  ``alembic_version`` with a ``VARCHAR(32)`` column for the first migration,
  then we widen it to ``TEXT`` before Alembic writes the revision string.
  Actually we widen *before* Alembic's ``CREATE TABLE`` by pre-creating the
  table with a ``TEXT`` column.
* On an **existing database with VARCHAR(32)**: runs
  ``ALTER TABLE alembic_version ALTER COLUMN version_num TYPE TEXT``
  idempotently (safe to call multiple times).
* On a database that already has ``TEXT``: the ``ALTER`` is a no-op in
  PostgreSQL (no error, no downtime).

No manual SQL is ever required.
"""

import asyncio
from logging.config import fileConfig

import sqlalchemy as sa
from alembic import context
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import create_async_engine

# ── Load application settings ──────────────────────────────────────────────────
from app.core.config import get_settings

# ── Import *every* model so Alembic can auto-detect tables ────────────────────
import app.models  # noqa: F401  — registers all models with Base.metadata
from app.database.base import Base

# ── Alembic Config object ──────────────────────────────────────────────────────
config = context.config

# Set up Python logging from alembic.ini [loggers] section (if present)
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Tell Alembic which metadata to compare against for auto-generate
target_metadata = Base.metadata


# ── Helpers ────────────────────────────────────────────────────────────────────

def get_url() -> str:
    """Return the database URL from application settings."""
    return get_settings().DATABASE_URL


def _ensure_version_column_is_text(connection: sa.engine.Connection) -> None:
    """
    Guarantee that alembic_version.version_num is TEXT, not VARCHAR(32).

    This is an idempotent DDL guard that must run **before** any migration
    attempt.  It handles three states:

    1. Table absent   → pre-create it with TEXT so Alembic never writes
                        VARCHAR(32) in the first place.
    2. Column VARCHAR → ALTER COLUMN … TYPE TEXT (PostgreSQL no-op if already
                        wider than needed, error-free).
    3. Column TEXT    → ALTER is a no-op; function returns immediately.

    All branches are safe to call on every startup.
    """
    insp = sa.inspect(connection)
    if not insp.has_table("alembic_version"):
        # Pre-create the table with TEXT so Alembic never uses VARCHAR(32).
        connection.execute(sa.text(
            "CREATE TABLE alembic_version "
            "(version_num TEXT NOT NULL, CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num))"
        ))
        connection.commit()
        return

    # Table exists — check the column type.
    cols = {c["name"]: c for c in insp.get_columns("alembic_version")}
    col_type = str(cols.get("version_num", {}).get("type", "")).upper()
    if "VARCHAR" in col_type or "CHAR" in col_type:
        connection.execute(sa.text(
            "ALTER TABLE alembic_version ALTER COLUMN version_num TYPE TEXT"
        ))
        connection.commit()
    # TEXT or unknown — leave as-is.


# ── Offline migrations (generate SQL without a live DB) ───────────────────────

def run_migrations_offline() -> None:
    """
    Run migrations in 'offline' mode.

    This emits SQL to stdout / a file without connecting to the database.
    Useful for generating migration scripts to review before applying.
    """
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


# ── Online migrations (apply against a live DB) ───────────────────────────────

def do_run_migrations(connection: sa.engine.Connection) -> None:
    """Configure and run migrations using *connection*."""
    # Widen version_num to TEXT before Alembic reads or writes the version table.
    _ensure_version_column_is_text(connection)

    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """
    Run migrations in 'online' mode using an async engine.

    asyncpg does not support the synchronous connection interface that
    Alembic uses internally, so we run the sync migration function inside
    ``run_sync`` on an async connection.
    """
    connectable = create_async_engine(
        get_url(),
        poolclass=pool.NullPool,  # don't keep connections open after migration
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


# ── Entry point ────────────────────────────────────────────────────────────────

if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
