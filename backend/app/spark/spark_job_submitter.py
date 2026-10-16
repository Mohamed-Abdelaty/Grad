"""
app/spark/spark_job_submitter.py
----------------------------------
Submits PySpark jobs to the Spark cluster via ``spark-submit`` and parses
the job result from the subprocess stdout.

Design decisions
-----------------
* **asyncio.create_subprocess_exec** is used so the FastAPI event loop is
  not blocked while Spark runs (can take minutes for large datasets).
* The ``SPARK_JOB_RESULT:`` prefix protocol lets us extract structured
  metrics from stdout without any file-system side-channel.
* On timeout the subprocess is forcibly killed (``proc.kill()``) and then
  waited to reap the zombie before raising ``SparkJobTimeoutError``.
* ``health_check()`` calls the Spark master REST API (``/json/``) so the
  ``GET /api/spark/health`` endpoint and ``GET /api/system/status`` can
  report real cluster state.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
from collections import deque
from collections.abc import Callable
from typing import Any

import httpx

from app.core.config import get_settings
from app.pipeline.stream_progress import (
    parse_stream_progress_line,
    parse_stream_started_line,
)
from app.spark.exceptions import SparkJobFailedError, SparkJobTimeoutError

logger = logging.getLogger(__name__)

# Prefix that batch_processing_job.py prints to stdout
_RESULT_PREFIX = "SPARK_JOB_RESULT:"
_PROGRESS_PREFIX = "SPARK_JOB_PROGRESS:"


def parse_progress_line(line: str) -> dict[str, Any] | None:
    """
    Parse a SPARK_JOB_PROGRESS line from stdout.

    The line must start with SPARK_JOB_PROGRESS: (after optional whitespace).
    Log noise before the prefix causes the line to be ignored (returns None).
    Malformed JSON returns None and never raises.
    """
    stripped = line.strip()
    if not stripped.startswith(_PROGRESS_PREFIX):
        return None
    json_part = stripped[len(_PROGRESS_PREFIX):].strip()
    try:
        data = json.loads(json_part)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return None


class SparkJobSubmitter:
    """
    Submits ``spark-submit`` jobs and interprets their output.

    All public methods are async to avoid blocking the FastAPI event loop
    while waiting for Spark jobs, which can run for minutes.

    Usage::

        submitter = SparkJobSubmitter()
        result = await submitter.submit_batch_job(
            input_source="/data/sales.csv",
            output_path="/output/sales",
            processing_config={"processing": {"operations": [...]}},
        )
        print(result)  # {"input_rows": 20, "output_rows": 18, ...}
    """

    def __init__(self) -> None:
        """Initialise with settings from the environment."""
        self._settings = get_settings()

    # ── Public API ─────────────────────────────────────────────────────────────

    async def submit_batch_job(
        self,
        input_source: str,
        output_path: str,
        processing_config: dict[str, Any],
        run_id: str | None = None,
        on_progress: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """
        Submit ``batch_processing_job.py`` to the Spark cluster.

        Parameters
        ----------
        input_source : str
            Absolute path to the input CSV or JSON file (must be accessible
            from within the Spark worker containers via the shared volume).
        output_path : str
            Absolute path where the job should write Parquet output.
        processing_config : dict
            Full pipeline processing configuration serialised and passed as
            ``--config-json``.

        Returns
        -------
        dict
            Parsed result dict from the ``SPARK_JOB_RESULT:`` stdout line:
            ``{"input_rows": int, "output_rows": int, "duration_seconds": float,
            "status": "success"}``.

        Raises
        ------
        SparkJobTimeoutError
            Subprocess exceeded ``SPARK_SUBMIT_TIMEOUT_SECONDS``.
        SparkJobFailedError
            Subprocess exited non-zero.
        """
        jobs_path = self._settings.SPARK_JOBS_PATH
        job_script = f"{jobs_path}/batch_processing_job.py"
        config_json = json.dumps(processing_config)

        spark_home = os.environ.get("SPARK_HOME", "/opt/bitnami/spark")
        cmd = [
            f"{spark_home}/bin/spark-submit",
            "--master", self._settings.SPARK_MASTER_URL,
            job_script,
            "--config-json", config_json,
            "--input-source", input_source,
            "--output-path", output_path,
        ]

        logger.info(
            "Submitting batch Spark job: input=%s output=%s master=%s run_id=%s",
            input_source,
            output_path,
            self._settings.SPARK_MASTER_URL,
            run_id,
        )

        stdout, stderr = await self._run_subprocess(
            cmd, run_id=run_id, on_progress=on_progress
        )
        return self._parse_result(stdout, stderr)

    async def submit_streaming_job(
        self,
        config: dict[str, Any],
        output_path: str,
        checkpoint_path: str | None = None,
        run_id: str | None = None,
        on_stream_started: Callable[[dict[str, Any]], None] | None = None,
        on_stream_progress: Callable[[dict[str, Any]], None] | None = None,
        stop_event: asyncio.Event | None = None,
    ) -> dict[str, Any]:
        """
        Submit ``streaming_processing_job.py`` to the Spark cluster.

        Parameters
        ----------
        config : dict
            Full pipeline processing configuration serialized as JSON.
        output_path : str
            Absolute HDFS path where Parquet output will be appended.
        checkpoint_path : str | None
            Absolute HDFS checkpoint path.
        run_id : str | None
            Tracking run ID.
        on_stream_started : Callable | None
            Callback for STREAM_STARTED events.
        on_stream_progress : Callable | None
            Callback for STREAM_PROGRESS events.
        stop_event : asyncio.Event | None
            Event to signal graceful SIGTERM shutdown of the query.

        Returns
        -------
        dict
            Parsed result metrics dict.
        """
        jobs_path = self._settings.SPARK_JOBS_PATH
        job_script = f"{jobs_path}/streaming_processing_job.py"
        config_json = json.dumps(config)
        spark_home = os.environ.get("SPARK_HOME", "/opt/bitnami/spark")

        cmd = [
            f"{spark_home}/bin/spark-submit",
            "--master", self._settings.SPARK_MASTER_URL,
            "--conf", "spark.cores.max=1",
            job_script,
            "--config-json", config_json,
            "--input-source", "kafka",
            "--output-path", output_path,
        ]
        if checkpoint_path:
            cmd.extend(["--checkpoint-path", checkpoint_path])

        logger.info(
            "Submitting streaming Spark job: output=%s checkpoint=%s master=%s run_id=%s",
            output_path,
            checkpoint_path,
            self._settings.SPARK_MASTER_URL,
            run_id,
        )

        stdout, stderr = await self._run_subprocess(
            cmd,
            run_id=run_id,
            on_stream_started=on_stream_started,
            on_stream_progress=on_stream_progress,
            stop_event=stop_event,
        )
        return self._parse_result(stdout, stderr)

    async def health_check(self) -> dict[str, Any]:
        """
        Probe the Spark master REST API to confirm cluster availability.

        Calls ``http://spark-master:8080/json/`` and extracts worker count
        and status from the JSON response.

        Returns
        -------
        dict
            ``{"connected": bool, "worker_count": int, "status": str,
            "error": str | None}``
        """
        # Derive the master UI URL from the spark master URL
        # spark://spark-master:7077 → http://spark-master:8080
        master_url = self._settings.SPARK_MASTER_URL  # spark://spark-master:7077
        try:
            host = master_url.replace("spark://", "").split(":")[0]
            ui_url = f"http://{host}:8080/json/"
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(ui_url)
                resp.raise_for_status()
                data = resp.json()
            worker_count = len(data.get("workers", []))
            status = data.get("status", "UNKNOWN")
            logger.info(
                "Spark health check: status=%s workers=%d", status, worker_count
            )
            return {
                "connected": True,
                "worker_count": worker_count,
                "status": status,
                "error": None,
            }
        except Exception as exc:
            logger.warning("Spark health check failed: %s", exc)
            return {
                "connected": False,
                "worker_count": 0,
                "status": "UNREACHABLE",
                "error": str(exc),
            }

    # ── Private helpers ────────────────────────────────────────────────────────

    async def _run_subprocess(
        self,
        cmd: list[str],
        run_id: str | None = None,
        on_progress: Callable[[dict[str, Any]], None] | None = None,
        on_stream_started: Callable[[dict[str, Any]], None] | None = None,
        on_stream_progress: Callable[[dict[str, Any]], None] | None = None,
        stop_event: asyncio.Event | None = None,
    ) -> tuple[str, str]:
        """
        Execute *cmd* asynchronously, enforcing the configured timeout.

        Parameters
        ----------
        cmd : list[str]
            Command and arguments to execute.
        run_id : str | None
            If provided, the subprocess PID is registered with the global
            :data:`~app.pipeline.progress.tracker` so the cancel endpoint
            can send SIGTERM while the job is running.
        on_progress : Callable[[dict], None] | None
            Optional callback invoked whenever a valid SPARK_JOB_PROGRESS
            line is received on stdout while the job is running.
        on_stream_started : Callable[[dict], None] | None
            Optional callback invoked on STREAM_STARTED lines.
        on_stream_progress : Callable[[dict], None] | None
            Optional callback invoked on STREAM_PROGRESS lines.
        stop_event : asyncio.Event | None
            Optional event to trigger graceful SIGTERM shutdown of the query.

        Returns
        -------
        tuple[str, str]
            ``(stdout, stderr)`` as decoded strings.

        Raises
        ------
        SparkJobTimeoutError
            Process exceeded ``SPARK_SUBMIT_TIMEOUT_SECONDS``.
        SparkJobFailedError
            Process exited with a non-zero return code.
        """
        from app.pipeline.progress import tracker  # avoid circular import at module level

        timeout = self._settings.SPARK_SUBMIT_TIMEOUT_SECONDS
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ, "HADOOP_USER_NAME": self._settings.HDFS_USER},
            start_new_session=True,
        )

        # Register the PID with the tracker so cancel can kill this process.
        if run_id is not None:
            tracker.register_proc_pid(run_id, proc.pid)

        stdout_deque: deque[str] = deque(maxlen=2000)
        stderr_deque: deque[str] = deque(maxlen=2000)

        def _dispatch_stdout(line: str) -> None:
            if on_stream_started is not None:
                started = parse_stream_started_line(line)
                if started is not None:
                    try:
                        on_stream_started(started)
                    except Exception:
                        pass
            if on_stream_progress is not None:
                sprog = parse_stream_progress_line(line)
                if sprog is not None:
                    try:
                        on_stream_progress(sprog)
                    except Exception:
                        pass
            if on_progress is not None:
                prog = parse_progress_line(line)
                if prog is not None:
                    try:
                        on_progress(prog)
                    except Exception:
                        pass

        async def _read_stream(
            stream: asyncio.StreamReader,
            target_deque: deque[str],
            is_stdout: bool,
        ) -> None:
            buffer = ""
            while True:
                chunk = await stream.read(65536)
                if not chunk:
                    break
                text = chunk.decode("utf-8", errors="replace")
                buffer += text
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    target_deque.append(line)
                    if is_stdout:
                        _dispatch_stdout(line)
            if buffer:
                target_deque.append(buffer)
                if is_stdout:
                    _dispatch_stdout(buffer)

        async def _watch_stop_event() -> None:
            if stop_event is None:
                return
            await stop_event.wait()
            try:
                pgid = os.getpgid(proc.pid)
                os.killpg(pgid, signal.SIGTERM)
                logger.info("Sent SIGTERM to process group %d on stop_event", pgid)
            except Exception as exc:
                try:
                    proc.terminate()
                except Exception:
                    pass

        gather_coros = [
            _read_stream(proc.stdout, stdout_deque, True),
            _read_stream(proc.stderr, stderr_deque, False),
            proc.wait(),
        ]
        if stop_event is not None:
            gather_coros.append(_watch_stop_event())

        try:
            await asyncio.wait_for(
                asyncio.gather(*gather_coros),
                timeout=float(timeout),
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise SparkJobTimeoutError(
                f"spark-submit timed out after {timeout}s",
                stdout="\n".join(stdout_deque),
                stderr="\n".join(stderr_deque),
            )
        finally:
            # Always deregister — whether the job succeeded, failed, or was killed.
            if run_id is not None:
                tracker.deregister_proc_pid(run_id)

        stdout = "\n".join(stdout_deque)
        stderr = "\n".join(stderr_deque)

        if proc.returncode != 0:
            if stop_event is not None and stop_event.is_set():
                for line in stdout.splitlines():
                    if line.strip().startswith(_RESULT_PREFIX):
                        return stdout, stderr

            diagnostic_lines = (stderr + "\n" + stdout).splitlines()
            markers = ("error", "exception", "caused by", "failed", "denied")
            matches = [
                index
                for index, line in enumerate(diagnostic_lines)
                if any(marker in line.lower() for marker in markers)
            ]
            selected: list[str] = []
            for index in matches:
                selected.extend(diagnostic_lines[max(0, index - 2) : index + 4])
            diagnostic = "\n".join(dict.fromkeys(selected)) or "\n".join(diagnostic_lines[-40:])
            raise SparkJobFailedError(
                f"spark-submit exited with code {proc.returncode}",
                stdout=stdout,
                stderr=diagnostic,
            )

        return stdout, stderr

    def _parse_result(self, stdout: str, stderr: str) -> dict[str, Any]:
        """
        Extract the ``SPARK_JOB_RESULT:`` line from *stdout*.

        Parameters
        ----------
        stdout : str
            Full stdout from the ``spark-submit`` process.
        stderr : str
            Full stderr (used in error message if no result line found).

        Returns
        -------
        dict
            Parsed result dict.

        Raises
        ------
        SparkJobFailedError
            No ``SPARK_JOB_RESULT:`` line was found in stdout.
        """
        for line in stdout.splitlines():
            stripped = line.strip()
            if stripped.startswith(_RESULT_PREFIX):
                json_part = stripped[len(_RESULT_PREFIX):]
                try:
                    return json.loads(json_part)
                except json.JSONDecodeError as exc:
                    raise SparkJobFailedError(
                        f"Could not parse SPARK_JOB_RESULT JSON: {exc}",
                        stdout=stdout,
                        stderr=stderr,
                    ) from exc

        raise SparkJobFailedError(
            "No SPARK_JOB_RESULT line found in spark-submit output. "
            "The job may have failed before completing.",
            stdout=stdout,
            stderr=stderr,
        )
