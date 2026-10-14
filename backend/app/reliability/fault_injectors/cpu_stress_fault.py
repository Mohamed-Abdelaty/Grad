"""
app/reliability/fault_injectors/cpu_stress_fault.py
-----------------------------------------------------
Fault injector: ``cpu_stress``

Runs ``stress-ng --cpu <cores> --timeout <duration>s`` inside the target
container to saturate CPU resources.

Approach: execute ``stress-ng`` in **detached mode** inside the target
container (rather than spinning up an ephemeral sidecar) because:
- The target container is already running and accessible.
- Detached exec ties the stress process to the target container's cgroup,
  giving an accurate representation of CPU pressure on that service.
- No sidecar networking or lifecycle management overhead.

Requirements
    ``stress-ng`` must be installed in the target container image.
    If absent, the injector fails with a clear ``FaultInjectionError`` — never
    a silent no-op.

Rollback strategy
    ``stress-ng`` with ``--timeout`` exits automatically.  For safety,
    ``rollback()`` sends a ``pkill stress-ng`` to kill any lingering processes.
"""

from __future__ import annotations

import logging

from app.reliability.fault_injectors.base_fault import (
    BaseFaultInjector,
    register_fault_injector,
)

logger = logging.getLogger(__name__)

_DEFAULT_CORES = 2
_DEFAULT_DURATION = 20


@register_fault_injector
class CpuStressFault(BaseFaultInjector):
    """Run stress-ng CPU stress inside the target container."""

    fault_type = "cpu_stress"
    label = "CPU stress"
    description = "Consumes CPU cores using stress workloads to simulate resource exhaustion."
    blast_radius = "Increased latency and thread throttling on the target."
    params = [{"name": "cores", "type": "int", "min": 1, "max": 4, "default": 2}]
    icon = "cpu"

    def inject(self, target_service: str, params: dict) -> dict:
        """
        Start ``stress-ng --cpu`` in detached mode inside ``target_service``.

        Parameters
        ----------
        params : dict
            Keys: ``cores`` (int, default 2), ``duration`` (int, default 20s).

        Returns
        -------
        dict
            ``{"fault_type": str, "cores": int, "duration": int, "started_at": str}``
        """
        cores = int(params.get("cores", _DEFAULT_CORES))
        duration = int(params.get("duration", _DEFAULT_DURATION))

        command = [
            "stress-ng",
            "--cpu", str(cores),
            "--timeout", f"{duration}s",
            "--quiet",
        ]
        logger.info(
            "CpuStressFault: starting stress-ng (cores=%d, duration=%ds) "
            "in '%s'.",
            cores,
            duration,
            target_service,
        )
        result = self._docker.exec_in_container_detached(target_service, command)
        result["fault_type"] = self.fault_type
        result["cores"] = cores
        result["duration"] = duration
        return result

    def rollback(self, target_service: str, injection_metadata: dict) -> None:
        """
        Kill any lingering ``stress-ng`` processes in the container.

        stress-ng exits automatically via ``--timeout``, but if the experiment
        is cancelled early, this ensures no zombie stress processes remain.
        """
        try:
            exit_code, output = self._docker.exec_in_container(
                target_service, ["pkill", "-f", "stress-ng"]
            )
            # pkill returns 1 if no process found — that's fine
            if exit_code in (0, 1):
                logger.info(
                    "CpuStressFault.rollback: stress-ng stopped in '%s'.",
                    target_service,
                )
            else:
                logger.warning(
                    "CpuStressFault.rollback: pkill returned %d in '%s': %s",
                    exit_code,
                    target_service,
                    output,
                )
        except Exception as exc:
            logger.warning(
                "CpuStressFault.rollback: failed to kill stress-ng in '%s': %s",
                target_service,
                exc,
            )
