"""
app/reliability/fault_injectors/memory_stress_fault.py
--------------------------------------------------------
Fault injector: ``memory_stress``

Runs ``stress-ng --vm <workers> --vm-bytes <size>`` inside the target container
to apply memory pressure.

Same execution model as ``cpu_stress_fault`` — detached exec inside the target
container, tied to that container's cgroup for accurate resource attribution.

Requirements
    ``stress-ng`` must be installed in the target container image.
    Fails loudly with ``FaultInjectionError`` if absent — never silent.
"""

from __future__ import annotations

import logging

from app.reliability.fault_injectors.base_fault import (
    BaseFaultInjector,
    register_fault_injector,
)

logger = logging.getLogger(__name__)

_DEFAULT_VM_WORKERS = 2
_DEFAULT_VM_BYTES = "256M"
_DEFAULT_DURATION = 20


@register_fault_injector
class MemoryStressFault(BaseFaultInjector):
    """Run stress-ng VM workers inside the target container."""

    fault_type = "memory_stress"
    label = "Memory stress"
    description = "Allocates memory using stress workloads to simulate memory leaks."
    blast_radius = "Potential out-of-memory errors or severe GC pauses on the target."
    params = [{"name": "memory_bytes", "type": "str", "default": "256M"}]
    icon = "database"

    def inject(self, target_service: str, params: dict) -> dict:
        """
        Start ``stress-ng --vm`` in detached mode inside ``target_service``.

        Parameters
        ----------
        params : dict
            Keys: ``vm_workers`` (int), ``memory_bytes`` (str, e.g. ``'256M'``),
            ``duration`` (int seconds).

        Returns
        -------
        dict
            Injection metadata for rollback.
        """
        workers = int(params.get("vm_workers", _DEFAULT_VM_WORKERS))
        memory_bytes = str(params.get("memory_bytes", _DEFAULT_VM_BYTES))
        duration = int(params.get("duration", _DEFAULT_DURATION))

        command = [
            "stress-ng",
            "--vm", str(workers),
            "--vm-bytes", memory_bytes,
            "--timeout", f"{duration}s",
            "--quiet",
        ]
        logger.info(
            "MemoryStressFault: stress-ng (workers=%d, bytes=%s, duration=%ds) "
            "in '%s'.",
            workers,
            memory_bytes,
            duration,
            target_service,
        )
        result = self._docker.exec_in_container_detached(target_service, command)
        result["fault_type"] = self.fault_type
        result["vm_workers"] = workers
        result["memory_bytes"] = memory_bytes
        result["duration"] = duration
        return result

    def rollback(self, target_service: str, injection_metadata: dict) -> None:
        """Kill any lingering stress-ng processes."""
        try:
            exit_code, output = self._docker.exec_in_container(
                target_service, ["pkill", "-f", "stress-ng"]
            )
            if exit_code in (0, 1):
                logger.info(
                    "MemoryStressFault.rollback: stress-ng stopped in '%s'.",
                    target_service,
                )
            else:
                logger.warning(
                    "MemoryStressFault.rollback: pkill returned %d in '%s': %s",
                    exit_code,
                    target_service,
                    output,
                )
        except Exception as exc:
            logger.warning(
                "MemoryStressFault.rollback: failed in '%s': %s",
                target_service,
                exc,
            )
