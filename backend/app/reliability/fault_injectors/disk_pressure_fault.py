"""
app/reliability/fault_injectors/disk_pressure_fault.py
--------------------------------------------------------
Fault injector: disk_pressure

Writes a large scratch file inside the target container's writable layer to
consume disk space and create I/O pressure.

Implementation uses dd (universally available in Linux containers) to
write a bounded scratch file to /tmp/chaoslab_disk_scratch inside the
container. This is safer than fallocate (not always available) and
simpler than stress-ng --fallocate (which requires stress-ng).

Safety limits (ENFORCED regardless of configuration)
    - Maximum scratch file size: 500 MB -- hard-coded, never overridable
      by experiment YAML.
    - Always writes to the container's /tmp directory (writable layer),
      never the host filesystem.
    - rollback() always removes the scratch file, even if inject failed.

Requirements
    dd -- available in virtually all Linux base images (coreutils).
    No additional packages required.
"""

from __future__ import annotations

import logging

from app.reliability.fault_injectors.base_fault import (
    BaseFaultInjector,
    register_fault_injector,
)

logger = logging.getLogger(__name__)

_MAX_DISK_PRESSURE_MB: int = 500
_SCRATCH_PATH: str = "/tmp/chaoslab_disk_scratch"
_DEFAULT_DISK_MB: int = 200


@register_fault_injector
class DiskPressureFault(BaseFaultInjector):
    """Write a bounded scratch file to consume disk space in the target container."""

    fault_type = "disk_pressure"
    label = "Disk pressure"
    description = "Fills temporary disk space to simulate disk quota exhaustion."
    blast_radius = "Write operations to disk will fail due to lack of space."
    params = [{"name": "disk_size_mb", "type": "int", "min": 10, "max": 500, "default": 100}]
    icon = "hard-drive"

    def inject(self, target_service: str, params: dict) -> dict:
        """
        Write a scratch file of disk_size_mb MB inside the target container.

        Parameters
        ----------
        params : dict
            Key: disk_size_mb (int, default 200, max 500 -- hard-coded ceiling).

        Returns
        -------
        dict
            {"scratch_path": str, "size_mb": int, "started_at": str}
        """
        raw_mb = params.get("disk_size_mb")
        requested_mb = int(raw_mb) if raw_mb is not None else _DEFAULT_DISK_MB
        # HARD SAFETY LIMIT -- never exceed 500 MB regardless of config
        size_mb = min(requested_mb, _MAX_DISK_PRESSURE_MB)
        if requested_mb > _MAX_DISK_PRESSURE_MB:
            logger.warning(
                "DiskPressureFault: requested %dMB exceeds ceiling of %dMB -- capping at %dMB.",
                requested_mb,
                _MAX_DISK_PRESSURE_MB,
                _MAX_DISK_PRESSURE_MB,
            )

        # dd writes <size_mb> x 1MB blocks to the scratch path
        command = [
            "dd",
            "if=/dev/zero",
            f"of={_SCRATCH_PATH}",
            "bs=1M",
            f"count={size_mb}",
            "status=none",
        ]
        logger.info(
            "DiskPressureFault: writing %dMB scratch file in '%s' at '%s'.",
            size_mb,
            target_service,
            _SCRATCH_PATH,
        )
        result = self._docker.exec_in_container_detached(target_service, command)
        result["fault_type"] = self.fault_type
        result["scratch_path"] = _SCRATCH_PATH
        result["size_mb"] = size_mb
        return result

    def rollback(self, target_service: str, injection_metadata: dict) -> None:
        """
        Remove the scratch file from the target container.

        Safe to call even if inject() never completed -- rm -f on a
        non-existent path is a no-op.
        """
        scratch_path = injection_metadata.get("scratch_path", _SCRATCH_PATH)
        try:
            exit_code, output = self._docker.exec_in_container(
                target_service, ["rm", "-f", scratch_path]
            )
            if exit_code == 0:
                logger.info(
                    "DiskPressureFault.rollback: removed '%s' from '%s'.",
                    scratch_path,
                    target_service,
                )
            else:
                logger.warning(
                    "DiskPressureFault.rollback: rm returned %d in '%s': %s",
                    exit_code,
                    target_service,
                    output,
                )
        except Exception as exc:
            logger.warning(
                "DiskPressureFault.rollback: failed to remove scratch file from '%s': %s",
                target_service,
                exc,
            )
