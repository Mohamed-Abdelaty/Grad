"""
app/reliability/fault_injectors/container_stop_fault.py
---------------------------------------------------------
Fault injector: ``container_stop``

Stops the target container for the configured duration, then starts it back up.

Lifecycle:
- ``inject()``  → ``DockerController.stop_container()``
- Caller waits ``fault.duration`` seconds (the runner controls timing)
- ``rollback()`` → ``DockerController.start_container()`` (idempotent: checks
  ``is_container_running`` first to avoid double-starting an already-running
  container after a restart or natural recovery).
"""

from __future__ import annotations

import logging

from app.reliability.fault_injectors.base_fault import (
    BaseFaultInjector,
    register_fault_injector,
)

logger = logging.getLogger(__name__)


@register_fault_injector
class ContainerStopFault(BaseFaultInjector):
    """Stop the target container; start it back up on rollback."""

    fault_type = "container_stop"
    label = "Container stop"
    description = "Stops the target container to simulate total node or service crash."
    blast_radius = "The service is unavailable for the whole duration."
    params = []
    icon = "square"

    def inject(self, target_service: str, params: dict) -> dict:
        """
        Stop the target container.

        Returns
        -------
        dict
            ``{"container_id": str, "container_name": str, "stopped_at": str}``
        """
        logger.info(
            "ContainerStopFault: injecting stop fault on service '%s'.",
            target_service,
        )
        result = self._docker.stop_container(target_service)
        result["fault_type"] = self.fault_type
        return result

    def rollback(self, target_service: str, injection_metadata: dict) -> None:
        """
        Start the container back up if it is not already running.

        Safe to call even if ``inject()`` never succeeded (idempotent).
        """
        try:
            if self._docker.is_container_running(target_service):
                logger.debug(
                    "ContainerStopFault.rollback: '%s' already running — no-op.",
                    target_service,
                )
                return
            self._docker.start_container(target_service)
            logger.info(
                "ContainerStopFault.rollback: started '%s' successfully.",
                target_service,
            )
        except Exception as exc:
            logger.warning(
                "ContainerStopFault.rollback: failed to start '%s': %s "
                "(container may need manual recovery).",
                target_service,
                exc,
            )
