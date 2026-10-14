"""
app/reliability/fault_injectors/container_restart_fault.py
------------------------------------------------------------
Fault injector: ``container_restart``

Restarts the target container immediately.  This is an instantaneous fault
(not duration-based) — by the time ``inject()`` returns, the container is
already restarting.  ``rollback()`` is therefore a no-op.
"""

from __future__ import annotations

import logging

from app.reliability.fault_injectors.base_fault import (
    BaseFaultInjector,
    register_fault_injector,
)

logger = logging.getLogger(__name__)


@register_fault_injector
class ContainerRestartFault(BaseFaultInjector):
    """Restart the target container immediately."""

    fault_type = "container_restart"
    label = "Container restart"
    description = "Restarts the target container to simulate transient unexpected reboot."
    blast_radius = "Brief service interruption while the process restarts."
    params = []
    icon = "refresh-cw"

    def inject(self, target_service: str, params: dict) -> dict:
        """
        Restart the target container.

        Returns
        -------
        dict
            ``{"container_id": str, "container_name": str, "restarted_at": str}``
        """
        logger.info(
            "ContainerRestartFault: injecting restart fault on service '%s'.",
            target_service,
        )
        result = self._docker.restart_container(target_service)
        result["fault_type"] = self.fault_type
        return result

    def rollback(self, target_service: str, injection_metadata: dict) -> None:
        """
        No-op — the container is already back by the time restart returns.

        The restart fault is instantaneous: Docker's ``container.restart()``
        blocks until the container is running again.
        """
        logger.debug(
            "ContainerRestartFault.rollback: no-op for '%s' (restart is instantaneous).",
            target_service,
        )
