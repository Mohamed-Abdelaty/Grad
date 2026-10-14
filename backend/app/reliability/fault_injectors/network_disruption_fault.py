"""
app/reliability/fault_injectors/network_disruption_fault.py
-------------------------------------------------------------
Fault injector: ``network_disruption``

Applies 100% packet loss to the target container's ``eth0`` interface using
Linux ``tc`` (traffic control) ``netem`` discipline.

Why ``tc netem`` over disconnecting the Docker network?
    - Reversible in a single command: ``tc qdisc del dev eth0 root netem``
    - Does not require network-level Docker operations that can race with
      container orchestration.
    - Provides a clean audit trail in the container's network stats.

Requirements
    The target container image MUST have ``iproute2`` (``tc``) installed.
    If ``tc`` is absent, ``DockerController.apply_network_disruption`` raises
    a ``FaultInjectionError`` with a clear diagnostic — never a silent no-op.
"""

from __future__ import annotations

import logging

from app.reliability.fault_injectors.base_fault import (
    BaseFaultInjector,
    register_fault_injector,
)

logger = logging.getLogger(__name__)


@register_fault_injector
class NetworkDisruptionFault(BaseFaultInjector):
    """Apply 100% packet loss via tc netem; remove it on rollback."""

    fault_type = "network_disruption"
    label = "Network disruption"
    description = "Drops network packets on the container interface via traffic control."
    blast_radius = "Target cannot communicate with peer services during disruption."
    params = []
    icon = "wifi-off"

    def inject(self, target_service: str, params: dict) -> dict:
        """
        Apply 100% packet loss on the target's ``eth0`` interface.

        Parameters
        ----------
        params : dict
            Expected keys: ``duration`` (seconds, informational — caller
            controls the wait).

        Returns
        -------
        dict
            ``{"action": str, "rule": str, "applied_at": str, ...}``
        """
        duration = params.get("duration", 20)
        logger.info(
            "NetworkDisruptionFault: applying 100%% packet loss to '%s' "
            "(duration hint: %ds).",
            target_service,
            duration,
        )
        result = self._docker.apply_network_disruption(target_service, duration)
        result["fault_type"] = self.fault_type
        return result

    def rollback(self, target_service: str, injection_metadata: dict) -> None:
        """
        Remove the ``tc netem`` packet-loss rule.

        Safe to call even if ``inject()`` never ran — ``DockerController``
        handles the benign case where no rule exists.
        """
        try:
            self._docker.remove_network_disruption(target_service)
            logger.info(
                "NetworkDisruptionFault.rollback: disruption removed from '%s'.",
                target_service,
            )
        except Exception as exc:
            logger.warning(
                "NetworkDisruptionFault.rollback: failed to remove disruption "
                "from '%s': %s",
                target_service,
                exc,
            )
