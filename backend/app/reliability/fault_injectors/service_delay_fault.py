"""Fault injector for controlled, reversible service latency."""

from __future__ import annotations

import logging
from typing import Any

from app.reliability.exceptions import FaultInjectionError
from app.reliability.fault_injectors.base_fault import BaseFaultInjector, register_fault_injector

logger = logging.getLogger(__name__)


@register_fault_injector
class ServiceDelayFault(BaseFaultInjector):
    """Add latency to a container's ``eth0`` using the controller's safe exec API."""

    fault_type = "service_delay"
    label = "Service delay"
    description = "Adds latency to service network interface using netem."
    blast_radius = "Elevated response latency and caller timeouts."
    params = [{"name": "delay_ms", "type": "int", "min": 10, "max": 5000, "default": 200}]
    icon = "clock"
    default_delay_ms = 200

    def inject(self, target_service: str, params: dict[str, Any]) -> dict[str, Any]:
        delay_ms = int(params.get("delay_ms", self.default_delay_ms))
        duration = int(params.get("duration", 20))
        command = [
            "tc", "qdisc", "replace", "dev", "eth0", "root", "netem", "delay", f"{delay_ms}ms"
        ]
        exit_code, output = self._docker.exec_in_container(target_service, command)
        if exit_code != 0:
            raise FaultInjectionError(
                f"tc netem delay failed for '{target_service}' (exit={exit_code}): {output}",
                fault_type=self.fault_type,
                target_service=target_service,
            )
        result = {
            "fault_type": self.fault_type,
            "delay_ms": delay_ms,
            "duration_hint_seconds": duration,
        }
        logger.info(
            "Service delay injected for %s.",
            target_service,
            extra={"target_service": target_service, "delay_ms": delay_ms},
        )
        return result

    def rollback(self, target_service: str, injection_metadata: dict[str, Any]) -> None:
        try:
            self._docker.exec_in_container(
                target_service, ["tc", "qdisc", "del", "dev", "eth0", "root", "netem"]
            )
        except Exception as exc:
            logger.warning("Service delay rollback failed for %s: %s", target_service, exc)
