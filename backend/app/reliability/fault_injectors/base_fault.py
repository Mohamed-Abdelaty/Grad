"""
app/reliability/fault_injectors/base_fault.py
----------------------------------------------
Abstract base class and registry for fault injectors.

Registry pattern
----------------
Adding a new fault type requires only:
1. Creating a new module in ``fault_injectors/``.
2. Defining a class that extends ``BaseFaultInjector``.
3. Decorating it with ``@register_fault_injector``.

No changes to the runner, API, or any other core module are needed.
This mirrors the Phase 3 plugin system design for the same extensibility reason.

Architectural rule (ENFORCED)
-------------------------------
Fault injectors MUST NOT import or use the ``docker`` module directly.
All Docker operations MUST go through ``DockerController``, which in turn
always calls ``SafetyValidator``.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, ClassVar

logger = logging.getLogger(__name__)

# ── Registry ───────────────────────────────────────────────────────────────────

FAULT_INJECTOR_REGISTRY: dict[str, type["BaseFaultInjector"]] = {}
"""
Global registry mapping ``fault_type`` strings to injector classes.

Populated automatically by ``@register_fault_injector``.  Read-only outside
this module.
"""


def register_fault_injector(cls: type["BaseFaultInjector"]) -> type["BaseFaultInjector"]:
    """
    Class decorator that registers a ``BaseFaultInjector`` subclass.

    Usage::

        @register_fault_injector
        class MyFault(BaseFaultInjector):
            fault_type = "my_fault"
            ...
    """
    key = cls.fault_type
    if not key:
        raise ValueError(f"{cls.__name__} must define a non-empty ``fault_type``.")
    if key in FAULT_INJECTOR_REGISTRY:
        logger.warning(
            "Fault injector '%s' already registered — overwriting with %s.",
            key,
            cls.__name__,
        )
    FAULT_INJECTOR_REGISTRY[key] = cls
    logger.debug("Registered fault injector: '%s' → %s", key, cls.__name__)
    return cls


# ── Abstract base class ────────────────────────────────────────────────────────

class BaseFaultInjector(ABC):
    """
    Abstract base for all ChaosLab fault injectors.

    Subclasses must define ``fault_type`` and implement ``inject`` + ``rollback``.

    Parameters
    ----------
    docker_controller : DockerController
        Injected via constructor — injectors NEVER instantiate DockerController
        themselves, ensuring the safety validator is always included.
    """

    fault_type: ClassVar[str] = ""
    """Registry key — must match a value from the ``ALLOWED_FAULT_TYPES`` Literal."""

    label: ClassVar[str] = ""
    description: ClassVar[str] = ""
    blast_radius: ClassVar[str] = ""
    params: ClassVar[list[dict[str, Any]]] = []
    icon: ClassVar[str] = "square"

    def __init__(self, docker_controller: Any) -> None:
        """Accept DockerController via dependency injection."""
        # ARCHITECTURAL RULE: injectors receive DockerController — they never
        # import ``docker`` directly.  The type hint is ``Any`` to avoid a
        # circular import; the actual type is DockerController.
        self._docker = docker_controller

    @abstractmethod
    def inject(self, target_service: str, params: dict) -> dict:
        """
        Apply the fault to ``target_service``.

        Parameters
        ----------
        target_service : str
            Service name (already validated by the experiment runner).
        params : dict
            Fault-specific parameters from the experiment config.

        Returns
        -------
        dict
            Injection metadata needed by ``rollback()`` to precisely undo
            what was done.  Must include at minimum ``injected_at``.

        Raises
        ------
        FaultInjectionError
            If the injection fails after safety checks pass.
        """

    @abstractmethod
    def rollback(self, target_service: str, injection_metadata: dict) -> None:
        """
        Undo the fault applied by ``inject()``.

        This method MUST be:
        - **Always safely callable** — even if ``inject()`` failed partway
          through.  Implementations must be defensive and idempotent.
        - **Never raises** — catch all exceptions internally, log them as
          warnings (using ``RollbackError`` if needed), but never propagate.
          The experiment runner calls this in a ``finally`` block; an exception
          here would mask the original failure.

        Parameters
        ----------
        target_service : str
            Same service targeted by ``inject()``.
        injection_metadata : dict
            The dict returned by ``inject()``, or ``{}`` if inject failed.
        """
