from __future__ import annotations

import importlib
from typing import Any

from app.services.provider_capability_registry import provider_by_id


class RegisteredProviderAdapterError(RuntimeError):
    """Raised when runtime construction is not backed by the provider registry."""


def registered_adapter_paths(provider_id: str) -> tuple[str, ...]:
    registration = _registration(provider_id)
    return tuple(
        dict.fromkeys(
            (
                registration.adapter_path,
                *registration.additional_adapter_paths,
            )
        )
    )


def resolve_registered_adapter(
    provider_id: str,
    *,
    adapter_name: str | None = None,
) -> Any:
    paths = registered_adapter_paths(provider_id)
    if adapter_name is None:
        selected_path = paths[0]
    else:
        matches = [path for path in paths if _adapter_name(path) == adapter_name]
        if len(matches) != 1:
            raise RegisteredProviderAdapterError(
                f"REGISTERED_PROVIDER_ADAPTER_NOT_FOUND:{provider_id}:{adapter_name}"
            )
        selected_path = matches[0]
    return _load_symbol(selected_path)


def create_registered_adapter(
    provider_id: str,
    *args: Any,
    adapter_name: str | None = None,
    **kwargs: Any,
) -> Any:
    adapter = resolve_registered_adapter(
        provider_id,
        adapter_name=adapter_name,
    )
    instance = adapter(*args, **kwargs)
    setattr(instance, "_registry_provider_id", provider_id)
    return instance


def adapter_path_for(value: Any) -> str:
    target = value if isinstance(value, type) else type(value)
    return f"{target.__module__}:{target.__qualname__}"


def _registration(provider_id: str) -> Any:
    try:
        return provider_by_id(provider_id)
    except KeyError as exc:
        raise RegisteredProviderAdapterError(
            f"UNREGISTERED_RUNTIME_PROVIDER:{provider_id}"
        ) from exc


def _adapter_name(path: str) -> str:
    _, separator, qualname = str(path).partition(":")
    if not separator or not qualname:
        raise RegisteredProviderAdapterError(f"INVALID_REGISTERED_ADAPTER_PATH:{path}")
    return qualname.rsplit(".", 1)[-1]


def _load_symbol(path: str) -> Any:
    module_name, separator, qualname = str(path).partition(":")
    if not separator or not module_name or not qualname:
        raise RegisteredProviderAdapterError(f"INVALID_REGISTERED_ADAPTER_PATH:{path}")
    try:
        value: Any = importlib.import_module(module_name)
        for part in qualname.split("."):
            value = getattr(value, part)
    except (AttributeError, ImportError) as exc:
        raise RegisteredProviderAdapterError(
            f"REGISTERED_PROVIDER_ADAPTER_UNIMPORTABLE:{path}"
        ) from exc
    return value


__all__ = [
    "RegisteredProviderAdapterError",
    "adapter_path_for",
    "create_registered_adapter",
    "registered_adapter_paths",
    "resolve_registered_adapter",
]
