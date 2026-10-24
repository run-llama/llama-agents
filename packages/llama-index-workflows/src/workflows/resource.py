# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import functools
import inspect
import json
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Annotated,
    Any,
    Awaitable,
    Callable,
    Generic,
    Iterator,
    Protocol,
    TypeVar,
    cast,
    get_args,
    get_origin,
    get_type_hints,
    runtime_checkable,
)

from pydantic import (
    BaseModel,
    ConfigDict,
)

T = TypeVar("T")
B = TypeVar("B", bound=BaseModel)


def _get_factory_type_hints(
    factory: Callable[..., Any],
    localns: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve type hints for a factory function, avoiding shadowing.

    Filters localns to exclude names that exist in factory's __globals__,
    so types resolve from the factory's module while allowing closure variables.
    """
    filtered_localns = localns
    if filtered_localns:
        globalns = getattr(factory, "__globals__", {})
        filtered_localns = {
            k: v for k, v in filtered_localns.items() if k not in globalns
        }

    try:
        return get_type_hints(factory, include_extras=True, localns=filtered_localns)
    except NameError:
        return {}


@runtime_checkable
class ResourceDescriptor(Protocol):
    """Common interface for resource descriptors.

    Both _Resource and _ResourceConfig implement this protocol, allowing
    unified resolution through ResourceManager without isinstance checks.
    """

    @property
    def name(self) -> str:
        """Unique identifier for caching and cycle detection."""
        ...

    @property
    def cache(self) -> bool:
        """Whether to cache the resolved value."""
        ...

    async def resolve(self, manager: ResourceManager) -> Any:
        """Resolve the resource, returning the concrete value."""
        ...

    def set_type_annotation(self, type_annotation: Any) -> None:
        """Provide the annotated type for config-backed resources."""
        ...

    def set_localns(self, localns: dict[str, Any] | None) -> None:
        """Store local namespace for resolving deferred type annotations."""
        ...

    def get_dependencies(self) -> list[tuple[str, ResourceDescriptor, type | None]]:
        """Return factory dependencies. Empty for non-factory resources."""
        ...


class _Resource(Generic[T]):
    """Internal wrapper for resource factories.

    Wraps sync/async factories and records metadata such as the qualified name
    and cache behavior.
    """

    def __init__(self, factory: Callable[..., T | Awaitable[T]], cache: bool) -> None:
        self._factory = factory
        self._is_async = inspect.iscoroutinefunction(factory)
        self.name = getattr(factory, "__qualname__", type(factory).__name__)
        self.cache = cache
        self._localns: dict[str, Any] | None = None

    async def _resolve_dependencies(
        self, resource_manager: ResourceManager
    ) -> dict[str, Any]:
        """Resolve annotated ResourceDescriptor dependencies."""
        resolved: dict[str, Any] = {}

        for param_name, descriptor, type_annotation in self.get_dependencies():
            descriptor.set_type_annotation(type_annotation)
            descriptor.set_localns(self._localns)
            resolved[param_name] = await resource_manager.get(descriptor)

        return resolved

    async def call(self, resource_manager: ResourceManager) -> T:
        """Invoke the underlying factory, awaiting if necessary."""
        args = await self._resolve_dependencies(resource_manager)
        if self._is_async:
            result = await cast(Callable[..., Awaitable[T]], self._factory)(**args)
        else:
            result = cast(Callable[..., T], self._factory)(**args)
        return result

    async def resolve(self, manager: ResourceManager) -> T:
        """Resolve the resource via the manager.

        Implements ResourceDescriptor protocol.
        """
        return await self.call(manager)

    def set_type_annotation(self, type_annotation: Any) -> None:
        """No-op for factory-backed resources."""
        return None

    def set_localns(self, localns: dict[str, Any] | None) -> None:
        """Store local namespace for resolving deferred type annotations."""
        self._localns = localns

    def get_dependencies(self) -> list[tuple[str, ResourceDescriptor, type | None]]:
        """Extract ResourceDescriptor dependencies from factory signature."""
        deps: list[tuple[str, ResourceDescriptor, type | None]] = []
        params = inspect.signature(self._factory).parameters
        type_hints = _get_factory_type_hints(self._factory, self._localns)

        for param in params.values():
            annotation = type_hints.get(param.name, param.annotation)
            if get_origin(annotation) is Annotated:
                args = get_args(annotation)
                if len(args) >= 2:
                    descriptor = args[1]
                    if isinstance(descriptor, ResourceDescriptor):
                        type_annotation = args[0]
                        deps.append((param.name, descriptor, type_annotation))
        return deps


def _get_resource_config_data(
    config_file: str,
    path_selector: str | None,
) -> dict[str, Any]:
    # Resolve to absolute path for cache key to handle different working directories
    abs_path = str(Path(config_file).resolve())
    return _get_resource_config_data_cached(abs_path, path_selector)


@functools.lru_cache(maxsize=128)
def _get_resource_config_data_cached(
    config_file: str,
    path_selector: str | None,
) -> dict[str, Any]:
    with open(config_file, "r") as f:
        data = json.load(f)
    if path_selector is not None:
        keys = path_selector.split(".")
        val: dict[str, Any] = data
        cumulative_path = ""
        for key in keys:
            cumulative_path += key + "."
            got = cast(dict[str, Any] | None, val.get(key))
            if not isinstance(got, dict):
                raise ValueError(
                    f"Expected dictionary for configuration from {config_file} at path {cumulative_path.strip('.')}, got: {type(got)}"
                )
            val = got
        return val
    return data


class _ResourceConfig(Generic[B]):
    """
    Internal wrapper for a pydantic-based resource whose configuration can be read from a JSON file.
    """

    _original_config_file: str
    _resolved_config_file: str | None
    path_selector: str | None
    cls_factory: type[B] | None
    label: str | None
    description: str | None

    def __init__(
        self,
        config_file: str,
        path_selector: str | None,
        cls_factory: type[B] | None = None,
        label: str | None = None,
        description: str | None = None,
    ) -> None:
        config_path = Path(config_file)
        if config_path.suffix != ".json":
            raise ValueError(
                "Only JSON files can be used to load Pydantic-based resources."
            )
        # Resolved lazily in config_file property
        self._original_config_file = config_file
        self._resolved_config_file = None
        self.path_selector = path_selector
        self.cls_factory = cls_factory
        self.label = label
        self.description = description

    @property
    def config_file(self) -> str:
        """Return the resolved absolute path, validated on first access."""
        if self._resolved_config_file is None:
            config_path = Path(self._original_config_file)
            if not config_path.is_file():
                raise FileNotFoundError(f"No such file: {self._original_config_file}")
            self._resolved_config_file = str(config_path.resolve())
        return self._resolved_config_file

    @property
    def name(self) -> str:
        base_name = self._original_config_file
        if self.path_selector is not None:
            return base_name + "." + self.path_selector
        return base_name

    @property
    def cache(self) -> bool:
        """ResourceConfig instances are always cached."""
        return True

    def call(self) -> B:
        sel_data = _get_resource_config_data(
            config_file=self.config_file, path_selector=self.path_selector
        )
        # let validation error bubble up
        if self.cls_factory is not None:
            return self.cls_factory.model_validate(sel_data)
        else:
            raise ValueError(
                "Class factory should be set to a BaseModel subclass before calling"
            )

    async def resolve(self, manager: ResourceManager) -> B:
        """Resolve the config resource.

        Implements ResourceDescriptor protocol.
        Note: cls_factory must be set before calling this method.
        """
        return self.call()

    def set_type_annotation(self, type_annotation: Any) -> None:
        """Assign the annotated class for config-backed resources when missing."""
        if self.cls_factory is None:
            self.cls_factory = cast(type[B], type_annotation)

    def set_localns(self, localns: dict[str, Any] | None) -> None:
        """No-op for config-backed resources."""
        pass

    def get_dependencies(self) -> list[tuple[str, ResourceDescriptor, type | None]]:
        """No dependencies for config-backed resources."""
        return []


def ResourceConfig(
    config_file: str,
    path_selector: str | None = None,
    label: str | None = None,
    description: str | None = None,
) -> _ResourceConfig:
    """
    Create a config-backed resource that loads a Pydantic model from a JSON file.

    Args:
        config_file: JSON file where the configuration is stored.
        path_selector: Path selector to retrieve a specific value from the JSON map.
        label: Human-friendly short name for display in visualizations.
        description: Longer description explaining the purpose and contents of this config.

    Returns:
        _ResourceConfig: A configured resource representation.

    Example:
        ```python
        from typing import Annotated
        from pydantic import BaseModel
        from workflows import Workflow, step
        from workflows.events import StartEvent, StopEvent
        from workflows.resource import ResourceConfig


        class ClassifierConfig(BaseModel):
            categories: list[str]
            threshold: float


        class MyWorkflow(Workflow):
            @step
            async def classify(
                self,
                ev: StartEvent,
                config: Annotated[
                    ClassifierConfig,
                    ResourceConfig(
                        config_file="classifier.json",
                        label="Classifier",
                        description="Classification categories and threshold",
                    ),
                ],
            ) -> StopEvent:
                return StopEvent(result=config.categories)
        ```
    """
    return _ResourceConfig(
        config_file=config_file,
        path_selector=path_selector,
        label=label,
        description=description,
    )


class ResourceDefinition(BaseModel):
    """Definition for a resource injection requested by a step signature.

    Attributes:
        name (str): Parameter name in the step function.
        resource (ResourceDescriptor): Descriptor used to produce the dependency.
        type_annotation (type | None): The type annotation from Annotated[T, ...].
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)
    name: str
    resource: ResourceDescriptor
    type_annotation: Any = None


def Resource(
    factory: Callable[..., T],
    cache: bool = True,
) -> _Resource:
    """Declare a resource to inject into step functions.

    Args:
        factory (Callable[..., T] | None): Function returning the resource instance. May be async.
        cache (bool): If True, reuse the produced resource across steps. Defaults to True.

    Returns:
        _Resource[T]: A resource descriptor to be used in `typing.Annotated`.

    Examples:
        ```python
        from typing import Annotated
        from workflows.resource import Resource

        def get_memory(**kwargs) -> Memory:
            return Memory.from_defaults("user123", token_limit=60000)

        class MyWorkflow(Workflow):
            @step
            async def first(
                self,
                ev: StartEvent,
                memory: Annotated[Memory, Resource(get_memory)],
            ) -> StopEvent:
                await memory.aput(...)
                return StopEvent(result="ok")
        ```
    """
    return _Resource(factory, cache)


@dataclass
class _ResourceResolution:
    depth: int = 0
    resolving: list[str] = field(default_factory=list)
    cache: dict[str, Any] = field(default_factory=dict)


@dataclass
class _ResourceLock:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    owner: asyncio.Task[Any] | None = None
    users: int = 0


class ResourceManager:
    """Manage resource lifecycles and caching across workflow steps.

    Methods:
        set: Manually set a resource by name.
        get: Produce or retrieve a resource via its descriptor.
        get_all: Return the internal name->resource map.
    """

    def __init__(self) -> None:
        self.resources: dict[str, Any] = {}
        self._resolution_scopes: dict[
            asyncio.Task[Any] | None, _ResourceResolution
        ] = {}
        self._resource_locks: dict[str, _ResourceLock] = {}
        self._waiting: dict[asyncio.Task[Any], str] = {}

    @contextmanager
    def resolution_scope(self) -> Iterator[None]:
        """Scope non-cached resolution values to a single dependency graph."""
        try:
            task = asyncio.current_task()
        except RuntimeError:
            task = None
        scope = self._resolution_scopes.setdefault(task, _ResourceResolution())
        scope.depth += 1
        try:
            yield
        finally:
            scope.depth -= 1
            if scope.depth == 0:
                del self._resolution_scopes[task]

    async def set(self, name: str, val: Any) -> None:
        """Register a resource instance under a name."""
        self.resources.update({name: val})

    async def get(self, resource: ResourceDescriptor) -> Any:
        with self.resolution_scope():
            return await self._get(
                resource, self._resolution_scopes[asyncio.current_task()]
            )

    async def _get(
        self, resource: ResourceDescriptor, scope: _ResourceResolution
    ) -> Any:
        """Return a resource instance, honoring cache settings.

        Works with any ResourceDescriptor implementation (_Resource or _ResourceConfig).
        """
        # Cycle detection
        if resource.name in scope.resolving:
            chain = " -> ".join(scope.resolving) + f" -> {resource.name}"
            raise ValueError(f"Circular resource dependency detected: {chain}")

        # Check cache first (before marking as resolving)
        if resource.cache and resource.name in self.resources:
            return self.resources[resource.name]
        if resource.name in scope.cache:
            return scope.cache[resource.name]

        if resource.cache:
            return await self._get_cached(resource, scope)
        return await self._resolve(resource, scope)

    async def _get_cached(
        self, resource: ResourceDescriptor, scope: _ResourceResolution
    ) -> Any:
        task = asyncio.current_task()
        assert task is not None
        entry = self._resource_locks.setdefault(resource.name, _ResourceLock())
        entry.users += 1
        try:
            self._waiting[task] = resource.name
            try:
                # A cycle can span workers that each own a different resource.
                chain = [resource.name]
                owner = entry.owner
                while owner is not None:
                    if owner is task:
                        chain.append(resource.name)
                        raise ValueError(
                            f"Circular resource dependency detected: {' -> '.join(chain)}"
                        )
                    waiting = self._waiting.get(owner)
                    if waiting is None:
                        break
                    chain.append(waiting)
                    owner = self._resource_locks[waiting].owner
                await entry.lock.acquire()
            finally:
                self._waiting.pop(task, None)

            entry.owner = task
            try:
                if resource.name in self.resources:
                    return self.resources[resource.name]
                return await self._resolve(resource, scope)
            finally:
                entry.owner = None
                entry.lock.release()
        finally:
            entry.users -= 1
            if entry.users == 0:
                del self._resource_locks[resource.name]

    async def _resolve(
        self, resource: ResourceDescriptor, scope: _ResourceResolution
    ) -> Any:
        scope.resolving.append(resource.name)
        try:
            val = await resource.resolve(self)
            if resource.cache:
                await self.set(resource.name, val)
            scope.cache[resource.name] = val
            return val
        finally:
            scope.resolving.pop()

    def get_all(self) -> dict[str, Any]:
        """Return all materialized resources."""
        return self.resources
