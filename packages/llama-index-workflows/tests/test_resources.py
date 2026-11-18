# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Annotated
from unittest import mock

import pytest
from pydantic import BaseModel, Field
from workflows.decorators import step
from workflows.events import Event, StartEvent, StopEvent
from workflows.resource import (
    Resource,
    ResourceConfig,
    ResourceManager,
    _Resource,
    _ResourceConfig,
)
from workflows.workflow import Workflow

# Global counters used in resource workflow tests
cc: int
cc1: int
cc2: int


# Test fixtures for annotation shadowing tests
class _FactoryConfig(BaseModel):
    """Config class defined in test_resources module scope."""

    name: str


def _get_factory_with_config_path(config_path: str) -> _Resource:
    """Returns a Resource that creates a factory using module-scoped _FactoryConfig."""

    def factory(
        config: Annotated[_FactoryConfig, ResourceConfig(config_file=config_path)],
    ) -> dict:
        return {"name": config.name}

    return Resource(factory)


class SecondEvent(Event):
    msg: str = Field(description="A message")


class ThirdEvent(Event):
    msg: str = Field(description="A message")


class ChatMessage(BaseModel):
    @classmethod
    def from_str(cls, role, content):  # noqa: ANN001
        return mock.MagicMock(content=content)


class Memory(mock.MagicMock):
    @classmethod
    def from_defaults(cls, *args, **kwargs):  # noqa: ANN002, ANN003
        return mock.MagicMock()


class MessageStopEvent(StopEvent):
    llm_response: str | None = Field(default=None)


class FileData(BaseModel):
    file: str
    permission_mode: str


class FileOperator:
    def __init__(self, data: FileData) -> None:
        self.file = data.file
        self.permission_mode = data.permission_mode

    def operate(self) -> str:
        if self.permission_mode == "r":
            with open(self.file, self.permission_mode) as f:
                return f.read()
        elif self.permission_mode == "w":
            with open(self.file, self.permission_mode) as f:
                f.write("hello world!")
                return "hello world!"
        else:
            raise ValueError(f"Unsupported operation: {self.permission_mode}")


class ChatMessages(BaseModel):
    messages: list[str]


class Fs(BaseModel):
    files: list[str]
    dirs: list[str]


@pytest.mark.asyncio
async def test_function_resource_init() -> None:
    def get_string() -> str:
        return "string"

    retval = Resource(get_string)
    assert isinstance(retval, _Resource)
    assert "get_string" in retval.name
    assert retval.cache

    resource_manager = ResourceManager()
    result = await retval.call(resource_manager)
    assert result == "string"


def test_resource_config_init(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    data = {"messages": ["hello"]}

    with open("config.json", "w") as f:
        json.dump(data, f)

    retval = ResourceConfig(config_file="config.json")
    assert isinstance(retval, _ResourceConfig)
    assert retval.path_selector is None
    expected_path = str(tmp_path / "config.json")
    assert retval.config_file == expected_path
    assert retval.cls_factory is None
    assert retval.name == "config.json"

    # modify path selector, modify name
    retval.path_selector = "hello.world"
    assert retval.name == "config.json.hello.world"

    retval.path_selector = None

    with pytest.raises(
        ValueError,
        match="Class factory should be set to a BaseModel subclass before calling",
    ):
        retval.call()

    # define a cls_factory for the resource to be called
    retval.cls_factory = ChatMessages

    result = retval.call()
    assert isinstance(result, ChatMessages)
    assert result.messages == ["hello"]


def test_resource_config_path_selector(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    data = {
        "memory": {"messages": ["hello"]},
        "core": {"fs": {"files": ["hello.py"], "dirs": ["hello/"]}},
    }

    with open("config.json", "w") as f:
        json.dump(data, f)

    resource = ResourceConfig(config_file="config.json", path_selector="memory")
    assert resource.name == "config.json.memory"
    resource.cls_factory = ChatMessages
    value = resource.call()
    assert isinstance(value, ChatMessages)
    assert value.messages == ["hello"]
    resource.path_selector = "core.fs"
    assert resource.name == "config.json.core.fs"
    resource.cls_factory = Fs
    value = resource.call()
    assert isinstance(value, Fs)
    assert value.files == ["hello.py"]
    assert value.dirs == ["hello/"]


def test_resource_config_path_selector_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    data = {
        "core": {"fs": {"files": ["hello.py"], "dirs": ["hello/"]}},
    }

    with open("config.json", "w") as f:
        json.dump(data, f)

    # path selector does not return a dict
    resource = ResourceConfig(config_file="config.json", path_selector="core.fs.files")
    resource.cls_factory = Fs
    with pytest.raises(
        ValueError,
        match=r"Expected dictionary for configuration from .+config\.json at path core\.fs\.files, got: .*",
    ):
        resource.call()

    # path selector does not exist
    resource.path_selector = "core.filesystem"
    with pytest.raises(
        ValueError,
        match=r"Expected dictionary for configuration from .+config\.json at path core\.filesystem, got: .*",
    ):
        resource.call()

    # error occurs before reaching the end of the path_selector
    # (tests the not the full path_selector is shown, but only up to the item with the error)
    resource.path_selector = "core.filesystem.fs"
    with pytest.raises(
        ValueError,
        match=r"Expected dictionary for configuration from .+config\.json at path core\.filesystem, got: .*",
    ):
        resource.call()


@pytest.mark.asyncio
async def test_resource() -> None:
    m = Memory.from_defaults("user_id_123", token_limit=60000)

    def get_memory(*args, **kwargs) -> Memory:  # noqa: ANN002, ANN003
        return m

    class TestWorkflow(Workflow):
        @step
        def start_step(self, ev: StartEvent) -> SecondEvent:
            print("Start step is done", flush=True)
            return SecondEvent(msg="Hello")

        @step
        def f1(
            self, ev: SecondEvent, memory: Annotated[Memory, Resource(get_memory)]
        ) -> StopEvent:
            memory.put(ChatMessage.from_str(role="user", content=ev.msg))
            return StopEvent()

    wf = TestWorkflow(disable_validation=True)
    await wf.run()
    m.put.assert_called_once()


@pytest.mark.asyncio
async def test_resource_async() -> None:
    m = Memory.from_defaults("user_id_123", token_limit=60000)

    async def get_memory(*args, **kwargs) -> Memory:  # noqa: ANN002, ANN003
        return m

    class TestWorkflow(Workflow):
        @step
        def start_step(self, ev: StartEvent) -> SecondEvent:
            print("Start step is done", flush=True)
            return SecondEvent(msg="Hello")

        @step
        def f1(
            self,
            ev: SecondEvent,
            history: Annotated[Memory, Resource(get_memory)],
        ) -> StopEvent:
            history.put(ChatMessage.from_str(role="user", content=ev.msg))
            return StopEvent()

    wf = TestWorkflow(disable_validation=True)
    await wf.run()
    m.put.assert_called_once()


@pytest.mark.asyncio
async def test_resource_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    data = {"file": "hello.py", "permission_mode": "r"}

    with open("config.json", "w") as f:
        json.dump(data, f)

    with open("hello.py", "w") as f:
        f.write("print('hello')")

    def get_file_operator(
        config: Annotated[FileData, ResourceConfig(config_file="config.json")],
    ) -> FileOperator:
        return FileOperator(data=config)

    class TestWorkflow(Workflow):
        @step
        def start_step(self, ev: StartEvent) -> SecondEvent:
            print("Start step is done", flush=True)
            return SecondEvent(msg="Hello")

        @step
        def f1(
            self,
            ev: SecondEvent,
            file_operator: Annotated[FileOperator, Resource(get_file_operator)],
        ) -> StopEvent:
            assert file_operator.file == "hello.py"
            assert file_operator.permission_mode == "r"
            assert file_operator.operate() == "print('hello')"
            return StopEvent(result=None)

    wf = TestWorkflow(disable_validation=True)
    await wf.run()


@pytest.mark.asyncio
async def test_caching_behavior() -> None:
    class CounterThing:
        counter = 0

        def incr(self) -> None:
            self.counter += 1

    class StepEvent(Event):
        pass

    def provide_counter_thing() -> CounterThing:
        return CounterThing()

    class TestWorkflow(Workflow):
        @step
        async def test_step(
            self,
            ev: StartEvent,
            counter_thing: Annotated[CounterThing, Resource(provide_counter_thing)],
        ) -> StepEvent:
            counter_thing.incr()
            return StepEvent()

        @step
        async def test_step_2(
            self,
            ev: StepEvent,
            counter_thing: Annotated[CounterThing, Resource(provide_counter_thing)],
        ) -> StopEvent:
            global cc
            counter_thing.incr()
            cc = counter_thing.counter
            return StopEvent()

    wf_1 = TestWorkflow(disable_validation=True)
    await wf_1.run()
    assert (
        cc == 2
    )  # this is expected to be 2, as it is a cached resource shared by test_step and test_step_2, which means at test_step counter_thing.counter goes from 0 to 1 and at test_step_2 goes from 1 to 2

    wf_2 = TestWorkflow(disable_validation=True)
    await wf_2.run()
    assert (
        cc == 2
    )  # the cache is workflow-specific, so since wf_2 is different from wf_1, we expect no interference between the two


@pytest.mark.asyncio
async def test_caching_behavior_resource_configs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    data = {"file": "hello.py", "permission_mode": "r"}
    data_1 = {"file": "bye.py", "permission_mode": "w"}

    with open("config.json", "w") as f:
        json.dump(data, f)

    def get_file_operator(
        config: Annotated[FileData, ResourceConfig(config_file="config.json")],
    ) -> FileOperator:
        return FileOperator(data=config)

    class TestWorkflow(Workflow):
        @step
        def start_step(
            self,
            ev: StartEvent,
            file: Annotated[FileOperator, Resource(get_file_operator)],
        ) -> SecondEvent:
            print("first step")
            assert file.file == "hello.py"
            assert file.permission_mode == "r"
            # change config.json: the underlying resource has been cached, so will be unaffected
            with open("config.json", "w") as f:
                json.dump(data_1, f)
            return SecondEvent(msg="Hello")

        @step
        def f1(
            self,
            ev: SecondEvent,
            file: Annotated[FileOperator, Resource(get_file_operator)],
        ) -> StopEvent:
            print("second step")
            # this resource has been cached,
            # so even if config.json has changed,
            # the resource remains the same
            assert file.file == "hello.py"
            assert file.permission_mode == "r"
            return StopEvent()

    wf = TestWorkflow()
    await wf.run()


@pytest.mark.asyncio
async def test_non_caching_behavior() -> None:
    class CounterThing:
        counter = 0

        def incr(self) -> None:
            self.counter += 1

    class StepEvent(Event):
        pass

    def provide_counter_thing() -> CounterThing:
        return CounterThing()

    class TestWorkflow(Workflow):
        @step
        async def test_step(
            self,
            ev: StartEvent,
            counter_thing: Annotated[CounterThing, Resource(provide_counter_thing)],
        ) -> StepEvent:
            global cc1
            counter_thing.incr()
            cc1 = counter_thing.counter
            return StepEvent()

        @step
        async def test_step_2(
            self,
            ev: StepEvent,
            counter_thing: Annotated[
                CounterThing, Resource(provide_counter_thing, cache=False)
            ],
        ) -> StopEvent:
            global cc2
            counter_thing.incr()
            cc2 = counter_thing.counter
            return StopEvent()

    wf_1 = TestWorkflow(disable_validation=True)
    await wf_1.run()
    assert cc1 == 1
    assert cc2 == 1


@pytest.mark.asyncio
async def test_resource_manager() -> None:
    m = ResourceManager()
    await m.set("test_resource", 42)
    assert m.get_all() == {"test_resource": 42}


@pytest.mark.asyncio
async def test_recursive_resource_injection() -> None:
    """Test that a Resource can depend on another Resource."""

    class DBConnection:
        def __init__(self, host: str):
            self.host = host

    class Repository:
        def __init__(self, db: DBConnection):
            self.db = db

    def get_db_connection() -> DBConnection:
        return DBConnection(host="localhost")

    def get_repository(
        db: Annotated[DBConnection, Resource(get_db_connection)],
    ) -> Repository:
        return Repository(db)

    class TestWorkflow(Workflow):
        @step
        def start_step(self, ev: StartEvent) -> SecondEvent:
            return SecondEvent(msg="Hello")

        @step
        def use_repo(
            self,
            ev: SecondEvent,
            repo: Annotated[Repository, Resource(get_repository)],
        ) -> StopEvent:
            assert repo.db.host == "localhost"
            return StopEvent()

    wf = TestWorkflow(disable_validation=True)
    await wf.run()


@pytest.mark.asyncio
async def test_recursive_resource_caching() -> None:
    """Test that nested resources respect individual cache settings."""
    call_counts = {"db": 0, "repo": 0}

    class DBConnection:
        pass

    class Repository:
        def __init__(self, db: DBConnection):
            self.db = db

    def get_db_connection() -> DBConnection:
        call_counts["db"] += 1
        return DBConnection()

    def get_repository(
        db: Annotated[DBConnection, Resource(get_db_connection)],
    ) -> Repository:
        call_counts["repo"] += 1
        return Repository(db)

    class StepEvent(Event):
        pass

    class TestWorkflow(Workflow):
        @step
        def step1(
            self,
            ev: StartEvent,
            repo: Annotated[Repository, Resource(get_repository)],
        ) -> StepEvent:
            return StepEvent()

        @step
        def step2(
            self,
            ev: StepEvent,
            repo: Annotated[Repository, Resource(get_repository)],
        ) -> StopEvent:
            return StopEvent()

    wf = TestWorkflow(disable_validation=True)
    await wf.run()

    # Both should be cached (called once each)
    assert call_counts["db"] == 1
    assert call_counts["repo"] == 1


@pytest.mark.asyncio
async def test_circular_resource_dependency_detection() -> None:
    """Test that circular dependencies are detected at runtime."""

    class A:
        pass

    class B:
        pass

    # Create the cycle by modifying __annotations__ after creating the resources
    # This allows us to create mutual dependencies

    def cyclic_factory_a(b: Annotated[B, "placeholder"]) -> A:
        return A()

    def cyclic_factory_b(a: Annotated[A, "placeholder"]) -> B:
        return B()

    # Create resources
    cyclic_res_a = Resource(cyclic_factory_a)
    cyclic_res_b = Resource(cyclic_factory_b)

    # Modify annotations to create the cycle:
    # cyclic_res_a depends on cyclic_res_b, and cyclic_res_b depends on cyclic_res_a
    cyclic_factory_a.__annotations__["b"] = Annotated[B, cyclic_res_b]
    cyclic_factory_b.__annotations__["a"] = Annotated[A, cyclic_res_a]

    class TestWorkflow(Workflow):
        @step
        def start_step(
            self,
            ev: StartEvent,
            a: Annotated[A, cyclic_res_a],
        ) -> StopEvent:
            return StopEvent()

    wf = TestWorkflow(disable_validation=True)
    expected_chain = (
        f"{cyclic_factory_a.__qualname__} -> "
        f"{cyclic_factory_b.__qualname__} -> "
        f"{cyclic_factory_a.__qualname__}"
    )
    with pytest.raises(ValueError, match=re.escape(expected_chain)):
        await wf.run()


@pytest.mark.asyncio
async def test_non_cached_resource_single_resolution_cycle() -> None:
    """Non-cached resources should resolve once per dependency graph."""
    call_counts = {"d": 0}

    class D:
        pass

    def get_d() -> D:
        call_counts["d"] += 1
        return D()

    def get_b(d: Annotated[D, Resource(get_d, cache=False)]) -> str:
        return "b"

    def get_c(d: Annotated[D, Resource(get_d, cache=False)]) -> str:
        return "c"

    class TestWorkflow(Workflow):
        @step
        def start_step(
            self,
            ev: StartEvent,
            b: Annotated[str, Resource(get_b)],
            c: Annotated[str, Resource(get_c)],
        ) -> StopEvent:
            assert b == "b"
            assert c == "c"
            return StopEvent()

    wf = TestWorkflow(disable_validation=True)
    await wf.run()
    assert call_counts["d"] == 1


@pytest.mark.asyncio
async def test_resource_config_in_step_signature(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that ResourceConfig can be used directly in step signatures."""
    monkeypatch.chdir(tmp_path)

    data = {"file": "test.txt", "permission_mode": "r"}
    with open("config.json", "w") as f:
        json.dump(data, f)

    class TestWorkflow(Workflow):
        @step
        def start_step(self, ev: StartEvent) -> SecondEvent:
            return SecondEvent(msg="Hello")

        @step
        def use_config(
            self,
            ev: SecondEvent,
            config: Annotated[FileData, ResourceConfig(config_file="config.json")],
        ) -> StopEvent:
            assert config.file == "test.txt"
            assert config.permission_mode == "r"
            return StopEvent()

    wf = TestWorkflow(disable_validation=True)
    await wf.run()


@pytest.mark.asyncio
async def test_resource_config_in_step_with_path_selector(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test ResourceConfig with path_selector in step signatures."""
    monkeypatch.chdir(tmp_path)

    data = {
        "database": {"file": "db.sqlite", "permission_mode": "rw"},
        "cache": {"file": "cache.json", "permission_mode": "r"},
    }
    with open("config.json", "w") as f:
        json.dump(data, f)

    class TestWorkflow(Workflow):
        @step
        def use_db_config(
            self,
            ev: StartEvent,
            db_config: Annotated[
                FileData,
                ResourceConfig(config_file="config.json", path_selector="database"),
            ],
        ) -> StopEvent:
            assert db_config.file == "db.sqlite"
            assert db_config.permission_mode == "rw"
            return StopEvent()

    wf = TestWorkflow(disable_validation=True)
    await wf.run()


# Tests for resource validation during workflow.validate()


def test_validate_detects_circular_resource_dependency() -> None:
    """Test that validate() detects circular resource dependencies."""

    class A:
        pass

    class B:
        pass

    def cyclic_factory_a(b: Annotated[B, "placeholder"]) -> A:
        return A()

    def cyclic_factory_b(a: Annotated[A, "placeholder"]) -> B:
        return B()

    cyclic_res_a = Resource(cyclic_factory_a)
    cyclic_res_b = Resource(cyclic_factory_b)

    # Create circular dependency
    cyclic_factory_a.__annotations__["b"] = Annotated[B, cyclic_res_b]
    cyclic_factory_b.__annotations__["a"] = Annotated[A, cyclic_res_a]

    class TestWorkflow(Workflow):
        @step
        def start_step(
            self,
            ev: StartEvent,
            a: Annotated[A, cyclic_res_a],
        ) -> StopEvent:
            return StopEvent()

    wf = TestWorkflow(disable_validation=True)
    # Circular deps are caught at resolution time by ResourceManager
    with pytest.raises(Exception, match=r"Circular resource dependency detected"):
        wf.validate(validate_resources=True)


def test_validate_resource_config_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that validate() succeeds with valid resource config."""
    monkeypatch.chdir(tmp_path)

    data = {"file": "test.txt", "permission_mode": "r"}
    with open("config.json", "w") as f:
        json.dump(data, f)

    class TestWorkflow(Workflow):
        @step
        def start_step(
            self,
            ev: StartEvent,
            config: Annotated[FileData, ResourceConfig(config_file="config.json")],
        ) -> StopEvent:
            return StopEvent()

    wf = TestWorkflow(disable_validation=True)
    # Should not raise
    wf.validate(validate_resource_configs=True, validate_resources=False)


def test_validate_resource_config_invalid_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that validate() fails with invalid resource config data."""
    monkeypatch.chdir(tmp_path)

    # Missing required 'permission_mode' field
    data = {"file": "test.txt"}
    with open("config.json", "w") as f:
        json.dump(data, f)

    class TestWorkflow(Workflow):
        @step
        def start_step(
            self,
            ev: StartEvent,
            config: Annotated[FileData, ResourceConfig(config_file="config.json")],
        ) -> StopEvent:
            return StopEvent()

    wf = TestWorkflow(disable_validation=True)
    with pytest.raises(
        Exception,
        match=r"(?s)step 'start_step', parameter 'config'.*permission_mode",
    ):
        wf.validate(validate_resource_configs=True, validate_resources=False)


def test_validate_resource_config_disabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that validate() skips resource config validation when disabled."""
    monkeypatch.chdir(tmp_path)

    # Invalid data but should be ignored since validation is disabled
    data = {"file": "test.txt"}  # Missing permission_mode
    with open("config.json", "w") as f:
        json.dump(data, f)

    class TestWorkflow(Workflow):
        @step
        def start_step(
            self,
            ev: StartEvent,
            config: Annotated[FileData, ResourceConfig(config_file="config.json")],
        ) -> StopEvent:
            return StopEvent()

    wf = TestWorkflow(disable_validation=True)
    # Should not raise because resource config validation is disabled
    wf.validate(validate_resource_configs=False, validate_resources=False)


def test_validate_nested_resource_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that validate() validates nested resource configs."""
    monkeypatch.chdir(tmp_path)

    data = {"file": "test.txt", "permission_mode": "r"}
    with open("config.json", "w") as f:
        json.dump(data, f)

    def get_file_operator(
        config: Annotated[FileData, ResourceConfig(config_file="config.json")],
    ) -> FileOperator:
        return FileOperator(data=config)

    class TestWorkflow(Workflow):
        @step
        def start_step(
            self,
            ev: StartEvent,
            operator: Annotated[FileOperator, Resource(get_file_operator)],
        ) -> StopEvent:
            return StopEvent()

    wf = TestWorkflow(disable_validation=True)
    # Should validate the nested ResourceConfig
    wf.validate(validate_resource_configs=True, validate_resources=False)


def test_validate_nested_resource_config_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that validate() fails with invalid nested resource config."""
    monkeypatch.chdir(tmp_path)

    # Invalid data - missing required field
    data = {"file": "test.txt"}
    with open("config.json", "w") as f:
        json.dump(data, f)

    def get_file_operator(
        config: Annotated[FileData, ResourceConfig(config_file="config.json")],
    ) -> FileOperator:
        return FileOperator(data=config)

    class TestWorkflow(Workflow):
        @step
        def start_step(
            self,
            ev: StartEvent,
            operator: Annotated[FileOperator, Resource(get_file_operator)],
        ) -> StopEvent:
            return StopEvent()

    wf = TestWorkflow(disable_validation=True)
    with pytest.raises(
        Exception,
        match=r"(?s)step 'start_step', parameter 'operator'.*get_file_operator.*config\.json.*permission_mode",
    ):
        wf.validate(validate_resource_configs=True, validate_resources=False)


def test_validate_resources_enabled() -> None:
    """Test that validate() resolves resources when enabled."""
    call_count = {"count": 0}

    def get_string() -> str:
        call_count["count"] += 1
        return "test"

    class TestWorkflow(Workflow):
        @step
        def start_step(
            self,
            ev: StartEvent,
            value: Annotated[str, Resource(get_string)],
        ) -> StopEvent:
            return StopEvent()

    wf = TestWorkflow(disable_validation=True)
    # Resource factory should be called during validation
    wf.validate(validate_resource_configs=True, validate_resources=True)
    assert call_count["count"] == 1


def test_validate_resources_disabled_by_default() -> None:
    """Test that resource factories are not resolved by default."""
    call_count = {"count": 0}

    def get_string() -> str:
        call_count["count"] += 1
        return "test"

    class TestWorkflow(Workflow):
        @step
        def start_step(
            self,
            ev: StartEvent,
            value: Annotated[str, Resource(get_string)],
        ) -> StopEvent:
            return StopEvent()

    wf = TestWorkflow(disable_validation=True)
    # Resource factory should NOT be called during validation (default)
    wf.validate()  # Uses defaults: validate_resource_configs=True, validate_resources=False
    assert call_count["count"] == 0


def test_validate_resource_factory_failure() -> None:
    """Test that validate() reports resource factory failures."""

    def failing_factory() -> str:
        raise RuntimeError("Factory failed!")

    class TestWorkflow(Workflow):
        @step
        def start_step(
            self,
            ev: StartEvent,
            value: Annotated[str, Resource(failing_factory)],
        ) -> StopEvent:
            return StopEvent()

    wf = TestWorkflow(disable_validation=True)
    with pytest.raises(
        Exception, match=r"step 'start_step', parameter 'value'.*Factory failed"
    ):
        wf.validate(validate_resources=True)


def test_validate_annotation_shadowing_with_resource_factory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that validation works with factory annotation shadowing."""
    monkeypatch.chdir(tmp_path)

    data = {"name": "test_name"}
    with open("config.json", "w") as f:
        json.dump(data, f)

    # Use the module-scoped helper that tests annotation shadowing
    factory_resource = _get_factory_with_config_path(str(tmp_path / "config.json"))

    class TestWorkflow(Workflow):
        @step
        def start_step(
            self,
            ev: StartEvent,
            result: Annotated[dict, factory_resource],
        ) -> StopEvent:
            return StopEvent()

    wf = TestWorkflow(disable_validation=True)
    # Should validate the nested ResourceConfig in the factory
    wf.validate(validate_resource_configs=True)


def test_resource_config_deferred_file_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that ResourceConfig can be declared before the config file exists."""
    monkeypatch.chdir(tmp_path)

    # Declare ResourceConfig BEFORE the file exists - this should NOT raise
    resource = ResourceConfig(config_file="config.json")
    assert resource.name == "config.json"

    # Accessing config_file property should raise because file doesn't exist yet
    with pytest.raises(FileNotFoundError, match="No such file: config.json"):
        _ = resource.config_file

    # calling should also raise
    resource.cls_factory = ChatMessages
    with pytest.raises(FileNotFoundError, match="No such file: config.json"):
        resource.call()

    # Now create the file
    data = {"messages": ["hello"]}
    with open("config.json", "w") as f:
        json.dump(data, f)

    # Now it should work
    result = resource.call()
    assert isinstance(result, ChatMessages)
    assert result.messages == ["hello"]


@pytest.mark.asyncio
async def test_resource_config_deferred_in_workflow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that workflow can be defined before config file exists."""
    monkeypatch.chdir(tmp_path)

    # Define workflow with ResourceConfig pointing to non-existent file
    # This should NOT raise at definition time
    class TestWorkflow(Workflow):
        @step
        def start_step(
            self,
            ev: StartEvent,
            config: Annotated[FileData, ResourceConfig(config_file="config.json")],
        ) -> StopEvent:
            assert config.file == "test.txt"
            return StopEvent()

    # Workflow can be instantiated even though file doesn't exist
    wf = TestWorkflow(disable_validation=True)

    # Running should fail because file doesn't exist
    with pytest.raises(FileNotFoundError, match="No such file: config.json"):
        await wf.run()

    # Now create the file
    data = {"file": "test.txt", "permission_mode": "r"}
    with open("config.json", "w") as f:
        json.dump(data, f)

    # Now it should work
    await wf.run()
