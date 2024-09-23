from enum import Enum
from pathlib import Path
from typing import Self, TypeAlias

import yaml
from pydantic import BaseModel, Field

from llama_deploy.control_plane.server import ControlPlaneConfig
from llama_deploy.message_queues import (
    AWSMessageQueueConfig,
    KafkaMessageQueueConfig,
    RedisMessageQueueConfig,
    SimpleMessageQueueConfig,
    RabbitMQMessageQueueConfig,
)

MessageQueueConfigType: TypeAlias = (
    AWSMessageQueueConfig
    | KafkaMessageQueueConfig
    | RedisMessageQueueConfig
    | SimpleMessageQueueConfig
    | RabbitMQMessageQueueConfig
)


class MessageQueueType(str, Enum):
    """Supported types of message queues"""

    aws = "aws"
    kafka = "kafka"
    redis = "redis"
    simple = "simple"
    rabbit = "rabbit"


class MessageQueueConfig(BaseModel):
    type: MessageQueueType
    config: MessageQueueConfigType


class SourceType(str, Enum):
    """Supported types for the `Service.source` parameter."""

    git = "git"
    docker = "docker"


class ServiceSource(BaseModel):
    """Configuration for the `source` parameter of a service."""

    type: SourceType
    name: str


class Service(BaseModel):
    """Configuration for a single service."""

    name: str
    source: ServiceSource | None = None
    path: str | None = None
    port: int | None = None
    python_dependencies: list[str] | None = Field(None, alias="python-dependencies")
    ts_dependencies: dict[str, str] | None = Field(None, alias="ts-dependencies")


class Config(BaseModel):
    """Model definition mapping a deployment config file."""

    name: str
    control_plane: ControlPlaneConfig = Field(alias="control-plane")
    message_queue: MessageQueueConfig = Field(alias="message-queue")
    services: dict[str, Service]

    @classmethod
    def from_yaml(cls, path: Path) -> Self:
        """Read config data from a yaml file."""
        with open(path, "r") as yaml_file:
            config = yaml.safe_load(yaml_file) or {}
        return cls(**config)
