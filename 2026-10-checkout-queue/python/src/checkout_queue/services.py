"""Everything one sale needs, wired once per app: the Python stand-in for the .NET service collection."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from redis.asyncio import Redis

from .admin import AuditStore, TicketBoardStore, sale_settings
from .clock import Clock
from .config import Config
from .maintenance import SaleMaintenance
from .orders import OrderStore
from .push import TicketPushWorker
from .sale_redis import SaleRedis
from .streams import TicketStreams
from .tickets import QueueStore


@dataclass(frozen=True)
class Services:
    config: Config
    clock: Clock
    redis: SaleRedis
    queue: QueueStore
    orders: OrderStore
    audit: AuditStore
    board: TicketBoardStore
    maintenance: SaleMaintenance
    streams: TicketStreams
    push: TicketPushWorker
    settings: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def build(cls, config: Config, client: Redis, clock: Clock) -> Services:
        config.validate()
        redis = SaleRedis(client, config, clock)
        queue = QueueStore(redis, config)
        orders = OrderStore(redis, config)
        streams = TicketStreams()
        return cls(
            config=config,
            clock=clock,
            redis=redis,
            queue=queue,
            orders=orders,
            audit=AuditStore(redis),
            board=TicketBoardStore(redis, config),
            maintenance=SaleMaintenance(queue, orders),
            streams=streams,
            push=TicketPushWorker(redis, queue, streams, config, clock),
            settings=sale_settings(config),
        )
