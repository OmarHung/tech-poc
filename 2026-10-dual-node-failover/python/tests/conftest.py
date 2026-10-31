from __future__ import annotations

import pytest

from helpers import FakeClock


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()
