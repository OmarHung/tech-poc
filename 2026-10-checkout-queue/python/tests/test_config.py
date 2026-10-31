import pytest

from checkout_queue.config import Config, ConfigError
from checkout_queue.ids import is_valid_id, new_id


def test_from_lookup_uses_defaults_when_nothing_is_set() -> None:
    assert Config.from_lookup(lambda _: None) == Config()


def test_from_lookup_reads_every_variable() -> None:
    env = {
        "PORT": "9001",
        "REDIS_URL": "redis.local:6380",
        "INSTANCE_NAME": "node-a",
        "SALE_ID": "black-friday",
        "INITIAL_STOCK": "500",
        "MAX_PER_ORDER": "3",
        "MAX_ACTIVE": "80",
        "ADMISSION_WINDOW_MS": "30000",
        "PAYMENT_WINDOW_MS": "60000",
        "HEARTBEAT_TIMEOUT_MS": "9000",
        "POLL_INTERVAL_MS": "2000",
        "TICK_MS": "50",
    }

    config = Config.from_lookup(env.get)

    assert config == Config(
        port=9001,
        redis_url="redis.local:6380",
        instance_name="node-a",
        sale_id="black-friday",
        initial_stock=500,
        max_per_order=3,
        max_active=80,
        admission_window_ms=30_000,
        payment_window_ms=60_000,
        heartbeat_timeout_ms=9_000,
        poll_interval_ms=2_000,
        tick_ms=50,
    )


def test_from_env_reads_the_process_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MAX_ACTIVE", "7")

    assert Config.from_env().max_active == 7


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("PORT", "0"),
        ("PORT", "http"),
        ("PORT", "65536"),
        ("PORT", " 8080"),
        ("PORT", "+8080"),
        ("PORT", "８０８０"),  # full-width digits: int() would take them, the contract does not
        ("PORT", "9" * 5000),  # past int()'s digit limit: must still name the variable
        ("INITIAL_STOCK", "-1"),
        ("INITIAL_STOCK", "10000001"),
        ("MAX_PER_ORDER", "0"),
        ("MAX_ACTIVE", "0"),
        ("ADMISSION_WINDOW_MS", "0"),
        ("ADMISSION_WINDOW_MS", "86400001"),
        ("PAYMENT_WINDOW_MS", "1.5"),
        ("TICK_MS", "0"),
        ("TICK_MS", ""),
        ("SALE_ID", "has space"),
        ("SALE_ID", "brace}breaks{the-hash-tag"),
        ("SALE_ID", "demo\n"),
        ("SALE_ID", ""),
        ("REDIS_URL", " "),
        ("HEARTBEAT_TIMEOUT_MS", "2999"),
    ],
)
def test_from_lookup_rejects_invalid_values(name: str, value: str) -> None:
    with pytest.raises(ConfigError) as error:
        Config.from_lookup(lambda n: value if n == name else None)

    assert name in str(error.value)


def test_ids_are_random_and_validate_their_own_format() -> None:
    ids = [new_id() for _ in range(100)]

    assert len(set(ids)) == 100
    assert all(is_valid_id(i) for i in ids)
    assert not is_valid_id(ids[0].upper())
    assert not is_valid_id(ids[0][:31])
    assert not is_valid_id(ids[0] + "\n")
    assert not is_valid_id(None)
    assert not is_valid_id(42)
