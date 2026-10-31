from __future__ import annotations

import pytest

from gateway.config import GatewayConfig, NodeTarget, parse_nodes
from node.config import ArbiterMode, NodeConfig, ReplicationMode
from shared.errors import ConfigError
from witness.config import WitnessConfig


def lookup(values: dict[str, str]):
    return values.get


def test_node_defaults_apply_when_nothing_is_set() -> None:
    config = NodeConfig.from_lookup(lambda _: None)

    assert (config.port, config.node_id, config.data_dir) == (8101, "a", "data/a")
    assert (config.arbiter, config.replication) == (ArbiterMode.WITNESS, ReplicationMode.SYNC)
    assert (config.lease_ttl, config.renew_interval, config.replication_timeout) == (3.0, 0.5, 1.0)
    assert config.lease_safety_margin == pytest.approx(0.3)


def test_node_settings_are_read_from_the_environment() -> None:
    config = NodeConfig.from_lookup(
        lookup(
            {
                "NODE_ID": "b",
                "PORT": "9102",
                "PEER_URL": "http://127.0.0.1:9101",
                "WITNESS_URL": "http://127.0.0.1:9103",
                "ARBITER": "none",
                "REPLICATION": "async",
                "LEASE_TTL_MS": "2000",
                "RENEW_INTERVAL_MS": "200",
                "REPLICATION_TIMEOUT_MS": "400",
                "ELECTION_DELAY_MS": "1500",
            }
        )
    )

    assert config == NodeConfig(
        port=9102,
        node_id="b",
        peer_url="http://127.0.0.1:9101",
        witness_url="http://127.0.0.1:9103",
        data_dir="data/b",
        arbiter=ArbiterMode.NONE,
        replication=ReplicationMode.ASYNC,
        lease_ttl_ms=2000,
        renew_interval_ms=200,
        replication_timeout_ms=400,
        election_delay_ms=1500,
    )
    assert config.election_delay == 1.5


def test_node_from_env_reads_os_environ(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NODE_ID", "c")
    monkeypatch.setenv("DATA_DIR", "/tmp/somewhere")

    config = NodeConfig.from_env()

    assert (config.node_id, config.data_dir) == ("c", "/tmp/somewhere")


@pytest.mark.parametrize(
    ("name", "value", "expected"),
    [
        ("NODE_ID", "Node A", "NODE_ID"),
        ("ARBITER", "zookeeper", "ARBITER"),
        ("REPLICATION", "semi", "REPLICATION"),
        ("PEER_URL", "not a url", "PEER_URL"),
        ("PEER_URL", "ftp://host", "PEER_URL"),
        ("WITNESS_URL", "http://host:99999", "WITNESS_URL"),
        ("RENEW_INTERVAL_MS", "1500", "LEASE_TTL_MS"),
        ("REPLICATION_TIMEOUT_MS", "2000", "REPLICATION_TIMEOUT_MS"),
        ("PORT", "70000", "PORT"),
        ("PORT", "-1", "PORT"),
        ("LEASE_TTL_MS", " 3000", "LEASE_TTL_MS"),
    ],
)
def test_bad_node_settings_name_the_variable_to_fix(name: str, value: str, expected: str) -> None:
    with pytest.raises(ConfigError, match=expected):
        NodeConfig.from_lookup(lambda n: value if n == name else None)


def test_node_config_rejects_a_lease_shorter_than_three_renewals() -> None:
    with pytest.raises(ConfigError, match=r"LEASE_TTL_MS \(1000\) must be at least 3 x RENEW_INTERVAL_MS \(500\)"):
        NodeConfig(lease_ttl_ms=1000, renew_interval_ms=500, replication_timeout_ms=400)


def test_witness_defaults_and_environment() -> None:
    assert WitnessConfig.from_lookup(lambda _: None) == WitnessConfig(8103, "data/witness", 3000, ("a", "b"))
    config = WitnessConfig.from_lookup(
        lookup({"PORT": "9000", "NODES": "x, y", "LEASE_TTL_MS": "1500", "DATA_DIR": "/tmp/w"})
    )
    assert config == WitnessConfig(9000, "/tmp/w", 1500, ("x", "y"))
    assert config.lease_ttl == 1.5


def test_witness_from_env_reads_os_environ(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NODES", "p,q")
    assert WitnessConfig.from_env().nodes == ("p", "q")


@pytest.mark.parametrize("nodes", ["a", "a,a", "a,b,c", "a,B", "a,"])
def test_witness_nodes_must_be_two_distinct_valid_ids(nodes: str) -> None:
    with pytest.raises(ConfigError, match="NODES"):
        WitnessConfig.from_lookup(lambda name: nodes if name == "NODES" else None)


@pytest.mark.parametrize(("name", "value"), [("PORT", "0"), ("LEASE_TTL_MS", "abc")])
def test_witness_bad_numbers_name_the_variable(name: str, value: str) -> None:
    with pytest.raises(ConfigError, match=name):
        WitnessConfig.from_lookup(lambda n: value if n == name else None)


def test_gateway_defaults_point_at_the_two_local_nodes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NODE_URLS", raising=False)
    config = GatewayConfig.from_env()

    assert config.port == 8100
    assert [node.id for node in config.nodes] == ["a", "b"]
    assert config.poll_interval == 0.1


def test_gateway_node_urls_are_parsed() -> None:
    nodes = parse_nodes("x=http://127.0.0.1:9001, y=http://127.0.0.1:9002")

    assert nodes == (NodeTarget("x", "http://127.0.0.1:9001"), NodeTarget("y", "http://127.0.0.1:9002"))
    assert GatewayConfig.from_lookup(lookup({"NODE_URLS": "x=http://h:1"})).nodes == (NodeTarget("x", "http://h:1"),)


@pytest.mark.parametrize(
    "raw", ["a", "a=notaurl", "A=http://127.0.0.1:1", "a=http://127.0.0.1:1,a=http://127.0.0.1:2", ","]
)
def test_bad_gateway_node_urls_are_rejected(raw: str) -> None:
    with pytest.raises(ConfigError, match="NODE_URLS"):
        parse_nodes(raw)
