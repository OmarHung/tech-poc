from __future__ import annotations

import pytest

from shared import state_file, wire
from shared.wire import JsonObject, WireError


def test_fields_match_case_insensitively_and_unknown_ones_are_ignored() -> None:
    fields = JsonObject({"Value": "v", "EPOCH": 3, "extra": [1]})

    assert fields.string("value") == "v"
    assert fields.integer("epoch") == 3
    assert fields.integer("missing", default=7) == 7
    assert fields.boolean("missing") is False
    assert fields.array("missing") is None


@pytest.mark.parametrize(
    ("body", "read"),
    [
        ({"n": "2"}, lambda f: f.integer("n")),
        ({"n": 2.0}, lambda f: f.integer("n")),
        ({"n": True}, lambda f: f.integer("n")),
        ({"n": None}, lambda f: f.integer("n")),
        ({"n": 2**63}, lambda f: f.integer("n")),
        ({"n": 1}, lambda f: f.string("n")),
        ({"n": "\ud800"}, lambda f: f.string("n")),
        ({"n": 1}, lambda f: f.boolean("n")),
        ({"n": None}, lambda f: f.boolean("n")),
        ({"n": {}}, lambda f: f.array("n")),
    ],
)
def test_wrong_types_name_their_path(body: dict[str, object], read) -> None:
    with pytest.raises(WireError) as caught:
        read(JsonObject(body))
    assert caught.value.path == "$.n"


@pytest.mark.parametrize("text", [b"", b"{", b"NaN", b"[1, Infinity]", b"\xff", b"[" * 100_000])
def test_only_standard_json_parses(text: bytes) -> None:
    with pytest.raises(WireError):
        wire.loads(text)


def test_non_objects_are_refused() -> None:
    with pytest.raises(WireError, match=r"\$\.entries\[0\]"):
        JsonObject([1], "$.entries[0]")


def test_state_files_round_trip_and_refuse_garbage(tmp_path) -> None:
    path = tmp_path / "deep" / "state.json"
    assert state_file.load(path) is None

    state_file.save(path, {"epoch": 3})
    assert state_file.load(path) == {"epoch": 3}
    assert not path.with_name("state.json.tmp").exists()

    path.write_text("null")
    with pytest.raises(ValueError, match="empty"):
        state_file.load(path)
    path.write_text("{oops")
    with pytest.raises(ValueError, match="corrupt"):
        state_file.load(path)


def test_int32_fields_refuse_what_does_not_fit_a_dotnet_int() -> None:
    fields = JsonObject({"small": 2**31 - 1, "big": 2**31, "neg": -(2**31) - 1})

    assert fields.int32("small") == 2**31 - 1
    assert fields.int32("missing", default=5) == 5
    for name in ("big", "neg"):
        with pytest.raises(WireError, match=rf"\$\.{name}"):
            fields.int32(name)


def test_integer_maps_keep_their_keys_and_check_every_value() -> None:
    assert JsonObject({"Balances": {"Acct-1": 5, "acct-1": -2}}).integers("balances") == {"Acct-1": 5, "acct-1": -2}
    assert JsonObject({}).integers("balances") is None
    with pytest.raises(WireError, match=r"\$\.balances\.x"):
        JsonObject({"balances": {"x": 1.5}}).integers("balances")
    with pytest.raises(WireError, match=r"\$\.balances"):
        JsonObject({"balances": [1]}).integers("balances")


def test_utf16_length_counts_like_dotnet() -> None:
    assert (wire.utf16_length("abc"), wire.utf16_length("€"), wire.utf16_length("😀")) == (3, 1, 2)
