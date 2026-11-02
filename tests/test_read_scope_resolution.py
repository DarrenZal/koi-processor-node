"""Unit tests for token -> ReadScope resolution and strict scopes-file validation
(api.read_scope).

No database. Covers: operator env tokens -> personal; configured field/public
tokens -> their scope; unknown token -> None; and that every invalid
configuration raises ReadScopeConfigError instead of resolving to a broader
scope. The two reported defects have negative controls here:

  * an unknown ``view`` value used to fall through to personal_scope();
  * ``bool("false")`` used to enable include_unassigned.

Run: venvs/koi-server/bin/python -m pytest tests/test_read_scope_resolution.py -q
"""
import json
import hashlib
import pytest

from api import read_scope as rs


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch, tmp_path):
    for e in rs._PERSONAL_TOKEN_ENVS + ("KOI_READ_SCOPES_FILE",):
        monkeypatch.delenv(e, raising=False)
    # read_admin_token() also reads $KOI_STATE_DIR/admin_token; point it at an
    # empty directory so the operator's real token file is never consulted.
    monkeypatch.setenv("KOI_STATE_DIR", str(tmp_path / "state"))
    yield


def _h(tok):
    return hashlib.sha256(tok.encode()).hexdigest()


def _scopes(monkeypatch, tmp_path, mapping, raw=None):
    p = tmp_path / "scopes.json"
    p.write_text(raw if raw is not None else json.dumps(mapping))
    monkeypatch.setenv("KOI_READ_SCOPES_FILE", str(p))
    return p


# --- resolution ---------------------------------------------------------------

def test_operator_env_token_is_personal(monkeypatch):
    monkeypatch.setenv("KOI_MCP_TOKEN", "mcp-secret")
    s = rs.scope_from_token_value("mcp-secret")
    assert s is not None and s.is_personal and s.principal == "token:KOI_MCP_TOKEN"


def test_admin_token_from_state_file_is_personal(monkeypatch, tmp_path):
    d = tmp_path / "state"
    d.mkdir()
    (d / "admin_token").write_text("adm-secret\n")
    s = rs.scope_from_token_value("adm-secret")
    assert s is not None and s.is_personal and s.principal == "token:KOI_ADMIN_TOKEN"


def test_unknown_or_absent_token_is_none():
    assert rs.scope_from_token_value("nope") is None
    assert rs.scope_from_token_value(None) is None
    assert rs.scope_from_token_value("") is None


def test_no_session_or_default_personal_path_exists():
    # Defect A's mechanism is gone: there is no resolve_scope(default_personal=...)
    # and scope_from_token_value takes no session identity.
    assert not hasattr(rs, "resolve_scope")
    with pytest.raises(TypeError):
        rs.scope_from_token_value(None, session_email="zal@example.org")


def test_configured_field_token(monkeypatch, tmp_path):
    _scopes(monkeypatch, tmp_path, {_h("field-tok"): {
        "view": "field", "fields": ["bio"], "include_unassigned": False, "principal": "svc:bio"}})
    s = rs.scope_from_token_value("field-tok")
    assert s is not None and s.view == "field" and s.fields == frozenset({"bio"})
    assert s.include_unassigned is False and not s.include_private


def test_include_unassigned_true_only_from_json_true(monkeypatch, tmp_path):
    _scopes(monkeypatch, tmp_path, {_h("f"): {"view": "field", "fields": ["bio"],
                                              "include_unassigned": True}})
    assert rs.scope_from_token_value("f").include_unassigned is True


def test_configured_public_token(monkeypatch, tmp_path):
    _scopes(monkeypatch, tmp_path, {_h("pub-tok"): {"view": "public", "sources": ["document:x"]}})
    s = rs.scope_from_token_value("pub-tok")
    assert s is not None and s.view == "public" and s.sources == frozenset({"document:x"})


def test_env_token_wins_even_when_file_is_broken(monkeypatch, tmp_path):
    monkeypatch.setenv("KOI_MCP_TOKEN", "mcp-secret")
    _scopes(monkeypatch, tmp_path, None, raw="{ not json")
    assert rs.scope_from_token_value("mcp-secret").is_personal


# --- negative controls for the two reported defects ---------------------------

@pytest.mark.parametrize("view", ["admin", "personal", "Field", "FIELD", "", None, 1, ["field"]])
def test_unknown_view_is_a_config_error_never_personal(monkeypatch, tmp_path, view):
    entry = {"view": view, "fields": ["bio"]}
    _scopes(monkeypatch, tmp_path, {_h("tok"): entry})
    with pytest.raises(rs.ReadScopeConfigError, match="view"):
        rs.scope_from_token_value("tok")


def test_missing_view_is_a_config_error(monkeypatch, tmp_path):
    _scopes(monkeypatch, tmp_path, {_h("tok"): {"fields": ["bio"]}})
    with pytest.raises(rs.ReadScopeConfigError, match="view"):
        rs.scope_from_token_value("tok")


@pytest.mark.parametrize("value", ["false", "true", "False", 0, 1, "", None, [], {}])
def test_include_unassigned_must_be_a_json_boolean(monkeypatch, tmp_path, value):
    _scopes(monkeypatch, tmp_path, {_h("tok"): {
        "view": "field", "fields": ["bio"], "include_unassigned": value}})
    with pytest.raises(rs.ReadScopeConfigError, match="include_unassigned"):
        rs.scope_from_token_value("tok")


# --- other strict-validation cases -------------------------------------------

@pytest.mark.parametrize("entry,match", [
    ({"view": "field", "fields": "bio"}, "fields"),                 # bare string -> chars
    ({"view": "field", "fields": ["bio", 3]}, "fields"),
    ({"view": "field", "fields": [""]}, "fields"),
    ({"view": "field", "fields": ["bio"], "sources": "document:x"}, "sources"),
    ({"view": "field", "fields": ["bio"], "include_private": True}, "not allowed"),
    ({"view": "public", "include_unassigned": True}, "not allowed"),
    ({"view": "public", "fields": ["bio"]}, "not allowed"),
    ({"view": "field", "fields": ["bio"], "principal": 5}, "principal"),
    ({"view": "field", "fields": ["bio"], "colour": "red"}, "not allowed"),
    ("field", "object"),
])
def test_invalid_entry_is_a_config_error(monkeypatch, tmp_path, entry, match):
    _scopes(monkeypatch, tmp_path, {_h("tok"): entry})
    with pytest.raises(rs.ReadScopeConfigError, match=match):
        rs.scope_from_token_value("tok")


def test_one_bad_entry_fails_the_whole_file(monkeypatch, tmp_path):
    _scopes(monkeypatch, tmp_path, {
        _h("good"): {"view": "field", "fields": ["bio"]},
        _h("bad"): {"view": "field", "fields": ["bio"], "include_unassigned": "false"}})
    with pytest.raises(rs.ReadScopeConfigError):
        rs.scope_from_token_value("good")


@pytest.mark.parametrize("raw,match", [
    ("{ this is not json", "not JSON"),
    ("[]", "JSON object"),
    (json.dumps({"not-a-hash": {"view": "public"}}), "sha256"),
    (json.dumps({_h("x").upper(): {"view": "public"}}), "sha256"),
])
def test_malformed_file_is_a_config_error(monkeypatch, tmp_path, raw, match):
    _scopes(monkeypatch, tmp_path, None, raw=raw)
    with pytest.raises(rs.ReadScopeConfigError, match=match):
        rs.scope_from_token_value("whatever")


def test_configured_but_missing_file_is_a_config_error(monkeypatch, tmp_path):
    monkeypatch.setenv("KOI_READ_SCOPES_FILE", str(tmp_path / "absent.json"))
    with pytest.raises(rs.ReadScopeConfigError, match="does not exist"):
        rs.scope_from_token_value("whatever")


def test_file_entry_for_an_operator_token_is_a_config_error(monkeypatch, tmp_path):
    monkeypatch.setenv("KOI_MCP_TOKEN", "mcp-secret")
    _scopes(monkeypatch, tmp_path, {_h("mcp-secret"): {"view": "public"}})
    with pytest.raises(rs.ReadScopeConfigError, match="operator token"):
        rs.load_scope_map()


def test_unset_file_means_no_scoped_tokens():
    assert rs.load_scope_map() == {}


def test_withheld_seeded_from_env(monkeypatch):
    monkeypatch.setenv("KOI_WITHHELD_SOURCE_RIDS", "document:extra1,document:extra2")
    s = rs.personal_scope()
    assert "document:extra1" in s.withheld and "document:extra2" in s.withheld
    # the seed (mislabeled email) is always present
    assert any(r.startswith("document:2f6c912b") for r in s.withheld)
