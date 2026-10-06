from __future__ import annotations

import copy
import json
import pathlib
import subprocess

import pytest

from tests.test_contracts import schema_semver

ROOT = pathlib.Path(__file__).parents[2]
FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "v1_1_0"


def test_schema_changes_require_contract_major_bump():
    try:
        tag, errors = schema_semver.check_checkout(ROOT)
    except schema_semver.BaselineUnavailable as exc:
        pytest.skip(str(exc))
    assert not errors, f"Quebras de contrato desde {tag}:\n" + "\n".join(errors)


def test_ci_carrega_historico_e_tags_para_o_gate():
    workflow = ROOT / ".github/workflows/tests.yml"
    if not workflow.exists():
        pytest.skip("Workflows não são distribuídos no sdist")
    yaml = pytest.importorskip("yaml")
    steps = yaml.safe_load(workflow.read_text(encoding="utf-8"))["jobs"]["test"]["steps"]
    checkouts = [step for step in steps if step.get("uses", "").startswith("actions/checkout@")]
    assert checkouts
    assert all(step.get("with", {}).get("fetch-depth") == 0 for step in checkouts)


def test_gate_reports_explicit_skip_when_baseline_is_unavailable(monkeypatch):
    def check_checkout(root):
        assert root == ROOT
        raise schema_semver.BaselineUnavailable("nenhuma tag local anterior")

    monkeypatch.setattr(schema_semver, "check_checkout", check_checkout)
    with pytest.raises(pytest.skip.Exception, match="nenhuma tag local anterior"):
        test_schema_changes_require_contract_major_bump()


@pytest.mark.parametrize(
    "error",
    [
        FileNotFoundError("schema ausente"),
        json.JSONDecodeError("schema inválido", "", 0),
        subprocess.CalledProcessError(128, ["git", "show"]),
    ],
)
def test_gate_propagates_unexpected_file_or_git_errors(monkeypatch, error):
    def check_checkout(root):
        assert root == ROOT
        raise error

    monkeypatch.setattr(schema_semver, "check_checkout", check_checkout)
    with pytest.raises(type(error)):
        test_schema_changes_require_contract_major_bump()


@pytest.fixture
def schema():
    return {
        "schema_version": "1.0",
        "primary_key": ["id"],
        "columns": [
            {"name": "id", "type": "str", "nullable": False},
            {"name": "valor", "type": "float", "nullable": False},
        ],
        "dtypes": {"id": "str", "valor": "float"},
        "nullable": {"id": False, "valor": False},
    }


@pytest.mark.parametrize("primary_key", [[], ["valor"], ["id", "valor"]])
def test_primary_key_change_requires_major(schema, primary_key):
    current = copy.deepcopy(schema)
    current["schema_version"] = "1.10"
    current["primary_key"] = primary_key
    errors = schema_semver.breaking_changes({"sample.json": schema}, {"sample.json": current})
    assert len(errors) == 1
    assert "primary_key" in errors[0] and "sem aumento de major" in errors[0]


@pytest.mark.parametrize("nullable", [False, True])
def test_column_removal_requires_major_even_when_nullable(schema, nullable):
    schema["columns"][1]["nullable"] = nullable
    current = copy.deepcopy(schema)
    current["columns"].pop()
    errors = schema_semver.breaking_changes({"sample.json": schema}, {"sample.json": current})
    assert len(errors) == 1
    assert "coluna removida: valor" in errors[0]


@pytest.mark.parametrize(
    "old_type,new_type",
    [("float", "int"), ("float", "str"), ("datetime", "date"), ("str", "int")],
)
def test_type_narrowing_requires_major(schema, old_type, new_type):
    schema["columns"][1]["type"] = old_type
    current = copy.deepcopy(schema)
    current["columns"][1]["type"] = new_type
    errors = schema_semver.breaking_changes({"sample.json": schema}, {"sample.json": current})
    assert len(errors) == 1
    assert f"valor: tipo incompatível {old_type} -> {new_type}" in errors[0]


@pytest.mark.parametrize(
    "old_type,new_type", [("int", "float"), ("int", "Decimal"), ("date", "datetime")]
)
def test_type_widening_is_compatible(schema, old_type, new_type):
    schema["columns"][1]["type"] = old_type
    current = copy.deepcopy(schema)
    current["columns"][1]["type"] = new_type
    assert schema_semver.breaking_changes({"sample.json": schema}, {"sample.json": current}) == []


def test_new_contract_and_optional_column_are_compatible(schema):
    current = copy.deepcopy(schema)
    current["schema_version"] = "1.1"
    current["columns"].append({"name": "latitude", "type": "float", "nullable": True})
    assert (
        schema_semver.breaking_changes(
            {"sample.json": schema}, {"sample.json": current, "new.json": schema}
        )
        == []
    )


def test_all_incompatible_changes_are_allowed_with_contract_major(schema):
    current = copy.deepcopy(schema)
    current["schema_version"] = "2.0"
    current["primary_key"] = []
    current["columns"].pop(0)
    current["columns"][0].update(type="int", nullable=True)
    assert schema_semver.breaking_changes({"sample.json": schema}, {"sample.json": current}) == []


def test_nullable_relaxation_requires_major(schema):
    current = copy.deepcopy(schema)
    current["columns"][1]["nullable"] = True
    errors = schema_semver.breaking_changes({"sample.json": schema}, {"sample.json": current})
    assert len(errors) == 1
    assert "valor: nullable False -> True" in errors[0]


@pytest.mark.parametrize(
    "name,diagnostic",
    [
        ("preco_atacado", "categoria: nullable False -> True"),
        ("mapa_psr_apolices", "primary_key"),
    ],
)
@pytest.mark.parametrize("unbumped_version", ["1.1", "1.2"])
def test_a_sem01_rejects_historical_minor_with_current_break(name, diagnostic, unbumped_version):
    filename = f"{name}.json"
    published = json.loads((FIXTURES / filename).read_text(encoding="utf-8"))
    current = json.loads((ROOT / "agrobr" / "schemas" / filename).read_text(encoding="utf-8"))
    assert published["schema_version"] == "1.0"
    current["schema_version"] = unbumped_version
    errors = schema_semver.breaking_changes({filename: published}, {filename: current})
    assert len(errors) == 1
    assert diagnostic in errors[0]
    if name == "mapa_psr_apolices":
        assert "seguradora" not in published["primary_key"]
        assert "seguradora" in current["primary_key"]
        assert "seguradora" in errors[0]


@pytest.mark.parametrize("name", ["preco_atacado", "mapa_psr_apolices"])
def test_a_sem01_current_major_accepts_schema_published_in_v1_1_0(name):
    filename = f"{name}.json"
    published = json.loads((FIXTURES / filename).read_text(encoding="utf-8"))
    current = json.loads((ROOT / "agrobr" / "schemas" / filename).read_text(encoding="utf-8"))
    assert published["schema_version"] == "1.0"
    assert current["schema_version"] == "2.0"
    assert schema_semver.breaking_changes({filename: published}, {filename: current}) == []


@pytest.fixture
def git_checkout(tmp_path, monkeypatch):
    (tmp_path / ".git").mkdir()
    responses = {
        ("rev-parse", "--show-toplevel"): str(tmp_path),
        ("rev-parse", "--verify", "HEAD"): "current-commit",
        ("tag", "--merged", "HEAD", "--list", "v*"): "v1.1.0\n",
        ("rev-parse", "--verify", "refs/tags/v1.1.0^{commit}"): "previous-commit",
    }
    calls = []

    def run(command, **kwargs):
        assert command[:3] == ["git", "-C", str(tmp_path)]
        assert kwargs == {
            "check": True,
            "capture_output": True,
            "text": True,
            "encoding": "utf-8",
            "timeout": 30,
        }
        arguments = tuple(command[3:])
        calls.append(arguments)
        response = responses[arguments]
        if isinstance(response, BaseException):
            raise response
        return subprocess.CompletedProcess(command, 0, stdout=response, stderr="")

    monkeypatch.setattr(schema_semver.subprocess, "run", run)
    return tmp_path, responses, calls


@pytest.mark.parametrize(
    "tags,head_tags,expected",
    [
        (["v1.9.0", "v1.10.0"], [], "v1.10.0"),
        (["v2.0.0", "v1.1.0"], ["v2.0.0"], "v1.1.0"),
        (["v2.0.0", "v1.10.0", "v1.1.0"], ["v2.0.0", "v1.10.0"], "v1.1.0"),
        (["v1.2.0-rc.10", "v1.2.0-rc.2"], [], "v1.2.0-rc.10"),
        (["v1.2.0-rc.10", "v1.2.0", "vnext"], [], "v1.2.0"),
        (["v1.2.0+build.5", "v1.1.9"], [], "v1.2.0+build.5"),
    ],
)
def test_previous_local_reachable_tag_uses_semver_and_excludes_head(
    git_checkout, tags, head_tags, expected
):
    root, responses, calls = git_checkout
    responses[("tag", "--merged", "HEAD", "--list", "v*")] = "\n".join(tags)
    for tag in tags:
        responses[("rev-parse", "--verify", f"refs/tags/{tag}^{{commit}}")] = (
            "current-commit" if tag in head_tags else "previous-commit"
        )
    assert schema_semver.previous_tag(root) == expected
    assert ("tag", "--merged", "HEAD", "--list", "v*") in calls
    assert not any("fetch" in command for command in calls)


@pytest.mark.parametrize("tags", ["", "vnext", "v2.0.0"])
def test_previous_tag_absence_is_explicit(git_checkout, tags):
    root, responses, _ = git_checkout
    responses[("tag", "--merged", "HEAD", "--list", "v*")] = tags
    responses[("rev-parse", "--verify", "refs/tags/v2.0.0^{commit}")] = "current-commit"
    with pytest.raises(schema_semver.BaselineUnavailable, match="nenhuma tag local"):
        schema_semver.previous_tag(root)


def test_git_absence_is_explicit(git_checkout):
    root, responses, _ = git_checkout
    responses[("rev-parse", "--show-toplevel")] = FileNotFoundError("git")
    with pytest.raises(schema_semver.BaselineUnavailable, match="executável git indisponível"):
        schema_semver.previous_tag(root)


def test_sdist_inside_other_checkout_does_not_use_parent_git(git_checkout):
    root, _, calls = git_checkout
    sdist = root / "dist" / "agrobr"
    sdist.mkdir(parents=True)
    with pytest.raises(schema_semver.BaselineUnavailable, match="checkout sem .git"):
        schema_semver.previous_tag(sdist)
    assert calls == []


@pytest.mark.parametrize("failure", [PermissionError("git"), subprocess.TimeoutExpired("git", 30)])
def test_git_os_errors_are_not_skipped(git_checkout, failure):
    root, responses, _ = git_checkout
    responses[("rev-parse", "--show-toplevel")] = failure
    with pytest.raises(type(failure)):
        schema_semver.previous_tag(root)


def test_git_command_failure_is_not_skipped(git_checkout):
    root, responses, _ = git_checkout
    responses[("tag", "--merged", "HEAD", "--list", "v*")] = subprocess.CalledProcessError(
        128, ["git", "tag"], stderr="bad object HEAD"
    )
    with pytest.raises(subprocess.CalledProcessError):
        schema_semver.check_checkout(root)


def test_git_checkout_mismatch_is_not_skipped(git_checkout):
    root, responses, _ = git_checkout
    responses[("rev-parse", "--show-toplevel")] = str(root.parent)
    with pytest.raises(ValueError, match="Checkout git inesperado"):
        schema_semver.previous_tag(root)


def test_tag_schemas_are_read_with_git_show(git_checkout, schema):
    root, responses, calls = git_checkout
    responses[
        ("ls-tree", "-r", "--name-only", "--full-tree", "refs/tags/v1.1.0", "--", "agrobr/schemas/")
    ] = "agrobr/schemas/sample.json\nagrobr/schemas/__init__.py\n"
    responses[("show", "refs/tags/v1.1.0:agrobr/schemas/sample.json")] = json.dumps(schema)
    assert schema_semver.tag_schemas(root, "v1.1.0") == {"sample.json": schema}
    assert ("show", "refs/tags/v1.1.0:agrobr/schemas/sample.json") in calls


@pytest.mark.parametrize("contents", ["broken-json", "{}", '{"columns": []}'])
def test_invalid_schema_does_not_become_skip(contents):
    with pytest.raises((ValueError, KeyError)):
        schema_semver.parse_schema(contents, "fixture.json")


@pytest.mark.parametrize("field", ["dtypes", "nullable"])
def test_inconsistent_schema_does_not_become_skip(schema, field):
    schema[field] = {}
    with pytest.raises(ValueError, match=f"{field} diverge"):
        schema_semver.parse_schema(json.dumps(schema), "fixture.json")


def test_missing_schema_directory_does_not_become_skip(tmp_path):
    with pytest.raises(FileNotFoundError):
        schema_semver.checkout_schemas(tmp_path)


def test_empty_schema_directory_does_not_pass_silently(tmp_path):
    (tmp_path / "agrobr" / "schemas").mkdir(parents=True)
    with pytest.raises(ValueError, match="nenhum schema JSON"):
        schema_semver.checkout_schemas(tmp_path)


def test_file_in_place_of_schema_directory_does_not_pass_silently(tmp_path):
    (tmp_path / "agrobr").mkdir()
    (tmp_path / "agrobr" / "schemas").write_text("inválido", encoding="utf-8")
    with pytest.raises(NotADirectoryError):
        schema_semver.checkout_schemas(tmp_path)


def test_missing_schemas_in_existing_tag_do_not_become_skip(git_checkout):
    root, responses, _ = git_checkout
    responses[
        ("ls-tree", "-r", "--name-only", "--full-tree", "refs/tags/v1.1.0", "--", "agrobr/schemas/")
    ] = ""
    with pytest.raises(ValueError, match="nenhum agrobr/schemas"):
        schema_semver.tag_schemas(root, "v1.1.0")


def test_missing_blob_in_existing_tag_does_not_become_skip(git_checkout):
    root, responses, _ = git_checkout
    responses[
        ("ls-tree", "-r", "--name-only", "--full-tree", "refs/tags/v1.1.0", "--", "agrobr/schemas/")
    ] = "agrobr/schemas/sample.json\n"
    responses[("show", "refs/tags/v1.1.0:agrobr/schemas/sample.json")] = (
        subprocess.CalledProcessError(128, ["git", "show"], stderr="missing blob")
    )
    with pytest.raises(subprocess.CalledProcessError):
        schema_semver.tag_schemas(root, "v1.1.0")
