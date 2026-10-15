"""Actual dbt parse of the package in an isolated, synthetic root project."""

import hashlib
import json
import os
import shutil
import socket
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from dbt.adapters.snowflake.connections import SnowflakeConnectionManager
from dbt.cli.main import dbtRunner
from dbt.flags import get_flags

REPO = Path(__file__).resolve().parents[2]
MODELS = ("gen_stg_yml", "gen_stg_src_name_yml", "gen_stg_sql", "gen_stage_files")
WARNING = "depends on the root project node"
BASE_COMMIT = "e6f566aafa380e21a4ea42f56f557de036b15316"
REF_FLAG = "require_ref_searches_node_package_before_root"
UPSTREAM = {
    "gen_stg_yml": "src_column_list",
    "gen_stg_sql": "src_column_list",
    "gen_stg_src_name_yml": "src_table_list",
    "gen_stage_files": "src_table_list",
}


@pytest.fixture
def parse_root():
    evidence = REPO / "logs" / "root-seed-ref"
    evidence.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix="parse-", dir=evidence))


def parse_package(root, project_name, flag, *, baseline=False, seed=True):
    home = root / "isolated-home"
    home.mkdir()
    project = root / "project"
    project.mkdir()
    profile = root / "profiles"
    profile.mkdir()
    package = project / "dbt_packages" / "cdc_dbt_codegen"
    package.mkdir(parents=True)
    shutil.copy2(REPO / "dbt_project.yml", package / "dbt_project.yml")
    shutil.copytree(REPO / "models", package / "models")
    shutil.copytree(REPO / "seeds", package / "seeds")
    if baseline:
        for name in MODELS:
            model = package / "models" / "dw_util" / f"{name}.sql"
            published = subprocess.run(
                [
                    "git",
                    "--no-optional-locks",
                    "-C",
                    str(REPO),
                    "show",
                    f"{BASE_COMMIT}:models/dw_util/{name}.sql",
                ],
                env={
                    "PATH": "/usr/bin:/bin",
                    "HOME": str(home),
                    "GIT_CONFIG_GLOBAL": "/dev/null",
                    "GIT_CONFIG_NOSYSTEM": "1",
                },
                capture_output=True,
                check=True,
                timeout=10,
            )
            model.write_bytes(published.stdout)
    config = {
        "name": project_name,
        "version": "1.0.0",
        "config-version": 2,
        "profile": "offline_parse",
        "flags": {REF_FLAG: flag},
        "vars": {"source_db": ["synthetic_raw"]},
    }
    (project / "dbt_project.yml").write_text(yaml.safe_dump(config))
    (project / "models").mkdir()
    (project / "seeds").mkdir()
    if seed:
        (project / "seeds" / "code_gen_config.csv").write_text(
            "generate_flag,source_name,description,database,schema,loader,loaded_at_field\n"
            "N,synthetic,Offline fixture,SYNTHETIC_RAW,SYNTHETIC,Fixture,unused\n"
        )
    (profile / "profiles.yml").write_text(
        yaml.safe_dump(
            {
                "offline_parse": {
                    "target": "parse_only",
                    "outputs": {
                        "parse_only": {
                            "type": "snowflake",
                            "account": "synthetic.invalid",
                            "user": "synthetic_parse_only",
                            "password": "unused",
                            "database": "SYNTHETIC",
                            "schema": "PARSE_ONLY",
                            "warehouse": "UNUSED",
                            "threads": 1,
                        }
                    },
                }
            }
        )
    )
    events = []
    env = {
        "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
        "HOME": str(home),
        "ZDOTDIR": str(home),
        "DBT_PROFILES_DIR": str(profile),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        "DBT_PARTIAL_PARSE": "false",
        "DBT_USE_COLORS": "false",
        "DO_NOT_TRACK": "1",
    }
    with (
        patch.dict(os.environ, env, clear=True),
        patch.object(
            SnowflakeConnectionManager,
            "open",
            side_effect=AssertionError("Parse attempted a warehouse connection"),
        ) as database_open,
        patch.object(
            socket.socket,
            "connect",
            side_effect=AssertionError("Parse attempted network access"),
        ) as network_connect,
    ):
        result = dbtRunner(
            callbacks=[lambda event: events.append(event.info.msg)]
        ).invoke(
            [
                "parse",
                "--project-dir",
                str(project),
                "--profiles-dir",
                str(profile),
                "--target-path",
                str(root / "target"),
                "--log-path",
                str(root / "dbt-log"),
                "--no-partial-parse",
                "--no-send-anonymous-usage-stats",
            ]
        )
        effective_flag = getattr(get_flags(), REF_FLAG.upper())
        assert effective_flag is flag, "Requested dbt behavior flag was not effective"
        database_open.assert_not_called()
        network_connect.assert_not_called()
    manifest_path = root / "target" / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else None
    (root / "result.json").write_text(
        json.dumps(
            {
                "success": result.success,
                "root_project": project_name,
                "flag": flag,
                "baseline": baseline,
                "seed_present": seed,
                "effective_flag": effective_flag,
                "flag_name": REF_FLAG,
                "model_source_sha256": {
                    name: hashlib.sha256(
                        (package / "models" / "dw_util" / f"{name}.sql").read_bytes()
                    ).hexdigest()
                    for name in MODELS
                },
                "root_dependency_warnings": [
                    message for message in events if WARNING in message
                ],
                "warehouse_connection_calls": database_open.call_count,
                "network_connection_calls": network_connect.call_count,
            },
            indent=2,
        )
        + "\n"
    )
    return result, manifest, events


@pytest.mark.integration
def test_bare_ref_baseline_reproduces_four_warnings(parse_root):
    result, manifest, events = parse_package(
        parse_root, "consumer_alpha", False, baseline=True
    )
    assert result.success
    warnings = [message for message in events if WARNING in message]
    assert len(warnings) == 4
    for model in MODELS:
        node = manifest["nodes"][f"model.cdc_dbt_codegen.{model}"]
        assert set(node["depends_on"]["nodes"]) == {
            "seed.consumer_alpha.code_gen_config",
            f"model.cdc_dbt_codegen.{UPSTREAM[model]}",
        }


@pytest.mark.integration
@pytest.mark.parametrize("project_name", ["consumer_alpha", "renamed_consumer"])
@pytest.mark.parametrize("flag", [False, True])
def test_explicit_root_reference_preserves_seed_dependency(
    parse_root, project_name, flag
):
    result, manifest, events = parse_package(parse_root, project_name, flag)
    assert result.success
    assert not [message for message in events if WARNING in message]
    for model in MODELS:
        node = manifest["nodes"][f"model.cdc_dbt_codegen.{model}"]
        assert set(node["depends_on"]["nodes"]) == {
            f"seed.{project_name}.code_gen_config",
            f"model.cdc_dbt_codegen.{UPSTREAM[model]}",
        }
        assert any(
            ref["name"] == "code_gen_config" and ref["package"] == project_name
            for ref in node["refs"]
        )
    assert (
        len(
            [
                node
                for node in manifest["nodes"].values()
                if node["resource_type"] == "model"
            ]
        )
        == 9
    )
    assert {name for name in manifest["nodes"] if name.startswith("seed.")} == {
        f"seed.{project_name}.code_gen_config"
    }


@pytest.mark.integration
@pytest.mark.parametrize("flag", [False, True])
def test_missing_consumer_seed_still_fails(parse_root, flag):
    result, manifest, events = parse_package(
        parse_root, "consumer_alpha", flag, seed=False
    )
    assert not result.success
    assert any(
        "code_gen_config" in message and "not found" in message for message in events
    )
    assert manifest is None
