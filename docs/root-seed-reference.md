# Consumer-Owned Codegen Configuration

Date: 2026-10-14. Scope: [cdc_dbt_codegen #8](https://github.com/CloudDataConsulting/cdc_dbt_codegen/issues/8).

The four codegen models explicitly reference `ref(project_name, 'code_gen_config')`.
In the tested dbt context, `project_name` is the consuming root project's name,
so renaming that project does not require a package edit. The consumer still
owns its configuration seed. This does not ship a package example seed or
approve the seed-shipping/interface change discussed in the maintainer comment.
A missing consumer seed remains a parse failure instead of falling back to
example data. Reverting the four references reverses this source change.

## Offline Parse Proof

Tested Python 3.13.16, dbt-core 1.11.16 and dbt-snowflake 1.11.6. Product
requirements, package version, seeds and release tags are unchanged. Older dbt
versions allowed by the package's existing `>=1.0.0,<2.0.0` range are unproved.

```sh
uv run --offline --python /opt/homebrew/bin/python3.13 \
  --with dbt-core==1.11.16 --with dbt-snowflake==1.11.6 \
  --with pytest --with pytest-mock \
  python -m pytest tests/integration/test_root_seed_parse.py -q
```

This requires those dependencies cached by uv and baseline Git object
`e6f566aafa380e21a4ea42f56f557de036b15316` present locally. Tests never fetch
missing dependencies or Git history. For another machine, select its supported
Python 3.13 interpreter explicitly.

Seven cases parse all nine actual package models with an isolated synthetic
root project/profile. They retain manifests and summaries only under ignored
`logs/root-seed-ref/`. No user's dbt profile is read; telemetry is disabled.
Guards reject any Snowflake connection-open or socket-connect attempt and assert
zero calls. No warehouse, `seed`, `build`, or model execution is tested.

- Immutable published baseline: exactly four root-dependency warnings and the
  expected root-seed/upstream dependencies.
- Two root names × effective behavior flag false/true: no such warnings, exactly
  the same intended dependency sets, only the consumer seed and no package seed.
- Missing consumer seed × false/true: parse fails with the missing-ref error.

The installed 1.11.16 flag is `require_ref_searches_node_package_before_root`;
tests assert its observed effective value. The warning text still names the
historical `require_ref_prefers_node_package_to_root` key; using that old key
would not prove the true case. Initial characterization using it was corrected
before publication. This evidence concerns parse resolution only, not generated
SQL correctness, warehouse behavior, consumer upgrades or a released fix.
