# AGENTS.md

Guidance for AI coding agents (Claude Code, Codex, Cursor, Aider, etc.) working in this repo. Human contributors should read `README.md` and `CHANGELOG.md` first.

## What this repo is

`dbt-date` is a dbt package of macros for date logic and calendar functionality. It is consumed by other dbt projects via `packages.yml`, not run as a standalone project. The `integration_tests/` directory is a self-contained dbt project that exercises the macros against every supported adapter.

- **Package code**: `macros/{calendar_date,fiscal_date,_utils}/`
- **Integration test project**: `integration_tests/`
- **Supported adapters**: BigQuery, Databricks, DuckDB, Postgres, Spark, Trino (full), Snowflake (partial). Fusion engine: BigQuery, Databricks.
- **dbt core**: `>=1.10.5, <3.0.0`

## Golden rule: every user-facing macro is dispatchable

End users override behaviour by providing their own implementation in their project's `dbt_date_dispatch_list`. New macros must follow the dispatch pattern — no exceptions.

```jinja
{%- macro day_of_month(date) -%}
    {{ return(adapter.dispatch("day_of_month", "dbt_date")(date)) }}
{%- endmacro %}

{%- macro default__day_of_month(date) -%}
    {{ dbt_date.date_part("day", date) }}
{%- endmacro %}

{# Adapter-specific overrides go alongside, prefixed with adapter name #}
{%- macro redshift__day_of_month(date) -%}
    cast({{ dbt_date.date_part("day", date) }} as {{ dbt.type_bigint() }})
{%- endmacro %}
```

When converting an existing macro to be dispatchable:

1. Rename the original body to `default__<name>`.
2. Add the top-level wrapper that calls `adapter.dispatch(...)`.
3. Preserve any existing `<adapter>__<name>` overrides — they only become reachable once the wrapper exists, so call this out in the PR description.

## Local development

Python deps are managed with `uv` (see `pyproject.toml`). Pre-commit uses `sqlfmt` for SQL/Jinja formatting (`uv run sqlfmt`).

```bash
# One-time setup
mise run setup      # runs uv sync + starts docker compose (postgres, spark, trino)

# Run a single adapter (duckdb is fastest, no docker needed)
mise run test:duckdb

# Run everything in parallel
mise run test:all

# Fusion engine targets
mise run test:fusion-bigquery
mise run test:fusion-databricks

# Pre-commit (use prek if installed, else pre-commit)
prek run --all-files     # or: pre-commit run --all-files
```

DuckDB is the recommended adapter for iterative local work — it needs no credentials and no docker. BigQuery, Databricks, Snowflake need credentials (see `.env_example`). Postgres, Spark, Trino need `mise run setup` to start their docker containers.

### `DBT_SUFFIX`

Local-only env var that suffixes BigQuery/Databricks schemas so consecutive runs land in fresh datasets. Defaults to empty (correct for CI, where `github.run_number` already provides uniqueness). Set it locally if you re-run the same target rapidly:

```bash
export DBT_SUFFIX=_$(date +%6N)
```

Do **not** use `{{ modules.datetime... }}` for this — the dbt-fusion Jinja engine does not expose the `modules` namespace, and that broke profiles.yml in the past.

## Conventions

- **SQL style**: enforced by `sqlfmt`. Lowercase keywords-as-formatted-by-sqlfmt (the tool decides; do not hand-format). Run sqlfmt before committing.
- **Branch naming**: descriptive kebab-case — `feature/dispatchable-<area>`, `fix-<thing>`, `ignore-<thing>`.
- **Commits**: small, single-purpose; subject under 70 chars.
- **PRs**: open as draft by default. Description starts with a paragraph (no "Summary" or "Test plan" headers). Flag any side effects — particularly an adapter-specific override becoming reachable for the first time because a wrapper was added.
- **Rebasing**: rebase PR branches onto `main`; never merge `main` in.
- **GitHub Actions `if:` conditions**: always quote the comparison or put the operator inside the expression — `${{ matrix.adapter == 'bigquery' }}`, not `${{ matrix.adapter }} == 'bigquery'` (the latter substitutes to a bare identifier and silently misbehaves).
- **Never commit** `integration_tests/service-key-file.json` — CI writes it from a base64 secret for the Fusion BigQuery job.
- **Package version**: lives in both `pyproject.toml` (`project.version`) and `dbt_project.yml` (`version:`). `pyproject.toml` is the source of truth; never hand-edit `dbt_project.yml`'s version — run `python3 scripts/check_versions.py --fix`. A pre-commit hook and the `version_check` CI job fail on drift, and the release pipeline bumps both and commits them before tagging.

## CI

Three workflows in `.github/workflows/`:

- `ci-multiple-dbt-versions.yml` — runs the no-secret adapters (`clickhouse, duckdb, postgres, spark, trino`) on every PR and push. Works the same for fork PRs and same-repo branches because nothing here needs GitHub secrets.
- `ci-secret-adapters.yml` — runs the secret-bearing adapters (`bigquery, databricks`) plus the Fusion engine job. Triggered by a maintainer (`write+` permission) commenting `/ok-to-test` on a PR, plus `push: main`, a daily schedule, and manual dispatch. Runs in base-repo context so secrets are available; checks out the PR's head SHA (after validating it as a 40-char hex string) and posts commit statuses back to it. Each new push needs a fresh `/ok-to-test` comment.
- `ci.yml` — delegates to `dbt-labs/dbt-package-testing` reusable workflow for BigQuery, Databricks, Postgres, Trino. Runs on every PR and push to main.

After pushing, monitor with `gh run list --branch <branch> --limit 5`. If a PR has merge conflicts CI won't trigger — rebase onto `main` first.

## File map

| Path                                                         | Purpose                                                                      |
| ------------------------------------------------------------ | ---------------------------------------------------------------------------- |
| `macros/calendar_date/`                                      | Day/week/month/year macros (`day_of_week.sql`, `iso_week_start.sql`, …)      |
| `macros/fiscal_date/`                                        | Fiscal period macros (`get_fiscal_periods.sql`, `get_fiscal_year_dates.sql`) |
| `macros/_utils/`                                             | Cross-cutting helpers (`date_part`, `day_name`, `convert_timezone`, …)       |
| `macros/get_base_dates.sql`, `macros/get_date_dimension.sql` | Top-level date-dimension builders                                            |
| `integration_tests/models/`                                  | dbt models that consume the macros for testing                               |
| `integration_tests/macros/`                                  | Test-only macros (`get_test_dates.sql`, etc.)                                |
| `integration_tests/profiles.yml`                             | Adapter profiles read from env vars                                          |
| `mise.toml`                                                   | Tool version management and per-adapter test tasks           |
| `supported_adapters.env`                                     | The four adapters tested locally by default                                  |
| `.pre-commit-config.yaml`                                    | sqlfmt + prettier + standard hooks                                           |
| `scripts/check_versions.py`                                  | Guards `pyproject.toml` / `dbt_project.yml` version parity (`--fix` to sync) |

## When you're stuck

- Adapter-specific date-function quirks: look at how an existing macro handles the same adapter (e.g. Postgres uses `isodow`/`dow`, BigQuery's `dayofweek` is 1-indexed Sunday-first, Snowflake has `dayofweekiso`).
- Fusion-specific failures: the Fusion Jinja engine is stricter than dbt-core's. If something works on core but not Fusion, suspect Jinja features not exposed (e.g. `modules`).
- Test failures only on one adapter: re-run just that target locally with `mise run test:<adapter>` and inspect the rendered SQL in `integration_tests/target/`.
