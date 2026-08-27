{{ config(enabled=dbt_date.is_after("9999-12-31", tz="UTC")) }}

{# If is_after wrongly returns true, parse fails instead of silently building this model. #}
{% if dbt_date.is_after("9999-12-31", tz="UTC") %}
    {{
        exceptions.raise_compiler_error(
            "test_compile_time_disabled should have been skipped"
        )
    }}
{% endif %}

select 1 as should_not_run
