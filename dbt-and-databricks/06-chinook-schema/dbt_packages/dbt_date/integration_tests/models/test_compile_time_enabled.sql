{{ config(enabled=dbt_date.is_before("9999-12-31", tz="UTC")) }}

{# Stays enabled so config(enabled=...) is exercised with a true Jinja boolean. #}
select 1 as one
