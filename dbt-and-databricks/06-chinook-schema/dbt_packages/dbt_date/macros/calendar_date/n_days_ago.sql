{%- macro n_days_ago(n, date=None, tz=None) -%}
    {{ return(adapter.dispatch("n_days_ago", "dbt_date")(n, date, tz)) }}
{%- endmacro -%}

{%- macro default__n_days_ago(n, date=None, tz=None) -%}
    {%- set dt = date if date else dbt_date.today(tz) -%}
    {%- set n = n | int -%}
    cast({{ dbt.dateadd("day", -1 * n, dt) }} as date)
{%- endmacro -%}
