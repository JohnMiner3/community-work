{%- macro n_months_ago(n, tz=None) -%}
    {{ return(adapter.dispatch("n_months_ago", "dbt_date")(n, tz)) }}
{%- endmacro -%}

{%- macro default__n_months_ago(n, tz=None) -%}
    {%- set n = n | int -%}
    {{ dbt.date_trunc("month", dbt.dateadd("month", -1 * n, dbt_date.today(tz))) }}
{%- endmacro -%}
