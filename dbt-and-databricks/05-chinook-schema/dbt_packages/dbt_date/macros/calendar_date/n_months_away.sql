{%- macro n_months_away(n, tz=None) -%}
    {{ return(adapter.dispatch("n_months_away", "dbt_date")(n, tz)) }}
{%- endmacro -%}

{%- macro default__n_months_away(n, tz=None) -%}
    {%- set n = n | int -%}
    {{ dbt.date_trunc("month", dbt.dateadd("month", n, dbt_date.today(tz))) }}
{%- endmacro -%}
