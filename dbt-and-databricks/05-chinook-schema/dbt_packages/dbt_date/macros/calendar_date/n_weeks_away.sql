{%- macro n_weeks_away(n, tz=None) -%}
    {{ return(adapter.dispatch("n_weeks_away", "dbt_date")(n, tz)) }}
{%- endmacro -%}

{%- macro default__n_weeks_away(n, tz=None) -%}
    {%- set n = n | int -%}
    {{ dbt.date_trunc("week", dbt.dateadd("week", n, dbt_date.today(tz))) }}
{%- endmacro -%}
