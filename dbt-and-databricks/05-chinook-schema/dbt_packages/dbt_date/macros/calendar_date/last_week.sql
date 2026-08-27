{%- macro last_week(tz=None) -%}
    {{ return(adapter.dispatch("last_week", "dbt_date")(tz)) }}
{%- endmacro -%}

{%- macro default__last_week(tz=None) -%}
    {{ dbt_date.n_weeks_ago(1, tz) }}
{%- endmacro -%}
