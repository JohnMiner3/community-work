{%- macro last_month(tz=None) -%}
    {{ return(adapter.dispatch("last_month", "dbt_date")(tz)) }}
{%- endmacro -%}

{%- macro default__last_month(tz=None) -%}
    {{ dbt_date.n_months_ago(1, tz) }}
{%- endmacro -%}
