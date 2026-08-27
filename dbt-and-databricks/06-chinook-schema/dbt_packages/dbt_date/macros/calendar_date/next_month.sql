{%- macro next_month(tz=None) -%}
    {{ return(adapter.dispatch("next_month", "dbt_date")(tz)) }}
{%- endmacro -%}

{%- macro default__next_month(tz=None) -%}
    {{ dbt_date.n_months_away(1, tz) }}
{%- endmacro -%}
