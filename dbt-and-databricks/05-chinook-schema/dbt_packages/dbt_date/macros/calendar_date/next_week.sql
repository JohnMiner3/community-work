{%- macro next_week(tz=None) -%}
    {{ return(adapter.dispatch("next_week", "dbt_date")(tz)) }}
{%- endmacro -%}

{%- macro default__next_week(tz=None) -%}
    {{ dbt_date.n_weeks_away(1, tz) }}
{%- endmacro -%}
