{%- macro tomorrow(date=None, tz=None) -%}
    {{ return(adapter.dispatch("tomorrow", "dbt_date")(date, tz)) }}
{%- endmacro -%}

{%- macro default__tomorrow(date=None, tz=None) -%}
    {{ dbt_date.n_days_away(1, date, tz) }}
{%- endmacro -%}
