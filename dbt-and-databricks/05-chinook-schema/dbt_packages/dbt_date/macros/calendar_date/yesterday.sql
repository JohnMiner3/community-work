{%- macro yesterday(date=None, tz=None) -%}
    {{ return(adapter.dispatch("yesterday", "dbt_date")(date, tz)) }}
{%- endmacro -%}

{%- macro default__yesterday(date=None, tz=None) -%}
    {{ dbt_date.n_days_ago(1, date, tz) }}
{%- endmacro -%}
