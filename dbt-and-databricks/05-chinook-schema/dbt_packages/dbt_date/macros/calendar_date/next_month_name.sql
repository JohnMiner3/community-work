{%- macro next_month_name(short=True, tz=None) -%}
    {{ return(adapter.dispatch("next_month_name", "dbt_date")(short, tz)) }}
{%- endmacro -%}

{%- macro default__next_month_name(short=True, tz=None) -%}
    {{ dbt_date.month_name(dbt_date.next_month(tz), short=short) }}
{%- endmacro -%}
