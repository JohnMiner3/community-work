{%- macro last_month_name(short=True, tz=None) -%}
    {{ return(adapter.dispatch("last_month_name", "dbt_date")(short, tz)) }}
{%- endmacro -%}

{%- macro default__last_month_name(short=True, tz=None) -%}
    {{ dbt_date.month_name(dbt_date.last_month(tz), short=short) }}
{%- endmacro -%}
