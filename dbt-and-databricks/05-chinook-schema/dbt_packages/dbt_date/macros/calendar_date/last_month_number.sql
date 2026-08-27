{%- macro last_month_number(tz=None) -%}
    {{ return(adapter.dispatch("last_month_number", "dbt_date")(tz)) }}
{%- endmacro -%}

{%- macro default__last_month_number(tz=None) -%}
    {{ dbt_date.date_part("month", dbt_date.last_month(tz)) }}
{%- endmacro -%}
