{%- macro next_month_number(tz=None) -%}
    {{ return(adapter.dispatch("next_month_number", "dbt_date")(tz)) }}
{%- endmacro -%}

{%- macro default__next_month_number(tz=None) -%}
    {{ dbt_date.date_part("month", dbt_date.next_month(tz)) }}
{%- endmacro -%}
