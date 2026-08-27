{%- macro today(tz=None) -%}
    {{ return(adapter.dispatch("today", "dbt_date")(tz)) }}
{%- endmacro -%}

{%- macro default__today(tz=None) -%}
    cast({{ dbt_date.now(tz) }} as date)
{%- endmacro -%}
