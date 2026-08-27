{%- macro now(tz=None) -%}
    {{ return(adapter.dispatch("now", "dbt_date")(tz)) }}
{%- endmacro -%}

{%- macro default__now(tz=None) -%}
    {{ dbt_date.convert_timezone(dbt.current_timestamp(), tz) }}
{%- endmacro -%}
