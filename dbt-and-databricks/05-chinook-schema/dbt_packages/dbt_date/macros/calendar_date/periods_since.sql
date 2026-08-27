{%- macro periods_since(date_col, period_name="day", tz=None) -%}
    {{
        return(
            adapter.dispatch("periods_since", "dbt_date")(date_col, period_name, tz)
        )
    }}
{%- endmacro -%}

{%- macro default__periods_since(date_col, period_name="day", tz=None) -%}
    {{ dbt.datediff(date_col, dbt_date.now(tz), period_name) }}
{%- endmacro -%}
