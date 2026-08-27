{% macro date_part(datepart, date) -%}
    {{ adapter.dispatch("date_part", "dbt_date")(datepart, date) }}
{%- endmacro %}

{% macro default__date_part(datepart, date) -%}
    date_part('{{ datepart }}', {{ date }})
{%- endmacro %}

{% macro bigquery__date_part(datepart, date) -%}
    extract({{ datepart }} from {{ date }})
{%- endmacro %}

{% macro trino__date_part(datepart, date) -%}
    extract({{ datepart }} from {{ date }})
{%- endmacro %}

{# sqlfmt disabled below: ClickHouse function names are case-sensitive #}
-- fmt: off
{% macro clickhouse__date_part(datepart, date) -%}
    {%- set datepart = datepart | lower -%}
    {#
        EXTRACT handles year/quarter/month/day/hour/minute/second/epoch. The rest need
        ClickHouse functions: dayofweek/dayofyear/isoweek aren't valid EXTRACT units, and
        EXTRACT(week) is ISO-numbered whereas week_of_year expects the non-ISO week.
    #}
    {%- if datepart in ["dayofweek", "dow"] -%}
        {# Sunday = 0 ... Saturday = 6, matching the default__ macros' expectations #}
        toDayOfWeek({{ date }}, 2)
    {%- elif datepart in ["dayofyear", "doy"] -%} toDayOfYear({{ date }})
    {%- elif datepart == "isoweek" -%} toISOWeek({{ date }})
    {%- elif datepart == "week" -%} toWeek({{ date }}, 0)
    {%- else -%} extract({{ datepart }} from {{ date }})
    {%- endif -%}
{%- endmacro %}
-- fmt: on
