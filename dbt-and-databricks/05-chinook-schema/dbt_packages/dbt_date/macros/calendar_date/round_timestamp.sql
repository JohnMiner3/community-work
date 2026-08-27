{% macro round_timestamp(timestamp) %}
    {{ return(adapter.dispatch("round_timestamp", "dbt_date")(timestamp)) }}
{% endmacro %}

{% macro default__round_timestamp(timestamp) %}
    {{ dbt.date_trunc("day", dbt.dateadd("hour", 12, timestamp)) }}
{% endmacro %}
