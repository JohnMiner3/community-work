{% macro cast_string_to_datetime2(column_name, format=None) %}
  {{ return(adapter.dispatch('cast_string_to_datetime2', 'your_project_name')(column_name, format)) }}
{% endmacro %}

{% macro default__cast_string_to_datetime2(column_name, format=None) %}
  -- This is the default implementation (e.g., for SQL Server)
  {% if format %}
    -- SQL Server often uses specific style codes with CONVERT
    -- You may need to adjust the style code (e.g., 103 for dd/mm/yyyy)
    CAST(CONVERT(datetime2, {{ column_name }}, {{ format }}) AS datetime2)
  {% else %}
    -- Assumes an ISO 8601 compatible string format (YYYY-MM-DD HH:MM:SS)
    CAST({{ column_name }} AS datetime2)
  {% endif %}
{% endmacro %}

{% macro snowflake__cast_string_to_datetime2(column_name, format=None) %}
  -- Snowflake uses TO_TIMESTAMP_* functions
  {% if format %}
    TO_TIMESTAMP({{ column_name }}, {{ format }})
  {% else %}
    -- Default to a common timestamp format if none specified
    TO_TIMESTAMP({{ column_name }}, 'YYYY-MM-DD HH24:MI:SS')
  {% endif %}
{% endmacro %}

