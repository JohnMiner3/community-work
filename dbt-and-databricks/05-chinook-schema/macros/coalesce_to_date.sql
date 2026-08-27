{% macro coalesce_to_date(column_name, default_date="current_date()") %}
  COALESCE(
    {{ column_name }},
    {{ default_date }}
  )
{% endmacro %}