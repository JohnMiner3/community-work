{% macro replace_null_with_empty(column_name) %}
  COALESCE({{ column_name }}, '')
{% endmacro %}