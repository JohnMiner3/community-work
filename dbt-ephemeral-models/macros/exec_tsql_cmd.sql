{% macro exec_tsql_cmd(my_sql) %}

{% if execute %}
  {{ log("Start - execute sql command ( " ~ my_sql ~ " ). ", info=True) }}
  {% set results = run_query(my_sql) %}
  {{ log("End - execute sql command. ", info=True) }}
{% endif %}

{% endmacro %}