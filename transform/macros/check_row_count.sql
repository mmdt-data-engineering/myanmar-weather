{% macro check_row_count(table_name) %}
    {% set results = run_query("select count(*) as row_count from " ~ table_name) %}
    {% if execute %}
        {{ print(results.columns[0].values()[0]) }}
    {% endif %}
{% endmacro %}