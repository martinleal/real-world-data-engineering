{% macro generate_schema_name(custom_schema_name, node) %}
    {# If a custom schema is provided, just use it directly #}
    {% if custom_schema_name is not none %}
        {{ custom_schema_name | trim }}
    {% else %}
        {{ target.schema }}
    {% endif %}
{% endmacro %}
