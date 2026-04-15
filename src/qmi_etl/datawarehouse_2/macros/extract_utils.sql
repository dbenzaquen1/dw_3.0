{% macro extract_user(raw_user) %}
    -- Extracts the user information from the entaio raw user string
    REGEXP_EXTRACT({{ raw_user }}, r'User: [^\\]+\\([^ ]+)')
{% endmacro %}

{% macro extract_workstation(raw_user) %}
    -- Extracts the workstation information from the entaio raw user string
    REGEXP_EXTRACT({{ raw_user }}, r'Workstation: ([^ ]+)')
{% endmacro %} 