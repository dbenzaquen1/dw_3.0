{% macro week_end_date(week_string) %}
    -- Returns the ISO week end date (Sunday) for a string like '2020W26'
    PARSE_DATE('%G-W%V-%u', CONCAT(SUBSTR({{ week_string }}, 1, 4), '-W', SUBSTR({{ week_string }}, 6, 2), '-7'))
{% endmacro %}

{% macro month_end_date(month_string) %}
    -- Returns the last date of the month for a string like '2023M10', '2021M12', '2020M1', etc.
    LAST_DAY(
        PARSE_DATE(
            '%YM%m',
            CONCAT(
                SPLIT(TRIM({{ month_string }}), 'M')[0],
                'M',
                LPAD(SPLIT(TRIM({{ month_string }}), 'M')[1], 2, '0')
            )
        ),
        MONTH
    )
{% endmacro %}

{% macro quarter_end_date(quarter_string) %}
    -- Returns the last date of the quarter for a string like '2020Q1', '2020Q4', etc.
    DATE_SUB(
        DATE_ADD(
            PARSE_DATE(
                '%YQ%q',
                {{ quarter_string }}
            ),
            INTERVAL 1 QUARTER
        ),
        INTERVAL 1 DAY
    )
{% endmacro %} 
{% macro year_end_date(year_string) %}
    -- Returns the last date of the year for a string like '2025'
    PARSE_DATE('%Y-%m-%d', CONCAT({{ year_string }}, '-12-31'))
{% endmacro %}

{% macro combine_date_and_time(date_var, time_var) %}
    TIMESTAMP(
        DATETIME(
            DATE({{ date_var }}),
            COALESCE(CAST({{ time_var }} AS TIME), TIME(0, 0, 0))
        )
    )
{% endmacro %}