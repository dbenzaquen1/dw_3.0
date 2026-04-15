{% macro ton_to_cwt(per_ton_value) %}
    -- Converts a per-ton value to per-hundredweight (CWT). 1 ton = 20 CWT.
    round(({{ per_ton_value }} / 20), 2)
{% endmacro %}

{% macro calc_linear_feet(pieces, width, length, weight, gauge=0, density=0) %}
    CASE  
        WHEN {{ width }} > 0 AND {{ length }} = 0 AND {{ gauge }} > 0 AND {{ density }} > 0 AND {{ pieces }} <> 0 THEN 
            round(({{ weight }} / ({{ width }} * {{ gauge }} * {{ density }})) / 12 , 2)
        WHEN {{ width }} > 0 AND {{ length }} > 0 THEN 
            round(({{ pieces }} * {{ width }} * {{ length }}) / 144  ,2)
        ELSE 
            round(({{ pieces }} * {{ length }}),2)
    END
{% endmacro %} 
{% macro cwt_to_ton(per_cwt_value) %}
    -- Converts a per-hundredweight (CWT) value to per-ton. 1 CWT = 0.05 ton.
    round(({{ per_cwt_value }} * 20), 3)
{% endmacro %}

{% macro ton_to_pound(per_ton_value) %}
    -- Converts a per-ton value to per-pound. 1 ton = 2000 pounds.
    round(({{ per_ton_value }} / 2000), 3)
{% endmacro %}

{% macro pound_to_ton(per_pound_value) %}
    -- Converts a per-pound value to per-ton. 1 pound = 0.0005 ton.
    round(({{ per_pound_value }} * 2000), 3)
{% endmacro %}

{% macro cwt_to_pound(per_cwt_value) %}
    -- Converts a per-hundredweight (CWT) value to per-pound. 1 CWT = 20 pounds.
    round(({{ per_cwt_value }} / 100), 4)
{% endmacro %}

