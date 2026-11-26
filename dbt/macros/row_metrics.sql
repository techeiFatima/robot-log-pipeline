{# Per-row summary expressions reused by the mart models. #}

{% macro avg_joint_temperature() -%}
    (temperature_j0 + temperature_j1 + temperature_j2
     + temperature_j3 + temperature_j4 + temperature_j5) / 6.0
{%- endmacro %}

{% macro max_joint_temperature() -%}
    greatest(temperature_j0, temperature_j1, temperature_j2,
             temperature_j3, temperature_j4, temperature_j5)
{%- endmacro %}

{% macro total_abs_current() -%}
    (abs(current_j0) + abs(current_j1) + abs(current_j2)
     + abs(current_j3) + abs(current_j4) + abs(current_j5))
{%- endmacro %}

{% macro total_abs_speed() -%}
    (abs(speed_j0) + abs(speed_j1) + abs(speed_j2)
     + abs(speed_j3) + abs(speed_j4) + abs(speed_j5))
{%- endmacro %}

{% macro count_if(condition) -%}
    sum(case when {{ condition }} then 1 else 0 end)
{%- endmacro %}
