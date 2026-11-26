{#
  Parse the source timestamp text (already stripped of stray quotes and the
  trailing 'Z') into a timestamp without time zone, meaning UTC.
  DuckDB: a plain cast parses ISO 8601 with a 'T'.
  Snowflake: give the format explicitly instead of relying on AUTO detection.
#}
{% macro parse_iso_utc_ts(expr) -%}
    {{ return(adapter.dispatch('parse_iso_utc_ts', 'robot_logs')(expr)) }}
{%- endmacro %}

{% macro default__parse_iso_utc_ts(expr) -%}
    cast({{ expr }} as {{ dbt.type_timestamp() }})
{%- endmacro %}

{% macro snowflake__parse_iso_utc_ts(expr) -%}
    to_timestamp_ntz({{ expr }}, 'YYYY-MM-DD"T"HH24:MI:SS.FF3')
{%- endmacro %}
