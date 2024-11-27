{% macro snapshot_check_all_get_existing_columns(node, target_exists, check_cols_config) -%}
    
    {{ return((false, check_cols_config)) }}

{%- endmacro %}