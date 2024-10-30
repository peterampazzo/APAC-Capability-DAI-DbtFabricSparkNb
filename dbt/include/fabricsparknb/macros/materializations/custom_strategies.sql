{% macro get_incremental_delete_reload_sql(arg_dict) %}

  {% do return(delete_reload_macro_with_sql(arg_dict["target_relation"], arg_dict["temp_relation"], arg_dict["unique_key"], arg_dict["dest_columns"], arg_dict["incremental_predicates"])) %}

{% endmacro %}


{% macro delete_reload_macro_with_sql(target_relation, temp_relation, unique_key, dest_columns, incremental_predicates) %}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    DELETE FROM {{ target_relation }}
    insert into {{ target_relation }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ temp_relation }}
    );

{% endmacro %}




{% macro dbt_spark_validate_get_incremental_strategy(raw_strategy, file_format) %}
  {#-- Validate the incremental strategy #}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ raw_strategy }}
    Expected one of: 'append', 'merge', 'insert_overwrite', 'delete_reload'
  {%- endset %}

  {% set invalid_merge_msg -%}
    Invalid incremental strategy provided: {{ raw_strategy }}
    You can only choose this strategy when file_format is set to 'delta' or 'iceberg' or 'hudi'
  {%- endset %}

  {% set invalid_insert_overwrite_endpoint_msg -%}
    Invalid incremental strategy provided: {{ raw_strategy }}
    You cannot use this strategy when connecting via endpoint
    Use the 'append' or 'merge' strategy instead
  {%- endset %}

  {% if raw_strategy not in ['append', 'merge', 'insert_overwrite','delete_reload'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {%-else %}
    {% if raw_strategy == 'merge' and file_format not in ['delta', 'iceberg', 'hudi'] %}
      {% do exceptions.raise_compiler_error(invalid_merge_msg) %}
    {% endif %}
    {% if raw_strategy == 'insert_overwrite' and file_format not in ['delta', 'iceberg', 'hudi'] %} --  and target.endpoint
      {% do exceptions.raise_compiler_error(invalid_insert_overwrite_endpoint_msg) %}
    {% endif %}
  {% endif %}

  {% do return(raw_strategy) %}
{% endmacro %}


{% macro dbt_spark_get_incremental_sql(strategy, source, target, existing, unique_key, incremental_predicates) %}
  {%- if strategy == 'append' -%}
    {#-- insert new records into existing table, without updating or overwriting #}
    {{ get_insert_into_sql(source, target) }}
  {%- elif strategy == 'delete_reload' -%}
    {#-- insert new records into existing table, without updating or overwriting #}
    DELETE FROM {{ target }};
    {{ get_insert_into_sql(source, target) }}
  {%- elif strategy == 'insert_overwrite' -%}
    {#-- insert statements don't like CTEs, so support them via a temp view #}
    {{ get_insert_overwrite_sql(source, target, existing) }}
  {%- elif strategy == 'merge' -%}
  {#-- merge all columns for datasources which implement MERGE INTO (e.g. databricks, iceberg) - schema changes are handled for us #}
    {{ get_merge_sql(target, source, unique_key, dest_columns=none, incremental_predicates=incremental_predicates) }}
  {%- else -%}
    {% set no_sql_for_strategy_msg -%}
      No known SQL for the incremental strategy provided: {{ strategy }}
    {%- endset %}
    {%- do exceptions.raise_compiler_error(no_sql_for_strategy_msg) -%}
  {%- endif -%}

{% endmacro %}
