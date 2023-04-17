{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_public",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('candidates_custom_ab1') }}
select
    _airbyte_candidates_hashid,
    candidateid,
    cast({{ adapter.quote('name') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('name') }},
    cast({{ adapter.quote('type') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('type') }},
    {{ adapter.quote('value') }},
    cast(fieldid as {{ dbt_utils.type_bigint() }}) as fieldid,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('candidates_custom_ab1') }}
-- custom at Candidates/custom
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

