{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('candidates_recruiters_ab1') }}
select
    cast(updatedat as {{ dbt_utils.type_string() }}) as updatedat,
    candidateid,
    cast(userid as {{ dbt_utils.type_string() }}) as userid,
    cast(email as {{ dbt_utils.type_string() }}) as email,
    cast(firstName as {{ dbt_utils.type_string() }}) as firstName,
    cast(lastName as {{ dbt_utils.type_string() }}) as lastName,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('candidates_recruiters_ab1') }}
-- custom at candidates/recruiters
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}