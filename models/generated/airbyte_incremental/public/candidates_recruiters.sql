{{ config(
    indexes = [{'columns':['_airbyte_unique_key'],'unique':True}],
    unique_key = "_airbyte_unique_key",
    schema = "public",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('candidates_recruiters_scd') }}
select distinct on (_airbyte_unique_key)
    _airbyte_unique_key,
    candidateid,
    updatedat,
    userid,
    email,
    firstName,
    lastName,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_recruiters_hashid
from {{ ref('candidates_recruiters_scd') }}
-- recruiters from {{ source('public', '_airbyte_raw_candidates') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}