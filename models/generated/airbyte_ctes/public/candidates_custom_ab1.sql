{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_public",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ ref('candidates_scd') }}
{{ unnest_cte(ref('candidates_scd'), 'candidates', 'custom') }}
select
    _airbyte_candidates_hashid,
    candidateid,
    {{ json_extract_scalar(unnested_column_value('custom'), ['name'], ['name']) }} as {{ adapter.quote('name') }},
    {{ json_extract_scalar(unnested_column_value('custom'), ['type'], ['type']) }} as {{ adapter.quote('type') }},
    {{ json_extract('', unnested_column_value('custom'), ['value']) }} as {{ adapter.quote('value') }},
    {{ json_extract_scalar(unnested_column_value('custom'), ['fieldId'], ['fieldId']) }} as fieldid,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('candidates_scd') }} as table_alias
-- custom at Candidates/custom
{{ cross_join_unnest('candidates', 'custom') }}
where 1 = 1
and custom is not null
{{ incremental_clause('_airbyte_emitted_at', this) }}

