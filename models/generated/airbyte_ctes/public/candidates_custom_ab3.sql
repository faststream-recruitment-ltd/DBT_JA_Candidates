{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_public",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('candidates_custom_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        '_airbyte_candidates_hashid',
        'candidateId',
        adapter.quote('name'),
        adapter.quote('type'),
        adapter.quote('value'),
        'fieldid',
    ]) }} as _airbyte_custom_hashid,
    tmp.*
from {{ ref('candidates_custom_ab2') }} tmp
-- custom at Candidates/custom
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

