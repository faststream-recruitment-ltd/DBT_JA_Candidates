{{ config(
    indexes = [{'columns':['_airbyte_unique_key'],'unique':True}],
    unique_key = "_airbyte_unique_key",
    schema = "JobAdder",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('candidates_applications_scd') }}
select
    _airbyte_unique_key,
    candidateid,
    appid,    
    candidate_updatedat,
    jobid,
    job_title,
    owner_userId,
    owner_lastName,
    owner_firstName,
    owner_email,
    application_source,
    application_status,
    application_statusId,
    application_rejected,
    application_workflow_step,
    application_workflow_stage,
    application_workflow_progress,
    application_workflow_index,
    application_createdAt,
    application_updatedAt,
    updatedBy_userId,
    updatedBy_lastName,
    updatedBy_firstName,
    updatedBy_email,
    job_Reference,
    application_rating,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_applications_hashid
from {{ ref('candidates_applications_scd') }}
-- custom from {{ source('public', '_airbyte_raw_candidates') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}