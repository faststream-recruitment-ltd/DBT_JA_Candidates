{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('public', '_airbyte_raw_candidates') }}
select
    {{ json_extract_scalar('_airbyte_data', ['candidateId'], ['candidateId']) }} as candidateid,
    jsonb_array_elements(jsonb_extract_path(_airbyte_data, 'applications'))->>'applicationId' appid,    
    {{ json_extract_scalar('_airbyte_data', ['updatedAt'], ['updatedAt']) }} as candidate_updatedat,
    jsonb_extract_path_text(jsonb_array_elements(jsonb_extract_path(_airbyte_data, 'applications')), 'job', 'jobId') as jobid,
    jsonb_extract_path_text(jsonb_array_elements(jsonb_extract_path(_airbyte_data, 'applications')), 'job', 'jobTitle') as job_title,
    jsonb_extract_path_text(jsonb_array_elements(jsonb_extract_path(_airbyte_data, 'applications')), 'owner', 'userId') as owner_userId,
    jsonb_extract_path_text(jsonb_array_elements(jsonb_extract_path(_airbyte_data, 'applications')), 'owner', 'lastName') as owner_lastName,
    jsonb_extract_path_text(jsonb_array_elements(jsonb_extract_path(_airbyte_data, 'applications')), 'owner', 'firstName') as owner_firstName,
    jsonb_extract_path_text(jsonb_array_elements(jsonb_extract_path(_airbyte_data, 'applications')), 'owner', 'email') as owner_email,
    jsonb_array_elements(jsonb_extract_path(_airbyte_data, 'applications'))->>'source' as application_source,
    jsonb_extract_path_text(jsonb_array_elements(jsonb_extract_path(_airbyte_data, 'applications')), 'status', 'name') as application_status,
    jsonb_extract_path_text(jsonb_array_elements(jsonb_extract_path(_airbyte_data, 'applications')), 'status', 'statusId') as application_statusId,
    jsonb_extract_path_text(jsonb_array_elements(jsonb_extract_path(_airbyte_data, 'applications')), 'status', 'rejected') as application_rejected,
    jsonb_extract_path_text(jsonb_array_elements(jsonb_extract_path(_airbyte_data, 'applications')), 'status', 'workflow', 'step') as application_workflow_step,
    jsonb_extract_path_text(jsonb_array_elements(jsonb_extract_path(_airbyte_data, 'applications')), 'status', 'workflow', 'stage') as application_workflow_stage,
    jsonb_extract_path_text(jsonb_array_elements(jsonb_extract_path(_airbyte_data, 'applications')), 'status', 'workflow', 'progress') as application_workflow_progress,
    jsonb_extract_path_text(jsonb_array_elements(jsonb_extract_path(_airbyte_data, 'applications')), 'status', 'workflow', 'stageIndex') as application_workflow_index,
    jsonb_array_elements(jsonb_extract_path(_airbyte_data, 'applications'))->>'createdAt' as application_createdAt,
    jsonb_array_elements(jsonb_extract_path(_airbyte_data, 'applications'))->>'updatedAt' as application_updatedAt,
    jsonb_extract_path_text(jsonb_array_elements(jsonb_extract_path(_airbyte_data, 'applications')), 'updatedBy', 'userId') as updatedBy_userId,
    jsonb_extract_path_text(jsonb_array_elements(jsonb_extract_path(_airbyte_data, 'applications')), 'updatedBy', 'lastName') as updatedBy_lastName,
    jsonb_extract_path_text(jsonb_array_elements(jsonb_extract_path(_airbyte_data, 'applications')), 'updatedBy', 'firstName') as updatedBy_firstName,
    jsonb_extract_path_text(jsonb_array_elements(jsonb_extract_path(_airbyte_data, 'applications')), 'updatedBy', 'email') as updatedBy_email,
    jsonb_array_elements(jsonb_extract_path(_airbyte_data, 'applications'))->>'jobReference' as job_Reference,
    jsonb_array_elements(jsonb_extract_path(_airbyte_data, 'applications'))->>'rating' as application_rating,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('public', '_airbyte_raw_candidates') }} as table_alias
-- candidate_applications
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}