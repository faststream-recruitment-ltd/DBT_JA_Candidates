{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('candidates_applications_ab1') }}
select
    candidateid,
    appid,
    cast(candidate_updatedat as {{ dbt_utils.type_string() }}) as candidate_updatedat,
    cast(jobid as {{ dbt_utils.type_bigint() }}) as jobid,
    cast(job_title as {{ dbt_utils.type_string() }}) as job_title,
    cast(owner_userId as {{ dbt_utils.type_string() }}) as owner_userId,
    cast(owner_lastName as {{ dbt_utils.type_string() }}) as owner_lastName,
    cast(owner_firstName as {{ dbt_utils.type_string() }}) as owner_firstName,
    cast(owner_email as {{ dbt_utils.type_string() }}) as owner_email,
    cast(application_source as {{ dbt_utils.type_string() }}) as application_source,
    cast(application_status as {{ dbt_utils.type_string() }}) as application_status,
    cast(application_statusId as {{ dbt_utils.type_string() }}) as application_statusId,
    cast(application_rejected as {{ dbt_utils.type_string() }}) as application_rejected,
    cast(application_workflow_step as {{ dbt_utils.type_string() }}) as application_workflow_step,
    cast(application_workflow_stage as {{ dbt_utils.type_string() }}) as application_workflow_stage,
    cast(application_workflow_progress as {{ dbt_utils.type_string() }}) as application_workflow_progress,
    cast(application_workflow_index as {{ dbt_utils.type_string() }}) as application_workflow_index,
    cast(application_createdAt as {{ dbt_utils.type_string() }}) as application_createdAt,
    cast(application_updatedAt as {{ dbt_utils.type_string() }}) as application_updatedAt,
    cast(updatedBy_userId as {{ dbt_utils.type_string() }}) as updatedBy_userId,
    cast(updatedBy_lastName as {{ dbt_utils.type_string() }}) as updatedBy_lastName,
    cast(updatedBy_firstName as {{ dbt_utils.type_string() }}) as updatedBy_firstName,
    cast(updatedBy_email as {{ dbt_utils.type_string() }}) as updatedBy_email,
    cast(job_Reference as {{ dbt_utils.type_string() }}) as job_Reference,
    cast(application_rating as {{ dbt_utils.type_string() }}) as application_rating,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('candidates_applications_ab1') }}
-- applications at Candidates/applications
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}
