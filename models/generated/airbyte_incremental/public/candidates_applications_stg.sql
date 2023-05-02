{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "Staging",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('candidates_applications_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'candidateid',
        'appid',
        'candidate_updatedat',
        'jobid',
        'job_title',
        'owner_userId',
        'owner_lastName',
        'owner_firstName',
        'owner_email',
        'application_source',
        'application_status',
        'application_statusId',
        'application_rejected',
        'application_workflow_step',
        'application_workflow_stage',
        'application_workflow_progress',
        'application_workflow_index',
        'application_createdAt',
        'application_updatedAt',
        'updatedBy_userId',
        'updatedBy_lastName',
        'updatedBy_firstName',
        'updatedBy_email',
        'job_Reference',
        'application_rating',
    ]) }} as _airbyte_applications_hashid,
    tmp.*
from {{ ref('candidates_applications_ab2') }} tmp
-- applications at Candidates/applications
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}


