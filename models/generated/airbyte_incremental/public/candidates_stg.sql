{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "Staging",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('candidates_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'lastname',
        array_to_string('education'),
        adapter.quote('source'),
        array_to_string('skills'),
        'createdat',
        array_to_string('recruiters'),
        array_to_string('otheremail'),
        'email',
        'updatedat',
        'summary',
        'country',
        'city',
        'street',
        'countrycode',
        'postalcode',
        'postcode',
        'state',
        'updatedBy_firstName',
        'updatedBy_lastName',
        'updatedBy_UserId',
        'updatedBy_email', 
        'twitter',
        'linkedin',
        'facebook',
        'emergencycontact',
        array_to_string('custom'),
        'mobile',
        'dateofbirth',
        object_to_string('employment'),
        object_to_string('ideal'),
        object_to_string('history'),
        'employer',
        'workType',
        'position',
        'seeking',
        'salary',
        'firstname',
        boolean_to_string('unsubscribed'),
        'phone',
        'createdby_firstName',
        'createdby_lastName',
        'createdby_UserId', 
        'createdby_email',          
        'salutation',
        'candidateid',
        'emergencyphone',
        'status_name', 
        object_to_string(adapter.quote('statistics')),
        array_to_string('applications'),
    ]) }} as _airbyte_candidates_hashid,
    tmp.*
from {{ ref('candidates_ab2') }} tmp
-- candidates
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

