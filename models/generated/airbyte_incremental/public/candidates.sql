{{ config(
    indexes = [{'columns':['_airbyte_unique_key'],'unique':True}],
    unique_key = "_airbyte_unique_key",
    schema = "JobAdder",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('candidates_scd') }}
select
    _airbyte_unique_key,
    lastname,
    education,
    {{ adapter.quote('source') }},
    skills,
    createdat,
    recruiters,
    otheremail,
    email,
    updatedat,
    summary,
    country,
    city,
    street,
    countrycode,
    postalcode,
    postcode,
    state,    
    updatedBy_firstName,
    updatedBy_lastName,
    updatedBy_UserId,
    updatedBy_email, 
    twitter,
    linkedin,
    facebook,
    emergencycontact,
    custom,
    mobile,
    dateofbirth,
    employment,
    ideal,
    history,
    employer,
    workType,
    position,
    salary,   
    seeking,
    firstname,
    unsubscribed,
    phone,
    createdby_firstName,
    createdby_lastName,
    createdby_UserId, 
    createdby_email,       
    salutation,
    candidateid,
    emergencyphone,
    status_name,
    {{ adapter.quote('statistics') }},
    applications,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_candidates_hashid
from {{ ref('candidates_scd') }}
-- candidates from {{ source('public', '_airbyte_raw_candidates') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

