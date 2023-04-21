{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('public', '_airbyte_raw_companies') }}
select
    {{ json_extract_scalar('_airbyte_data', ['name'], ['name']) }} as name,
    {{ json_extract_scalar('_airbyte_data', ['owner', 'email'], ['owner_email']) }} as owner_email,
    {{ json_extract_scalar('_airbyte_data', ['owner', 'userId'], ['owner_userId']) }} as owner_userId,
    {{ json_extract_scalar('_airbyte_data', ['owner', 'lastName'], ['owner_lastName']) }} as owner_lastName,
    {{ json_extract_scalar('_airbyte_data', ['owner', 'firstName'], ['owner_firstName']) }} as owner_firstName,
    {{ json_extract_scalar('_airbyte_data', ['owner', 'deleted'], ['owner_deleted']) }} as owner_deleted,
    {{ json_extract_scalar('_airbyte_data', ['social', 'twitter'], ['social_twitter']) }} as social_twitter,
    {{ json_extract_scalar('_airbyte_data', ['social', 'linkedin'], ['social_linkedin']) }} as social_linkedin,
    {{ json_extract_scalar('_airbyte_data', ['social', 'facebook'], ['social_facebook']) }} as social_facebook,
    {{ json_extract_scalar('_airbyte_data', ['status', 'name'], ['status_name']) }} as status_name,
    {{ json_extract_scalar('_airbyte_data', ['status', 'statusId'], ['status_statusId']) }} as status_statusId,
    {{ json_extract_scalar('_airbyte_data', ['companyId'], ['companyId']) }} as companyId,
    {{ json_extract_scalar('_airbyte_data', ['createdAt'], ['createdAt']) }} as createdAt,
    {{ json_extract_scalar('_airbyte_data', ['createdBy', 'email'], ['createdBy_email']) }} as createdBy_email,
    {{ json_extract_scalar('_airbyte_data', ['createdBy', 'userId'], ['createdBy_userId']) }} as createdBy_userId,
    {{ json_extract_scalar('_airbyte_data', ['createdBy', 'lastName'], ['createdBy_lastName']) }} as createdBy_lastName,
    {{ json_extract_scalar('_airbyte_data', ['createdBy', 'firstName'], ['createdBy_firstName']) }} as createdBy_firstName,
    {{ json_extract_scalar('_airbyte_data', ['createdBy', 'deleted'], ['createdBy_deleted']) }} as createdBy_deleted,
    {{ json_extract_scalar('_airbyte_data', ['updatedAt'], ['updatedAt']) }} as updatedAt,
    {{ json_extract_scalar('_airbyte_data', ['updatedBy', 'email'], ['updatedBy_email']) }} as updatedBy_email,
    {{ json_extract_scalar('_airbyte_data', ['updatedBy', 'userId'], ['updatedBy_userId']) }} as updatedBy_userId,
    {{ json_extract_scalar('_airbyte_data', ['updatedBy', 'lastName'], ['updatedBy_lastName']) }} as updatedBy_lastName,
    {{ json_extract_scalar('_airbyte_data', ['updatedBy', 'firstName'], ['updatedBy_firstName']) }} as updatedBy_firstName,
    {{ json_extract_scalar('_airbyte_data', ['updatedBy', 'deleted'], ['updatedBy_deleted']) }} as updatedBy_deleted,
    {{ json_extract_array('_airbyte_data', ['recruiters'], ['recruiters']) }} as recruiters,
    {{ json_extract_scalar('_airbyte_data', ['primaryAddress', 'url'], ['primaryAddress_url']) }} as primaryAddress_url,
    {{ json_extract_scalar('_airbyte_data', ['primaryAddress', 'city'], ['primaryAddress_city']) }} as primaryAddress_city,
    {{ json_extract_scalar('_airbyte_data', ['primaryAddress', 'name'], ['primaryAddress_name']) }} as primaryAddress_name,
    {{ json_extract_scalar('_airbyte_data', ['primaryAddress', 'phone'], ['primaryAddress_phone']) }} as primaryAddress_phone,
    {{ json_extract_scalar('_airbyte_data', ['primaryAddress', 'street'], ['primaryAddress_street']) }} as primaryAddress_street,
    {{ json_extract_scalar('_airbyte_data', ['primaryAddress', 'state'], ['primaryAddress_state']) }} as primaryAddress_state,
    {{ json_extract_scalar('_airbyte_data', ['primaryAddress', 'country'], ['primaryAddress_country']) }} as primaryAddress_country,
    {{ json_extract_scalar('_airbyte_data', ['primaryAddress', 'postcode'], ['primaryAddress_postcode']) }} as primaryAddress_postcode,
    {{ json_extract_scalar('_airbyte_data', ['primaryAddress', 'addressId'], ['primaryAddress_addressId']) }} as primaryAddress_addressId,
    {{ json_extract_scalar('_airbyte_data', ['primaryAddress', 'postalCode'], ['primaryAddress_postalCode']) }} as primaryAddress_postalCode,
    {{ json_extract_scalar('_airbyte_data', ['primaryAddress', 'countryCode'], ['primaryAddress_countryCode']) }} as primaryAddress_countryCode,
    {{ json_extract_scalar('_airbyte_data', ['legalName'], ['legalName']) }} as legalName,
    {{ json_extract_array('_airbyte_data', ['custom'], ['custom']) }} as custom,
    {{ json_extract_scalar('_airbyte_data', ['mainContact', 'lastName'], ['mainContact_lastName']) }} as mainContact_lastName,
    {{ json_extract_scalar('_airbyte_data', ['mainContact', 'contactId'], ['mainContact_contactId']) }} as mainContact_contactId,
    {{ json_extract_scalar('_airbyte_data', ['mainContact', 'firstName'], ['mainContact_firstName']) }} as mainContact_firstName,
    {{ json_extract_scalar('_airbyte_data', ['mainContact', 'email'], ['mainContact_email']) }} as mainContact_email,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('public', '_airbyte_raw_companies') }} as table_alias
-- companies
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

