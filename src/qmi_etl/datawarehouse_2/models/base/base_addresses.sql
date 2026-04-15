with base_ad as (
select * from {{ source('sql_server', 'Addresses') }}
)
select 
AddressID as address_id,
State as state,
City as city,
Zip as zip_code,
Address1 as address_line_1,
Address2 as address_line_2,
from base_ad