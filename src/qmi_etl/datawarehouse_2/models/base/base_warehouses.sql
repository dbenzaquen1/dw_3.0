with export_warehouses as (
    select * from {{ source('sql_server', 'Warehouse') }}
)

select
WarehouseID as warehouse_id,
Description as warehouse_name,
AddressID as address_id,
Phone as warehouse_phone,
Fax as warehouse_fax,
eMail as warehouse_email
from export_warehouses
