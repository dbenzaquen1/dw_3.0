with code_reader_export as (
    select * from quality-metals-prod.zapier_import.cycle_count_import
)
,rename_col as (
    select
    Scanid as scan_id,
    Tid as inventory_tag,
    Sid as s_id,
    Udid as ud_id,
    Userid as user_id,
    Deviceid as device_id,
    Text as text_response,
    Timestamp as timestamp,
    Scanned_at_utc as scanned_timestamp,
    Recived_at_utc as received_timestamp,
    Capture_type as capture_type,
    Timezone as timezone,
    location
     from code_reader_export
)
select * from rename_col