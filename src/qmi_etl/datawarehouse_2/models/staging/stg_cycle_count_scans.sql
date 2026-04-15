with
    base_cycle_count_scans as (select * from {{ ref("base_codereader_scans") }}),
    codereader_users as (select * from {{ ref("codereadr_usernames_seed") }}),
    codereader_devices as (select * from {{ ref("seed_codereadr_devices") }})
select
    scan_id,
    inventory_tag,
    text_response,
    scanned_timestamp,
    received_timestamp,
    capture_type,
    codereader_users.username,
    codereader_devices.device_name,
    location
from base_cycle_count_scans
left join codereader_users on codereader_users.user_id = base_cycle_count_scans.user_id
left join
    codereader_devices
    on codereader_devices.device_id = base_cycle_count_scans.device_id
