with export_workstation as (
    select * from {{ source('sql_server', 'WorkStations') }}
)

select
    WorkStationID as workstation_id,
    Description as work_station_name
from export_workstation