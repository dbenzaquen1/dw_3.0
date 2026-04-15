with base_gauge as (
    select * from {{ source('sql_server', 'App_UIC_Gauge') }}
)

select 
GaugeID as gauge_id,
Maximum as gauge_max,
Minimum as gauge_min,
Nominal as gauge_nom,
GaugeCode as gauge_code,
Description as gauge_description
from base_gauge