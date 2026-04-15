with base_grade as (
    select * from {{ source('sql_server', 'App_UIC_Grade') }}
)
select 
GradeID as grade_id,
Grade as grade 
from
base_grade