with base_gl_master as (
    select * from {{ ref('base_gl_master') }}
)
select * from base_gl_master