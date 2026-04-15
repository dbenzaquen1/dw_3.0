with gl_master_seed as (
    select * from {{ ref('seed_gl_master') }}
)
select * from gl_master_seed