with stg_gl_master as (
    select * from {{ ref('stg_gl_master') }}
)
select * from stg_gl_master