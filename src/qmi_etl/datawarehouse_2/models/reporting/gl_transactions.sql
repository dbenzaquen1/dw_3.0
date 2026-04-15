with gl_transactions as (
    select * from {{ ref('stg_gl_transactions') }}
)
select * from gl_transactions