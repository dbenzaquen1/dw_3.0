with seed_galvannealed_cost as (
    select * from {{ ref('seed_galvannealed_cost') }}
),

base_galvannealed_cost as (
    select 
        location,
        cost,
        effective_start_timestamp,
        cast(effective_end_timestamp as datetime) as effective_end_timestamp,
        'GALVANNEALED' as steel_type
    from seed_galvannealed_cost
)

select * from base_galvannealed_cost 