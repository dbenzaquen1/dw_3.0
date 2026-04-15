with inventory_history as (
    select * from {{ref("inventory_history")}}
)
, inventory as (
    select * from {{ref("inventory")}}
)
, item_master as (
    select * from {{ ref('item_master') }}
)
, ranking_PR_inventory_transactions as (
    select 
        *,
        row_number() over (
            partition by first_inventory_id 
            order by submit_date_time, history_id desc
        ) as rn
    from inventory_history
    where source_type = 'PR'
)
, oldest_PR_transactions as (
    select 
        history_id,
        inventory_id,
        inventory_tag,
        first_inventory_id,
        po_number,
        location_name,
        notes,
        flaw,
        weight,
        width,
        length,
        username,
        pieces,
        feet,
        source_type,
        submit_date_time,
        received_date,
        price
    from ranking_PR_inventory_transactions
    where rn = 1
)
, ranking_RA_inventory_transactions as (
    select 
        *,
        row_number() over (
            partition by first_inventory_id 
            order by submit_date_time desc, history_id desc
        ) as rn
    from inventory_history
    where source_type = 'RA' 
)
, latest_RA_transactions as (
    select 
        history_id,
        inventory_id,
        inventory_tag,
        first_inventory_id,
        po_number,
        location_name,
        notes,
        flaw,
        weight,
        width,
        length,
        username,
        pieces,
        feet,
        source_type,
        submit_date_time,
        received_date,
        price
    from ranking_RA_inventory_transactions
    where rn = 1
)
, inventory_first_id_lookup as (
    select 
        inventory_id,
        coalesce(first_inventory_id, inventory_id) as first_inventory_id
    from inventory_history
    group by inventory_id, first_inventory_id
)

select 
    inventory.inventory_id, 
    inventory_first_id_lookup.first_inventory_id,
    round(oldest_PR_transactions.price, 2) as pr_price, 
    round(latest_RA_transactions.price, 2) as ra_price,
    inventory.receiving_date,
    oldest_PR_transactions.submit_date_time as pr_submit_date_time,
    latest_RA_transactions.submit_date_time as ra_submit_date_time
from inventory
left join inventory_first_id_lookup on inventory.inventory_id = inventory_first_id_lookup.inventory_id
left join oldest_PR_transactions on 
    case 
        when inventory_first_id_lookup.first_inventory_id = inventory.inventory_id 
        then inventory.inventory_id 
        else inventory_first_id_lookup.first_inventory_id 
    end = oldest_PR_transactions.first_inventory_id
left join latest_RA_transactions on 
    case 
        when inventory_first_id_lookup.first_inventory_id = inventory.inventory_id 
        then inventory.inventory_id 
        else inventory_first_id_lookup.first_inventory_id 
    end = latest_RA_transactions.first_inventory_id
left join item_master on inventory.item_id_width = item_master.item_id_width
where 
    oldest_PR_transactions.price is not null 
    and latest_RA_transactions.price is not null
    and abs(
        (oldest_PR_transactions.price - latest_RA_transactions.price) / 
        nullif(oldest_PR_transactions.price, 0) * 100
    ) > 10
    and item_master.item_grade not like '%3XXX%'
order by inventory.receiving_date desc 