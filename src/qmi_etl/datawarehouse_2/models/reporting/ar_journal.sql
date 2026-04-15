with source as (
    select * from {{ ref('stg_ar_journal') }}
),
ar_with_aging as (
    select
        *,
        case
            when is_open_invoice and current_date<due_date then 'Upcoming'
            when is_open_invoice and date_diff(current_date, due_date, day) between 0 and 30 then '0-30'
            when is_open_invoice and date_diff(current_date, due_date, day) between 31 and 90 then '31-90'
            when is_open_invoice and date_diff(current_date, due_date, day) between 91 and 180 then '91-180'
            when is_open_invoice and date_diff(current_date, due_date, day) between 181 and 365 then '181-365'
            when is_open_invoice and date_diff(current_date, due_date, day) > 365 then '366+'
            
        end as aging_bucket
    from source
)

select * from ar_with_aging