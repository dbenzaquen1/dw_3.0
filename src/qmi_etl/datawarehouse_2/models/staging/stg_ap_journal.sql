with
    base_ap_journal as (select * from {{ ref("base_ap_journal") }}),
    stg_vendor as (select * from {{ ref("stg_vendor") }}),
    ap_comment as (select * from {{ref("base_ap_comment")}}),
    join_tables as (
        select
            base_ap_journal.*,
            case
                when discount_date = '1900-01-01' then false else true
            end as has_discount_date,
            case
                when open_amount > 0 then true else false
            end as has_outstanding_dollars,
            term_id,
            ach_number,
            default_contact_id,
            vendor_name,
            fax,
            phone,
            email,
            vendor_comment,
            discount_term_days,
            total_due_dates,
            in_inactive,
            state,
            city,
            zip_code,
            address_line_1,
            address_line_2,
            ap_comment.ap_comment
        from base_ap_journal
        left join stg_vendor on stg_vendor.vendor_id = base_ap_journal.vendor_id
        left join ap_comment on ap_comment.ap_comment_id = base_ap_journal.ap_comment_id
    )
select *
from join_tables