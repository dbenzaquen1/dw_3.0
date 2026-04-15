with export_vendor as  (select * from {{ source('sql_server', 'Vendor') }}),
reformat_cols as 
(
    select
    VendorID as vendor_id,
    AddressID as address_id,
    APCommentID as ap_comment_id,
    Terms as term_id,
    ACHNumber as ach_number,
    DefaultContact as default_contact_id,
    Name as vendor_name,
    Fax as fax,
    Phone as phone,
    eMail as email,
    Comments as vendor_comment,
    DiscDays as discount_term_days,
    TotalDays as total_due_dates,


    Inactive as in_inactive
    from 
    export_vendor


    


)
select * from reformat_cols