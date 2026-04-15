with export_gl_source as (
select * from {{ source('sql_server', 'GLAdjSource') }}
),
rename as (
    select 
    GLSrcID as gl_source_id,
    SourceGLSrcID as source_gl_number,

    Updated as is_updated,
    Pending as is_pending,
    Deleted as is_deleted,
    Inactive as is_inactive,


    Comments as ajustment_comment,
    Description as ajustment_description,

    cast(GLSrcDate as date)  as gl_source_date,
    cast(ReversalDt as date) as gl_reversal_date,
    cast(ClearedDate as date ) as gl_cleared_date,
    cast (PostingDate as date) as posting_date,
    {{extract_user('ClearedUserInfo')}} as cleared_username,
    {{extract_workstation('ClearedUserInfo')}} as cleared_workstation,
    {{extract_user('Created_UserInfo')}} as created_username,
    {{extract_workstation('Created_UserInfo')}} as created_workstation,

    TotalAdjAmt as total_ajustment_amount
    from export_gl_source
)
    




select * from rename