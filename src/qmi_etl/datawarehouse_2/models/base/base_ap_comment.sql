with export_comments as (
select * from {{ source('sql_server', 'APComment') }}
)
select Comment as ap_comment, APCommentID as ap_comment_id from export_comments