with export_comments as (
select * from {{ source('sql_server', 'Comments') }}
)
select 
    CommentID as comment_id,
    Comment as comment_text
 from export_comments 