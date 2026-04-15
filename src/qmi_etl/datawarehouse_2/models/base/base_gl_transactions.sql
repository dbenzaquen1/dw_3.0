WITH export_gl_trans AS (
    select * from {{ source('sql_server', 'GLTransactions') }}
),

flatten AS (
    SELECT 
        GLTransID AS gl_transaction_id,
        GLNumber AS gl_full,
        SPLIT(GLNumber, "-")[SAFE_OFFSET(0)] AS gl_number,
        IF(
            ARRAY_LENGTH(SPLIT(GLNumber, "-")) > 1,
            SPLIT(GLNumber, "-")[SAFE_OFFSET(1)],
            NULL
        ) AS gl_group,
        SourceID AS source_id,
        {{extract_user('UserInfo') }} AS username,
        {{extract_workstation('UserInfo')}} AS workstation,
        Amount AS transaction_amount,
        DATE(TranDt) AS transaction_date,
        DATE(PostDt) AS post_date,
        GLPeriod AS gl_period
    FROM export_gl_trans
) 

SELECT * FROM flatten
