with base_order_header as(
    select * from {{ source('sql_server', 'SOQT') }}
)
, flattened_order_header as 
(
    select 
            -- ids
            SOQTID as order_id,
            QuoteNum as qoute_number,
            ShipToID as ship_to_id,
            CustomerID as customer_id,
            CustomerPO as customer_po,
            Salesperson1 as salesperson_id_1,
            Salesperson2 as salesperson_id_2,
        

            -- fact
            CostAmt as order_cost_amount,
            MaterialCost as material_cost,


            -- dim
            ShipVia as shippment_method,
            OrderNum as order_number,
            TaxCode1 as tax_state,
            ShipToZip as ship_to_zip,
            ShipToCity as ship_to_city,
            ShipToState as ship_to_state,
            ShipToName as ship_to_company_name, 
            ShipToPhone as ship_to_phone,
                --Fix formating for names
             ARRAY_TO_STRING(
                 ARRAY(
                     SELECT CONCAT(UPPER(SUBSTR(word, 1, 1)), LOWER(SUBSTR(word, 2)))
                     FROM UNNEST(SPLIT(ShipToContact, ' ')) AS word
                  ), ' ') as   ship_to_contact,




            -- dates
            cast(OrderDate as date) as order_date,
            cast(QuoteDate as date) as quote_date,
            cast(OrderDueDate as date) as order_due_date,
            -- flags 
            case when Lower(Spacers) = 'yes' or Lower(Spacers) = 'y' then true else false end as has_spacer,
            Forklift as has_forklift,
            PaperWrap as has_paper_wrap,
            CreditRelease as has_credit_Release,
            ReceivingHours as receiving_hours,
            MaxSkidweight as max_skid_weight,
            CommentsPrint as printed_comment,
            BlanketOrder as is_blanket_order

            


     from base_order_header
)
select * from flattened_order_header

