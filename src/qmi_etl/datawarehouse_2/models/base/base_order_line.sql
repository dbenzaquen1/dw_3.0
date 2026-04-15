with
    soqt_line as (select * from {{ source('sql_server', 'SOQtLine') }}),
    format_order_lines as (
        select
            -- ids
            soqtid as order_id,
            itemid as item_id,
            lineid as order_line_id,
            inventoryid as inventory_id,
            processid as process_id,
            taxid1 as tax_id,
            customerid as customer_id,
            reserveitemid as reserve_item_id,
            cancelreasonid as cancel_reason_id,
            cancelcommentid as cancel_comment_id,
            LineNum as order_line_number,

            -- fact
            price as price,
            qty as order_qty,
            width as width,
            length as length,
            pieces as pieces_amount,
            weight as weight,
            taxamt1 as tax_amount,
            costamt as cost_amount,
            salesamt as sale_amount,
            netweight as net_weight,
            grossweight as gross_weight,
            billedpieces as billed_pieces,
            billedweight as billed_weight,

            -- dim
            gaugemax as gauge_max,
            gaugemin as gauge_min,
            thickness as thickness,
            taxexempt1 as tax_exempt_code,
            description as item_description,
            reservewidth as reserve_width,
            

            -- dates
            cast(orderdate as date) as order_date,
            cast(itemduedate as date) as item_due_date,
            -- flags 
            instock as is_in_stock,
            isorder as is_order,
            complete as is_order_complete,

        from soqt_line

    )
select * from format_order_lines 