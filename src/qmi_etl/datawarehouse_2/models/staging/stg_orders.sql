{#
    Staging Orders Model
    
    This model combines order line, order header, invoice, inventory, and status data to create
    a comprehensive staging table for order analysis. It includes order details, shipping information,
    salesperson details, invoice details, inventory details, and calculated flags for order status.
    
    Key Features:
    - Combines order line and header data with invoice and inventory information
    - Includes salesperson details (primary and secondary)
    - Calculates order status flags (is_late_order, is_credited_order, short_ship_order, over_ship_order)
    - Includes outstanding order metrics (weight_outstanding, quantity_outstanding, open_dollars)
    - Joins inventory data from invoice records when available
    
    Data Sources:
    - base_order_line: Order line item details
    - base_order_header: Order header information
    - base_invoice_line: Invoice line details
    - stg_inventory: Inventory records
    - stg_most_recent_order_status: Most recent order status
    - base_open_orders: Outstanding order metrics
    - last_shipped_status: Last shipment date information
    - stg_salesperson: Salesperson details
#}

with
    order_line as (
        select * from {{ ref("base_order_line") }}
    ),
    
    order_header as (
        select * from {{ ref("base_order_header") }}
    ),
    
    salesperson as (
        select * from {{ ref("stg_salesperson") }}
    ),
    
    base_invoice as (
        select * from {{ ref("base_invoice_line") }}
    ),
    
    base_inventory as (
        select * from {{ ref("stg_inventory") }}
    ),
    
    stg_most_recent_order_status as (
        select * from {{ ref("stg_most_recent_order_status") }}
    ),
    
    base_open_orders as (
        select * from {{ ref('base_open_orders') }}
    ),
    
    base_invoice_head as (
        select * from {{ ref('base_inv_header') }}
    ),
    
    last_shipped_status as (
        select * from {{ ref("last_shipped_status") }}
    ),
    joined_orders as (
        select
            -- IDs and Keys
            case 
                when order_line.width is null 
                then cast(order_line.item_id as string)
                else concat(
                    cast(order_line.item_id as string), 
                    '-', 
                    cast(order_line.width as string)
                )
            end as item_id_width,
            order_line.order_id,
            order_line.order_line_id,
            order_line.item_id,
            order_line.order_line_number,
            order_header.order_number,
            concat(
                cast(order_header.order_number as string),
                '-',
                cast(order_line.order_line_number as string)
            ) as order_number_line,
            order_header.customer_id,
            order_header.customer_po,
            s1.salesperson_id as primary_sales_person_id,
            s2.salesperson_id as secondary_sales_person_id,
            s1.username as primary_sales_person_username,
            s2.username as secondary_sales_person_username,
            base_invoice.invoice_id,
            base_invoice.invoice_line_id,
            base_invoice_head.invoice_number,
            
            -- Fact Columns - Quantities and Weights
            order_line.order_qty,
            order_line.pieces_amount as order_pieces_amount,
            base_invoice.invoice_pieces as invoiced_pieces_amount,
            order_line.weight as order_weight,
            base_invoice.invoice_weight,
            order_line.net_weight,
            order_line.gross_weight,
            order_line.length,
            order_line.width,
            weight_outstanding,
            quantity_outstanding,
            
            -- Fact Columns - Financial
            order_line.price,
            order_line.tax_amount,
            order_line.cost_amount,
            order_line.sale_amount,
            base_invoice.invoice_amount,
            base_invoice.taxable_amount as invoice_taxable_amount,
            inventory.cost as inventory_cost,
            open_dollars,
            
            -- Dimension Columns - Shipping
            order_header.shippment_method,
            order_header.ship_to_company_name,
            order_header.ship_to_zip,
            order_header.ship_to_city,
            order_header.ship_to_state,
            order_header.ship_to_contact,
            order_header.receiving_hours,
            order_header.max_skid_weight,
            order_header.printed_comment,
            
            -- Dimension Columns - Item Specifications
            order_line.gauge_max,
            order_line.gauge_min,
            order_line.thickness,
            inventory.item_number,
            inventory.tag_id as inventory_tag,
            inventory.location_name,
            
            -- Dimension Columns - Status and Descriptions
            order_header.tax_state,
            stg_most_recent_order_status.status_code,
            stg_most_recent_order_status.status_description,
            base_invoice.invoiced_description,
            
            -- Date Columns
            order_header.order_date,
            order_header.quote_date,
            order_header.order_due_date,
            base_invoice.invoice_date,
            base_invoice.invoice_posted_date,
            base_invoice.transaction_date,
            stg_most_recent_order_status.status_date,
            date_diff(
                current_date(), 
                stg_most_recent_order_status.status_date, 
                day
            ) as days_since_status_date,
            last_shipped_status.status_date as last_shipped_date,
            
            -- Flag Columns - Order Attributes
            order_header.has_spacer,
            order_header.has_forklift,
            order_header.has_paper_wrap,
            order_header.has_credit_release,
            case 
                when order_header.is_blanket_order = true 
                then true 
                else false 
            end as is_blanket_order,
            
            -- Flag Columns - Order Status
            case
                when weight_outstanding is not null
                then true
                else false
            end as is_open_order,
            order_line.is_order_complete,
            case
                when stg_most_recent_order_status.status_code = 'Credit' 
                then true 
                else false
            end as is_credited_order,
            case
                when base_invoice.invoice_id is null 
                then false 
                else true 
            end as has_invoice,
            
            -- Flag Columns - Calculated Status
            case 
                when order_header.shippment_method = "Will Call" 
                    and inventory.receiving_date is not null 
                    and inventory.receiving_date > order_header.order_due_date 
                then true
                when order_header.shippment_method <> "Will Call" 
                    and (
                        (last_shipped_status.status_date is not null 
                         and last_shipped_status.status_date > order_header.order_due_date)
                        or (last_shipped_status.status_date is null 
                            and current_date() > order_header.order_due_date 
                            and order_line.is_order_complete = false)
                    )
                then true
                else false 
            end as is_late_order,
            
            -- Flag Columns - Shipment Discrepancies
            case 
                when base_invoice.invoice_id is not null 
                    and base_invoice.invoice_pieces is not null 
                    and order_line.pieces_amount > base_invoice.invoice_pieces 
                then true 
                else false 
            end as short_ship_order,
            case 
                when base_invoice.invoice_id is not null 
                    and base_invoice.invoice_pieces is not null 
                    and order_line.pieces_amount < base_invoice.invoice_pieces 
                then true 
                else false 
            end as over_ship_order
            
        from order_line
        full outer join base_invoice 
            on base_invoice.order_line_id = order_line.order_line_id
        full outer join order_header 
            on order_line.order_id = order_header.order_id
        left join last_shipped_status 
            on last_shipped_status.order_line_id = order_line.order_line_id
        left join stg_most_recent_order_status
            on stg_most_recent_order_status.order_line_id = order_line.order_line_id
        left join salesperson as s1 
            on s1.salesperson_id = order_header.salesperson_id_1
        left join salesperson as s2 
            on s2.salesperson_id = order_header.salesperson_id_2
        left join base_inventory as inventory 
            on base_invoice.inventory_id = inventory.inventory_id
        left join base_open_orders 
            on base_open_orders.order_line_id = order_line.order_line_id
        left join base_invoice_head 
            on base_invoice_head.invoice_id = base_invoice.invoice_id
    )

select * from joined_orders
order by order_line_id desc
