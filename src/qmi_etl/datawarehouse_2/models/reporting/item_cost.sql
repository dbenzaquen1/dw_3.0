/*
================================================================================
ITEM COST CALCULATION MODEL
================================================================================

PURPOSE:
This model calculates the estimated total cost per pound for each item in the master item catalog
by combining base material prices with various adders and adjustments.

BUSINESS LOGIC OVERVIEW:
The cost calculation follows this formula:
Total Cost = Base Price + Gauge Adder + Width Adder + Freight Adder + Coating Adder + Steel Gauge Adder + CRU Price

DATA FLOW:
1. Start with master item data and standardize material types/grades
2. Get most recent pricing data (aluminum ingot, CRU daily prices)
3. Get most recent adder tables (Texarkana gauge/width, steel adders)
4. Find cheapest steel coating/gauge/freight adders for each combination
5. Join all pricing components based on material type, gauge, and width ranges
6. Calculate final cost per pound

KEY BUSINESS RULES:
- Uses most recent pricing data (latest reporting_date)
- For steel adders, selects cheapest available option per gauge/width/type
- Texarkana adders are filtered by gauge/width ranges and aluminum grade
- CRU prices are matched by material type and standardized metal subtypes
- All adders are converted to per-pound basis (divided by 100 where needed)

================================================================================
*/

-- ============================================================================
-- BASE ITEM DATA WITH STANDARDIZED MATERIAL TYPES
-- ============================================================================
-- This CTE standardizes material grades and types for consistent joining
-- with pricing tables that use different naming conventions
with stg_master_item as (
    select *, width as item_width,
            -- Standardize aluminum grades for Texarkana adder matching
            case when item_grade like '%3003%' then '3003'
            -- this is for colored aluminum grades
            when item_grade like '%3XXX%' then '3003'
             when item_grade like '%5052%' then '5052'
             else item_grade end as grade_for_texarkana,
             
             -- Map GALVANNEALED to GALVANIZED for CRU price matching
             case when type_description = 'GALVANNEALED' then 'GALVANIZED'
             when type_description = 'HOT ROLLED BLACK' then 'HOT ROLLED'
             when type_description = 'HOT ROLLED P&O' then 'HOT ROLLED'
             else type_description end as type_description_fixed_for_cru
              from {{ref("stg_item_master")}}
),
-- ============================================================================
-- ALUMINUM INGOT PRICING DATA
-- ============================================================================
-- Base aluminum ingot prices from the reference table
alum_ingot_prices as (
    select * from {{ref("base_alum_ingot")}}
),

-- Get the most recent aluminum ingot price (latest reporting_date)
-- Used as base price for aluminum items
most_recent_alum_ingot_prices as (
  select *
  from alum_ingot_prices
  qualify row_number() over (order by reporting_date desc) = 1
),

-- ============================================================================
-- TEXARKANA ADDER TABLES
-- ============================================================================
-- Base Texarkana gauge adders - additional cost based on material gauge
texarkana_gauge_adders as (
    select * from {{ref("texarkana_gauge_adders")}}
),

-- Base Texarkana width adders - additional cost based on material width
texarkana_width_adders as (
    select * from {{ref("texarkana_width_adders")}}
),

-- Filter to only active Texarkana gauge adders
-- These adders are applied based on gauge ranges and aluminum grade
most_recent_texarkana_gauge_adders as (
    select * from texarkana_gauge_adders
    where is_active = true
),

-- Filter to only active Texarkana width adders
-- These adders are applied based on width ranges
most_recent_texarkana_width_adders as (
    select * from texarkana_width_adders
    where is_active = true
),
-- ============================================================================
-- STEEL PRICING AND ADDER TABLES
-- ============================================================================
-- Base steel coating costs - additional cost for different steel coatings
steel_coatings as (
    select * from {{ref("base_steel_coatings")}}
),

-- Base steel gauge adders - additional cost based on steel gauge
steel_gauge_adders as (
    select * from {{ref("base_steel_gauge_adders")}}
),

-- Base steel freight adders - additional freight cost for steel materials
steel_freight_adders as (
    select * from {{ref("base_steel_freight_adders")}}
),

-- ============================================================================
-- CRU (COMMODITY RESEARCH UNIT) DAILY PRICING DATA
-- ============================================================================
-- Standardize CRU metal subtypes to match our internal naming conventions
-- Only include Base and Standard pricing types (excludes premium pricing)
daily_cru_prices as (
    select *,
    case when metal_sub_type = 'Hot Dipped Galvanised' then 'GALVANIZED'
         when metal_sub_type = 'Cold-rolled Coil' then 'COLD ROLLED'
         when metal_sub_type = 'Hot-rolled Coil'   then 'HOT ROLLED'
         when metal_sub_type = 'Electrogalvanised Coil' then 'ALUMINIZED'
         else metal_sub_type
    end as metal_sub_type_fixed,
     from {{ref("daily_cru")}} where hot_dipped_pricing_type in ('Base', 'Standard')
),

-- Get the most recent CRU price for each metal subtype and pricing type
-- Used as base commodity price for steel items
most_recent_daily_cru_prices as (
    select * from daily_cru_prices
    qualify row_number() over (partition by metal_sub_type,hot_dipped_pricing_type order by reporting_date desc) = 1
),

-- ============================================================================
-- CHEAPEST STEEL ADDER SELECTION LOGIC
-- ============================================================================
-- BUSINESS RULE: For steel adders, always select the cheapest available option
-- for each gauge/width/steel_type combination to minimize costs

-- Find the cheapest steel coating cost for each gauge/width/steel_type combination
-- Only considers currently effective pricing (no end date or end date = 1900-01-01)
cheapest_steel_coatings as (
    select 
        *,
        row_number() over (
            partition by gauge, width, steel_type 
            order by coating_amount asc
        ) as rn
    from steel_coatings
    where effective_end_date is null or effective_end_date = '1900-01-01'
),

-- Find the cheapest steel gauge adder for each gauge/width/steel_type combination
-- Only considers currently effective pricing
cheapest_steel_gauge_adders as (
    select 
        *,
        row_number() over (
            partition by gauge, width, steel_type 
            order by gauge_adder_amount asc
        ) as rn
    from steel_gauge_adders
    where effective_end_date is null or effective_end_date = '1900-01-01'
),

-- Find the cheapest steel freight adder (no partitioning needed - global cheapest)
-- Only considers currently effective pricing
cheapest_steel_freight_adders as (
    select 
        *,
        row_number() over (
            order by freight_adder_amount asc
        ) as rn
    from steel_freight_adders
    where effective_end_date is null or effective_end_date = '1900-01-01'
),

-- ============================================================================
-- FILTERED AND STANDARDIZED STEEL ADDER TABLES
-- ============================================================================
-- Filter to only the cheapest steel coating records and standardize for joining
-- Standardizes steel_type naming and gauge format to match item master data
cheapest_steel_coatings_filtered as (
    select *,
    case when steel_type = 'galvanized' then 'GALVANIZED'
         when steel_type = 'galvanneal' then 'GALVANNEALED'
         else steel_type
    end as material_sub_type,
    concat(gauge,'G') as gauge_fixed
      from cheapest_steel_coatings where rn = 1
),

-- Filter to only the cheapest steel gauge adder records and standardize for joining
-- Standardizes steel_type naming, gauge format, and adds material type
cheapest_steel_gauge_adders_filtered as (
    select *,
     case when steel_type = 'galvanized' then 'GALVANIZED'
         when steel_type = 'galvanneal' then 'GALVANNEALED'
         else steel_type
    end as material_sub_type,
     concat(gauge,'G') as gauge_fixed,'Steel' as material_type_fixed from cheapest_steel_gauge_adders where rn = 1
),

-- Filter to only the cheapest steel freight adder record and add material type
cheapest_steel_freight_adders_filtered as (
    select *,'Steel' as material_type from cheapest_steel_freight_adders where rn = 1
),
-- ============================================================================
-- MAIN COST CALCULATION WITH ALL PRICING COMPONENTS
-- ============================================================================
-- This is the core logic that joins all pricing components and calculates
-- the final cost per pound for each item
join_texarkana_adders as (
    select 
        stg_master_item.*,
        coalesce(most_recent_texarkana_gauge_adders.gauge_adder_amount, 0) as gauge_adder_amount,
        coalesce(most_recent_texarkana_width_adders.width_adder_amount, 0) as width_adder_amount,
        coalesce(cheapest_steel_freight_adders_filtered.freight_adder_amount, 0) as freight_adder_amount,
        case when stg_master_item.type_description = 'HOT ROLLED P&O' then .11 else 0 end + coalesce(cheapest_steel_coatings_filtered.coating_amount, 0) as coating_amount,
        coalesce(cheapest_steel_gauge_adders_filtered.gauge_adder_amount, 0) as steel_gauge_adder_amount,
        coalesce(most_recent_daily_cru_prices.price_value_per_pound, 0) as price_value_per_pound,
        
        -- ========================================================================
        -- TOTAL COST CALCULATION FORMULA
        -- ========================================================================
        -- Formula: Base Price + All Applicable Adders + CRU Price
        -- Note: Steel adders are divided by 100 to convert from cents to dollars
        round(
            (case when stg_master_item.type_description = 'HOT ROLLED P&O' then .11 else 0 end) +
            coalesce(midwest_price, 0)  +  -- Base aluminum ingot price
            coalesce(most_recent_texarkana_gauge_adders.gauge_adder_amount, 0)  +  -- Texarkana gauge adder
            coalesce(most_recent_texarkana_width_adders.width_adder_amount, 0)  +  -- Texarkana width adder
            coalesce((cheapest_steel_freight_adders_filtered.freight_adder_amount/100), 0)  +  -- Steel freight adder (cents to dollars)
            coalesce((cheapest_steel_coatings_filtered.coating_amount/100), 0)  +  -- Steel coating adder (cents to dollars)
            coalesce((cheapest_steel_gauge_adders_filtered.gauge_adder_amount/100), 0)  +  -- Steel gauge adder (cents to dollars)
            coalesce(most_recent_daily_cru_prices.price_value_per_pound, 0) ,  -- CRU commodity price
            2
        ) as total_cost_per_pound
    from stg_master_item
    -- ========================================================================
    -- JOIN CONDITIONS FOR PRICING COMPONENTS
    -- ========================================================================
    
    -- JOIN 1: Texarkana Gauge Adders
    -- Business Rule: Apply gauge-based adders for aluminum items based on:
    -- - Material type match (e.g., "Aluminum")
    -- - Item gauge falls within the defined range (gauge_min <= item_gauge < gauge_max)
    -- - Aluminum grade matches (e.g., "3003", "5052")
    left join most_recent_texarkana_gauge_adders on stg_master_item.type_description = most_recent_texarkana_gauge_adders.material_type
    and stg_master_item.item_gauge_numeric >= most_recent_texarkana_gauge_adders.gauge_min 
    and stg_master_item.item_gauge_numeric < most_recent_texarkana_gauge_adders.gauge_max
    and stg_master_item.grade_for_texarkana = most_recent_texarkana_gauge_adders.alum_type

    -- JOIN 2: Texarkana Width Adders
    -- Business Rule: Apply width-based adders for aluminum items based on:
    -- - Material type match
    -- - Item width falls within the defined range (min_width <= item_width < max_width)
    left join most_recent_texarkana_width_adders on stg_master_item.type_description = most_recent_texarkana_width_adders.material_type
    and stg_master_item.item_width >= most_recent_texarkana_width_adders.min_width 
    and stg_master_item.item_width < most_recent_texarkana_width_adders.max_width

    -- JOIN 3: Steel Freight Adders
    -- Business Rule: Apply freight adders for steel materials only
    -- - Material type must be "Steel"
    left join cheapest_steel_freight_adders_filtered on stg_master_item.material_type = cheapest_steel_freight_adders_filtered.material_type

    -- JOIN 4: Steel Coating Adders
    -- Business Rule: Apply coating costs for steel items based on:
    -- - Material subtype match (e.g., "GALVANIZED", "GALVANNEALED")
    -- - Exact gauge match (e.g., "16G", "18G")
    -- - Exact width match
    left join cheapest_steel_coatings_filtered on stg_master_item.type_description = cheapest_steel_coatings_filtered.material_sub_type
    and stg_master_item.item_gauge = cheapest_steel_coatings_filtered.gauge_fixed
    and stg_master_item.item_width = cheapest_steel_coatings_filtered.width

    -- JOIN 5: Steel Gauge Adders
    -- Business Rule: Apply gauge-based adders for steel items based on:
    -- - Material subtype match
    -- - Exact gauge match
    -- - Exact width match
    left join cheapest_steel_gauge_adders_filtered on stg_master_item.type_description = cheapest_steel_gauge_adders_filtered.material_sub_type
    and stg_master_item.item_gauge = cheapest_steel_gauge_adders_filtered.gauge_fixed
    and stg_master_item.item_width = cheapest_steel_gauge_adders_filtered.width

    -- JOIN 6: CRU Daily Pricing
    -- Business Rule: Apply commodity pricing for steel items based on:
    -- - Material type match (e.g., "Steel")
    -- - Standardized metal subtype match (e.g., "GALVANIZED", "COLD ROLLED")
    left join most_recent_daily_cru_prices on stg_master_item.material_type = most_recent_daily_cru_prices.commodity_group 
    and stg_master_item.type_description_fixed_for_cru = most_recent_daily_cru_prices.metal_sub_type_fixed 
    
    -- JOIN 7: Aluminum Ingot Pricing
    -- Business Rule: Apply base aluminum pricing based on material type description
    left join most_recent_alum_ingot_prices on stg_master_item.type_description = most_recent_alum_ingot_prices.type_description

 )

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================
-- Returns all item master data with calculated total_cost_per_pound
-- Each item will have the appropriate pricing components applied based on:
-- - Material type (Aluminum vs Steel)
-- - Gauge and width specifications
-- - Material grade and coating type
-- - Most recent pricing data and cheapest available adders
select *
from (
    select
        j.*,
        row_number() over (
            partition by item_id_width
            order by total_cost_per_pound asc, item_id_width
        ) as row_num
    from join_texarkana_adders j
)
where row_num = 1