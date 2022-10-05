-- Continue trying to get link the target groups to orders
-- Step 0: Declare inputs
DECLARE entity_id_var ARRAY <STRING>;
DECLARE start_date_var, end_date_var DATE;
DECLARE exclude_zones ARRAY <STRING>;
DECLARE exclude_cities ARRAY <STRING>;
DECLARE vertical_type_var ARRAY <STRING>;
SET entity_id_var = ['FP_SG', 'FP_MY'];
SET (start_date_var, end_date_var) = (DATE('2022-08-31'), DATE('2022-09-27')); 
SET exclude_zones = ['Sg_south', 'Jurongwest', 'Jurong east', 'Bukit timah', 'Woodlands', 'Bedok']; -- SG
SET exclude_cities = ['Klang valley']; -- MY
SET vertical_type_var = ['restaurants'];

-- Step 1: Extract the polygon shapes of the experiment's target zones
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_geo_data_additional_deep_dives` AS
SELECT 
    p.entity_id,
    co.country_code,
    ci.name AS city_name,
    ci.id AS city_id,
    zo.shape AS zone_shape, 
    zo.name AS zone_name,
    zo.id AS zone_id
FROM `fulfillment-dwh-production.cl.countries` co
LEFT JOIN UNNEST(co.platforms) p
LEFT JOIN UNNEST(co.cities) ci
LEFT JOIN UNNEST(ci.zones) zo
WHERE TRUE 
    AND p.entity_id IN UNNEST(entity_id_var)
    AND co.country_code != 'dp-sg'
    AND zo.is_active -- Active city
    AND ci.is_active -- Active zone
    AND zo.name NOT IN UNNEST(exclude_zones)
    AND ci.name NOT IN UNNEST(exclude_cities);

###----------------------------------------------------------END OF EXP SETUPS PART----------------------------------------------------------###

-- Step 2: Pull the business KPIs from dps_sessions_mapped_to_orders_v2
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_individual_orders_additional_deep_dives` AS
WITH entities AS (
    SELECT
        ent.region,
        p.entity_id,
        ent.country_iso,
        ent.country_name,
FROM `fulfillment-dwh-production.cl.entities` ent
LEFT JOIN UNNEST(platforms) p
INNER JOIN (SELECT DISTINCT entity_id FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`) dps ON p.entity_id = dps.entity_id 
WHERE p.entity_id IN UNNEST(entity_id_var)
)

SELECT 
    -- Identifiers and supplementary fields     
    -- Date and time
    a.created_date AS created_date_utc,
    a.order_placed_at AS order_placed_at_utc,
    a.order_placed_at_local,
    FORMAT_DATE('%A', DATE(order_placed_at_local)) AS dow_local,
    a.dps_sessionid_created_at AS dps_sessionid_created_at_utc,
    DATE_DIFF(DATE(a.order_placed_at_local), DATE_ADD(DATE(start_date_var), INTERVAL 1 DAY), DAY) + 1 AS day_num_in_test, -- We add "+1" so that the first day gets a "1" not a "0"
    CASE WHEN MOD(DATE_DIFF(DATE(a.order_placed_at_local), DATE_ADD(DATE(start_date_var), INTERVAL 1 DAY), DAY) + 1, 2) = 0 THEN 'even' ELSE 'odd' END AS even_or_odd_day,

    -- Location of order
    a.region,
    a.entity_id,
    a.country_code,
    a.city_name,
    a.city_id,
    a.zone_name,
    a.zone_id,
    zn.zone_shape,
    ST_GEOGPOINT(dwh.delivery_location.longitude, dwh.delivery_location.latitude) AS customer_location,

    -- Order/customer identifiers and session data
    a.variant,
    a.experiment_id AS test_id,
    start_date_var,
    a.perseus_client_id,
    a.ga_session_id,
    a.dps_sessionid,
    a.dps_customer_tag,
    a.customer_total_orders,
    a.customer_first_order_date,
    DATE_DIFF(a.order_placed_at, a.customer_first_order_date, DAY) AS days_since_first_order,
    a.order_id,
    a.platform_order_code,
    a.scheme_id,
    a.vendor_price_scheme_type,	-- The assignment type of the scheme to the vendor during the time of the order, such as 'Automatic', 'Manual', 'Campaign', and 'Country Fallback'.
    
    -- Vendor data and information on the delivery
    a.vendor_id,
    a.is_in_treatment,
    a.chain_id,
    a.chain_name,
    a.vertical_type,
    CASE 
      WHEN a.vendor_vertical_parent IS NULL THEN NULL 
      WHEN LOWER(a.vendor_vertical_parent) IN ('restaurant', 'restaurants') THEN 'restaurant'
      WHEN LOWER(a.vendor_vertical_parent) = 'shop' THEN 'shop'
      WHEN LOWER(a.vendor_vertical_parent) = 'darkstores' THEN 'darkstores'
    END AS vendor_vertical_parent,
    a.delivery_status,
    a.is_own_delivery,
    a.exchange_rate,

    -- Business KPIs (These are the components of profit)
    a.dps_delivery_fee_local,
    a.dps_surge_fee_local,
    a.dps_travel_time_fee_local,
    a.delivery_fee_local,
    a.commission_local,
    a.joker_vendor_fee_local,
    COALESCE(a.service_fee_local, 0) AS service_fee_local,
    dwh.value.mov_customer_fee_local AS sof_local_cdwh,
    IF(a.gfv_local - a.dps_minimum_order_value_local >= 0, 0, COALESCE(dwh.value.mov_customer_fee_local, (a.dps_minimum_order_value_local - a.gfv_local))) AS sof_local,
    a.delivery_costs_local,
    CASE
        WHEN ent.region IN ('Europe', 'Asia') THEN COALESCE( -- Get the delivery fee data of Pandora countries from Pandata tables
            pd.delivery_fee_local, 
            -- In 99 pct of cases, we won't need to use that fallback logic as pd.delivery_fee_local is reliable
            IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.delivery_fee_local)
        )
        -- If the order comes from a non-Pandora country, use delivery_fee_local
        WHEN ent.region NOT IN ('Europe', 'Asia') THEN (CASE WHEN a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE THEN 0 ELSE a.delivery_fee_local END)
    END AS actual_df_paid_by_customer,
    a.gfv_local,
    a.gmv_local,

    -- Logistics KPIs
    a.mean_delay, -- A.K.A Average fleet delay --> Average lateness in minutes of an order at session start time (Used by dashboard, das, dps). This data point is only available for OD orders
    a.dps_mean_delay, -- A.K.A DPS Average fleet delay --> Average lateness in minutes of an order placed at this time coming from DPS service
    a.dps_mean_delay_zone_id, -- ID of the zone where fleet delay applies
    a.travel_time, -- The time (min) it takes rider to travel from vendor location coordinates to the customers. This data point is only available for OD orders.
    a.dps_travel_time, -- The calculated travel time in minutes from the vendor to customer coming from DPS
    a.travel_time_distance_km, -- The distance (km) between the vendor location coordinates and customer location coordinates. This data point is only available for OD orders
    a.delivery_distance_m, -- This is the "Delivery Distance" field in the overview tab in the AB test dashboard. The Manhattan distance (km) between the vendor location coordinates and customer location coordinates
    -- This distance doesn't take into account potential stacked deliveries, and it's not the travelled distance. This data point is only available for OD orders.
    a.to_customer_time, -- The time difference between rider arrival at customer and the pickup time. This data point is only available for OD orders
    a.actual_DT, -- The time it took to deliver the order. Measured from order creation until rider at customer. This data point is only available for OD orders.

    -- Special fields
    a.is_delivery_fee_covered_by_discount, -- Needed in the profit formula
    a.is_delivery_fee_covered_by_voucher, -- Needed in the profit formula
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` a
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` dwh 
  ON TRUE 
    AND a.entity_id = dwh.global_entity_id
    AND a.platform_order_code = dwh.order_id -- There is no country_code field in this table
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` pd -- Contains info on the orders in Pandora countries
  ON TRUE 
    AND a.entity_id = pd.global_entity_id
    AND a.platform_order_code = pd.code 
    AND a.created_date = pd.created_date_utc -- There is no country_code field in this table
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_geo_data_additional_deep_dives` zn 
  ON TRUE 
    AND a.entity_id = zn.entity_id 
    AND a.country_code = zn.country_code
    AND a.zone_id = zn.zone_id
INNER JOIN entities ent ON a.entity_id = ent.entity_id -- Get the region associated with every entity_id
WHERE TRUE
  AND a.entity_id IN (SELECT DISTINCT entity_id FROM `dh-logistics-product-ops.pricing.ab_test_geo_data_additional_deep_dives`)
  AND a.created_date BETWEEN DATE_SUB(start_date_var, INTERVAL 1 DAY) AND DATE_ADD(end_date_var, INTERVAL 1 DAY)
  AND variant IN ('Original', 'Control')
  AND vendor_price_scheme_type IN ('Automatic scheme', 'Manual')
  AND a.delivery_status = 'completed' -- Successful orders
  AND vertical_type IN UNNEST(vertical_type_var)
  AND ST_CONTAINS(zn.zone_shape, ST_GEOGPOINT(dwh.delivery_location.longitude, dwh.delivery_location.latitude)); -- Filter for orders coming from the target zones

###----------------------------------------------------------SEPARATOR----------------------------------------------------------###

-- Step 7.1: We did not add the profit metrics and the parent_vertical filter to the previous query because some of the fields used below had to be computed first
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_individual_orders_augmented_additional_deep_dives` AS
SELECT
  a.*,
  -- Revenue and profit formulas
  actual_df_paid_by_customer + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local_cdwh, sof_local) AS revenue_local,
  actual_df_paid_by_customer + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local_cdwh, sof_local) - delivery_costs_local AS gross_profit_local,
FROM `dh-logistics-product-ops.pricing.ab_test_individual_orders_additional_deep_dives` a;

-- Step 7.2: Get the vendor locations
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.vendor_locations_additional_deep_dives` AS
WITH vendor_list AS (
  SELECT DISTINCT
    entity_id,
    test_id,
    vendor_id,
  FROM `dh-logistics-product-ops.pricing.ab_test_individual_orders_augmented_additional_deep_dives`
)

SELECT
  a.*,
  ST_ASTEXT(b.location) AS vendor_location,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT zn.name ORDER BY zn.name), ', ') AS zone_name,
FROM vendor_list a
LEFT JOIN `fulfillment-dwh-production.cl.vendors_v2` b ON a.entity_id = b.entity_id AND a.vendor_id = b.vendor_code
LEFT JOIN UNNEST(b.zones) zn
GROUP BY 1,2,3,4;

-- Step 7.3: Append the vendor locations to "ab_test_individual_orders_augmented_additional_deep_dives"
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_individual_orders_augmented_additional_deep_dives` AS
SELECT 
  -- Identifiers and supplementary fields     
    -- Date and time
    a.created_date_utc,
    a.order_placed_at_utc,
    a.order_placed_at_local,
    a.dow_local,
    a.dps_sessionid_created_at_utc,
    a.day_num_in_test, -- We add "+1" so that the first day gets a "1" not a "0"
    a.even_or_odd_day,

    -- Location of order
    a.region,
    a.entity_id,
    a.country_code,
    a.city_name,
    a.city_id,
    a.zone_name AS zone_name_customer,
    a.zone_id AS zone_id_customer,
    a.zone_shape AS zone_shape_customer,
    a.customer_location,
    b.vendor_location,
    b.zone_name AS zone_name_vendor,

    a.* EXCEPT(
      created_date_utc, order_placed_at_utc, order_placed_at_local, dow_local, dps_sessionid_created_at_utc, day_num_in_test, even_or_odd_day,
      region, entity_id, country_code, city_name, city_id, zone_name, zone_id, zone_shape, customer_location
    )
FROM `dh-logistics-product-ops.pricing.ab_test_individual_orders_augmented_additional_deep_dives` a
LEFT JOIN `dh-logistics-product-ops.pricing.vendor_locations_additional_deep_dives` b USING (entity_id, test_id, vendor_id);

###----------------------------------------------------------END OF RAW ORDERS EXTRACTION PART----------------------------------------------------------###
