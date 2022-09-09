-- Continue trying to get link the target groups to orders
-- Step 0: Declare inputs
DECLARE entity_id_var ARRAY <STRING>;
SET entity_id_var = ['FP_PH', 'FP_TH'];

-- Step 1: Pull the valid experiment names
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.valid_exp_names_switchback_tests` AS 
SELECT DISTINCT
  entity_id,
  country_code,
  test_id,
  test_name
FROM `fulfillment-dwh-production.cl.dps_experiment_setups` a
CROSS JOIN UNNEST(a.matching_vendor_ids) AS vendor_id
LEFT JOIN UNNEST(test_vertical_parents) parent_vertical
WHERE TRUE
  AND DATE(test_start_date) >= DATE('2022-07-19') -- Filter for tests that started from July 19th, 2022 (date of the first switchback test)
  AND entity_id IN UNNEST(entity_id_var)
  AND (LOWER(test_name) LIKE '%sb%' OR LOWER(test_name) LIKE '%switchback%');

###----------------------------------------------------------END OF VALID EXP NAMES PART----------------------------------------------------------###

-- Step 2: Extract the vendor IDs per target group along with their associated parent vertical and vertical type
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_target_groups_switchback_tests` AS
WITH vendor_tg_vertical_mapping_with_dup AS (
  SELECT DISTINCT -- The DISTINCT command is important here
    entity_id,
    country_code,
    test_start_date,
    test_end_date,
    test_name,
    test_id,
    vendor_group_id,
    vendor_id AS vendor_code,
    parent_vertical, -- The parent vertical can only assume 7 values 'Restaurant', 'Shop', 'darkstores', 'restaurant', 'restaurants', 'shop', or NULL. The differences are due platform configurations
    CONCAT('TG', DENSE_RANK() OVER (PARTITION BY entity_id, test_name ORDER BY vendor_group_id)) AS tg_name,
    
    -- Time condition parameters
    schedule.id AS tc_id,
    schedule.priority AS tc_priority,
    schedule.start_at,
    schedule.recurrence_end_at,
    active_days,

    -- Customer condition parameters
    customer_condition.id AS cc_id,
    customer_condition.priority AS cc_priority,
    customer_condition.orders_number_less_than,
    customer_condition.days_since_first_order_less_than,
  FROM `fulfillment-dwh-production.cl.dps_experiment_setups` a
  CROSS JOIN UNNEST(a.matching_vendor_ids) AS vendor_id
  LEFT JOIN UNNEST(test_vertical_parents) parent_vertical
  LEFT JOIN UNNEST(schedule.active_days) active_days
  WHERE TRUE 
    AND test_name IN (SELECT DISTINCT test_name FROM `dh-logistics-product-ops.pricing.valid_exp_names_switchback_tests`)
),

vendor_tg_vertical_mapping_agg AS (
  SELECT 
    * EXCEPT (parent_vertical),
    ARRAY_TO_STRING(ARRAY_AGG(parent_vertical RESPECT NULLS ORDER BY parent_vertical), ', ') AS parent_vertical_concat -- We do this step because some tests have two parent verticals. If we do not aggregate, we will get duplicates 
  FROM vendor_tg_vertical_mapping_with_dup 
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)

SELECT
  a.*,
  CASE 
    WHEN parent_vertical_concat = '' THEN NULL -- Case 1
    WHEN parent_vertical_concat LIKE '%,%' THEN -- Case 2 (tests where multiple parent verticals were chosen during configuration)
      CASE
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r'(.*),\s') IN ('restaurant', 'restaurants') THEN 'restaurant'
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r'(.*),\s') = 'shop' THEN 'shop'
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r'(.*),\s') = 'darkstores' THEN 'darkstores'
      END
    -- Case 3 (tests where a single parent vertical was chosen during configuration)
    WHEN LOWER(parent_vertical_concat) IN ('restaurant', 'restaurants') THEN 'restaurant'
    WHEN LOWER(parent_vertical_concat) = 'shop' THEN 'shop'
    WHEN LOWER(parent_vertical_concat) = 'darkstores' THEN 'darkstores'
  ELSE REGEXP_SUBSTR(parent_vertical_concat, r'(.*),\s') END AS first_parent_vertical,
  
  CASE
    WHEN parent_vertical_concat = '' THEN NULL
    WHEN parent_vertical_concat LIKE '%,%' THEN
      CASE
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r',\s(.*)') IN ('restaurant', 'restaurants') THEN 'restaurant'
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r',\s(.*)') = 'shop' THEN 'shop'
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r',\s(.*)') = 'darkstores' THEN 'darkstores'
      END
  END AS second_parent_vertical,
  b.vertical_type -- Vertical type of the vendor (NOT parent vertical)
FROM vendor_tg_vertical_mapping_agg a
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.vendors` b ON a.entity_id = b.global_entity_id AND a.vendor_code = b.vendor_id
ORDER BY 1,2,3,4,5;

-- Step 3: Extract the zones that are part of the experiment
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_zone_ids_switchback_tests` AS
SELECT DISTINCT -- The DISTINCT command is important here
    entity_id,
    country_code,
    test_start_date,
    test_end_date,
    test_name,
    test_id,
    zone_id
FROM `fulfillment-dwh-production.cl.dps_experiment_setups` a
CROSS JOIN UNNEST(a.zone_ids) AS zone_id
CROSS JOIN UNNEST(a.matching_vendor_ids) AS vendor_id
LEFT JOIN UNNEST(test_vertical_parents) parent_vertical
WHERE TRUE
  AND test_name IN (SELECT DISTINCT test_name FROM `dh-logistics-product-ops.pricing.valid_exp_names_switchback_tests`)
ORDER BY 1,2;

-- Step 4.1: Extract the target groups, variants, and price schemes of the tests
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_tgs_variants_and_schemes_switchback_tests` AS
SELECT DISTINCT
    entity_id,
    country_code,
    test_start_date,
    test_end_date,
    test_name,
    test_id,
    CONCAT('TG', priority) AS target_group,
    variation_group AS variant,
    price_scheme_id
FROM `fulfillment-dwh-production.cl.dps_experiment_setups` a
CROSS JOIN UNNEST(a.matching_vendor_ids) AS vendor_id
LEFT JOIN UNNEST(test_vertical_parents) parent_vertical
WHERE TRUE 
  AND test_name IN (SELECT DISTINCT test_name FROM `dh-logistics-product-ops.pricing.valid_exp_names_switchback_tests`)
ORDER BY 1,2;

-- Step 4.2: Find the distinct combinations of target groups, variants, and price schemes per test
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_agg_tgs_variants_and_schemes_switchback_tests` AS
SELECT 
  entity_id,
  country_code,
  test_name,
  test_id,
  ARRAY_TO_STRING(ARRAY_AGG(CONCAT(target_group, ' | ', variant, ' | ', price_scheme_id)), ', ') AS tg_var_scheme_concat
FROM `dh-logistics-product-ops.pricing.ab_test_tgs_variants_and_schemes_switchback_tests`
GROUP BY 1,2,3,4;

-- Step 5: Extract the polygon shapes of the experiment's target zones
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_geo_data_switchback_tests` AS
SELECT 
    p.entity_id,
    co.country_code,
    ci.name AS city_name,
    ci.id AS city_id,
    zo.shape AS zone_shape, 
    zo.name AS zone_name,
    zo.id AS zone_id,
    tgt.test_name,
    tgt.test_id,
    tgt.test_start_date,
    tgt.test_end_date,
FROM `fulfillment-dwh-production.cl.countries` co
LEFT JOIN UNNEST(co.platforms) p
LEFT JOIN UNNEST(co.cities) ci
LEFT JOIN UNNEST(ci.zones) zo
INNER JOIN `dh-logistics-product-ops.pricing.ab_test_zone_ids_switchback_tests` tgt ON p.entity_id = tgt.entity_id AND co.country_code = tgt.country_code AND zo.id = tgt.zone_id 
WHERE TRUE 
    AND zo.is_active -- Active city
    AND ci.is_active; -- Active zone

###----------------------------------------------------------END OF EXP SETUPS PART----------------------------------------------------------###

-- Step 6: Pull the business KPIs from dps_sessions_mapped_to_orders_v2
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_individual_orders_switchback_tests` AS
WITH test_start_and_end_dates AS ( -- Get the start and end dates per test
  SELECT DISTINCT
    entity_id,
    country_code,
    test_start_date,
    test_end_date,
    test_name,
    test_id
  FROM `dh-logistics-product-ops.pricing.ab_test_zone_ids_switchback_tests`
),

entities AS (
    SELECT
        ent.region,
        p.entity_id,
        ent.country_iso,
        ent.country_name,
FROM `fulfillment-dwh-production.cl.entities` ent
LEFT JOIN UNNEST(platforms) p
INNER JOIN (SELECT DISTINCT entity_id FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`) dps ON p.entity_id = dps.entity_id 
WHERE TRUE
    AND p.entity_id NOT LIKE 'ODR%' -- Eliminate entities starting with DN_ as they are not part of DPS
    AND p.entity_id NOT LIKE 'DN_%' -- Eliminate entities starting with ODR (on-demand riders)
    AND p.entity_id NOT IN ('FP_DE', 'FP_JP') -- Eliminate JP and DE because they are not DH markets any more
    AND p.entity_id != 'TB_SA' -- Eliminate this incorrect entity_id for Saudi
    AND p.entity_id != 'HS_BH' -- Eliminate this incorrect entity_id for Bahrain
)

SELECT 
    -- Identifiers and supplementary fields     
    -- Date and time
    a.created_date AS created_date_utc,
    a.order_placed_at AS order_placed_at_utc,
    a.order_placed_at_local,
    FORMAT_DATE('%A', DATE(order_placed_at_local)) AS dow_local,
    a.dps_sessionid_created_at AS dps_sessionid_created_at_utc,
    DATE_DIFF(DATE(a.order_placed_at_local), DATE_ADD(DATE(dat.test_start_date), INTERVAL 1 DAY), DAY) + 1 AS day_num_in_test, -- We add "+1" so that the first day gets a "1" not a "0"
    CASE WHEN MOD(DATE_DIFF(DATE(a.order_placed_at_local), DATE_ADD(DATE(dat.test_start_date), INTERVAL 1 DAY), DAY) + 1, 2) = 0 THEN 'even' ELSE 'odd' END AS even_or_odd_day,

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
    dat.test_name,
    dat.test_start_date,
    dat.test_end_date,
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
    COALESCE(tg.tg_name, 'Non_TG') AS target_group,
    b.target_group AS target_group_bi,
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
    tg.parent_vertical_concat,
    -- This filter is used to clean the data. It removes all orders that did not belong to the correct target_group, variant, scheme_id combination as dictated by the experiment's setup
    CASE WHEN COALESCE(tg.tg_name, 'Non_TG') = 'Non_TG' OR vs.tg_var_scheme_concat LIKE CONCAT('%', COALESCE(tg.tg_name, 'Non_TG'), ' | ', a.variant, ' | ', a.scheme_id, '%') THEN 'Keep' ELSE 'Drop' END AS keep_drop_flag
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` a
LEFT JOIN `fulfillment-dwh-production.cl.dps_ab_test_orders_v2` b ON a.entity_id = b.entity_id AND a.order_id = b.order_id
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` dwh 
  ON TRUE 
    AND a.entity_id = dwh.global_entity_id
    AND a.platform_order_code = dwh.order_id -- There is no country_code field in this table
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` pd -- Contains info on the orders in Pandora countries
  ON TRUE 
    AND a.entity_id = pd.global_entity_id
    AND a.platform_order_code = pd.code 
    AND a.created_date = pd.created_date_utc -- There is no country_code field in this table
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_geo_data_switchback_tests` zn 
  ON TRUE 
    AND a.entity_id = zn.entity_id 
    AND a.country_code = zn.country_code
    AND a.zone_id = zn.zone_id 
    AND a.experiment_id = zn.test_id -- Filter for orders in the target zones (combine this JOIN with the condition in the WHERE clause)
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_target_groups_switchback_tests` tg -- Tag the vendors with their target group association
  ON TRUE
    AND a.entity_id = tg.entity_id
    AND a.vendor_id = tg.vendor_code 
    AND a.experiment_id = tg.test_id 
    AND DATE(a.order_placed_at_local) BETWEEN DATE(tg.start_at) AND DATE(tg.recurrence_end_at) -- A join for the time condition (1)
    AND UPPER(FORMAT_DATE('%A', DATE(a.order_placed_at_local))) = tg.active_days -- A join for the time condition (2)
    AND 
      CASE WHEN tg.orders_number_less_than IS NULL AND tg.days_since_first_order_less_than IS NULL THEN TRUE -- If there is no customer condition in the experiment, skip the join step
      ELSE -- If there is assign the orders with dps_customer_tag = 'New' to their relevant target groups depending on the "calendar week" AND the two customer condition parameters (total_orders and days_since_first_order)
        a.customer_total_orders < tg.orders_number_less_than -- customer_total_orders always > 0 when dps_customer_tag = 'New'
        -- customer_first_order_date could be NULL or have a DATETIME value. In both cases, dps_customer_tag could be equal to 'New'
        AND (DATE_DIFF(a.order_placed_at, a.customer_first_order_date, DAY) < tg.days_since_first_order_less_than OR DATE_DIFF(a.order_placed_at, a.customer_first_order_date, DAY) IS NULL)
      END
LEFT JOIN test_start_and_end_dates dat ON a.entity_id = dat.entity_id AND a.country_code = dat.country_code AND a.experiment_id = dat.test_id
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_agg_tgs_variants_and_schemes_switchback_tests` vs -- Get the list of target_group | variation | scheme_id combinations that are relevant to the experiment
  ON TRUE
    AND a.entity_id = vs.entity_id 
    AND a.country_code = vs.country_code 
    AND a.experiment_id = vs.test_id
INNER JOIN entities ent ON a.entity_id = ent.entity_id -- Get the region associated with every entity_id
WHERE TRUE
    AND a.created_date >= DATE('2022-07-19') -- Filter for tests that started from July 19th, 2022 (date of the first switchback test)
    
    AND CONCAT(a.entity_id, ' | ', a.country_code, ' | ', a.experiment_id, ' | ', a.variant) IN ( -- Filter for the right variants belonging to the experiment (essentially filter out NULL and Original)
      SELECT DISTINCT CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', variant) 
      FROM `dh-logistics-product-ops.pricing.ab_test_tgs_variants_and_schemes_switchback_tests`
      WHERE CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', variant) IS NOT NULL
    )
    
    AND a.delivery_status = 'completed' -- Successful orders
    
    AND CONCAT(a.entity_id, ' | ', a.country_code, ' | ', a.experiment_id) IN ( -- Filter for the right entity | experiment_id combos. 
      -- The "ab_test_target_groups_switchback_tests" table was specifically chosen from the tables in steps 2-4 because it automatically eliminates tests where there are no matching vendors
      SELECT DISTINCT CONCAT(entity_id, ' | ', country_code, ' | ', test_id)
      FROM `dh-logistics-product-ops.pricing.ab_test_target_groups_switchback_tests`
      WHERE CONCAT(entity_id, ' | ', country_code, ' | ', test_id) IS NOT NULL
    )
    
    AND ST_CONTAINS(zn.zone_shape, ST_GEOGPOINT(dwh.delivery_location.longitude, dwh.delivery_location.latitude)); -- Filter for orders coming from the target zones

###----------------------------------------------------------SEPARATOR----------------------------------------------------------###

-- Step 7.1: We did not add the profit metrics and the parent_vertical filter to the previous query because some of the fields used below had to be computed first
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_individual_orders_augmented_switchback_tests` AS
SELECT
  a.*,
  -- Revenue and profit formulas
  actual_df_paid_by_customer + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local_cdwh, sof_local) AS revenue_local,
  actual_df_paid_by_customer + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local_cdwh, sof_local) - delivery_costs_local AS gross_profit_local,

FROM `dh-logistics-product-ops.pricing.ab_test_individual_orders_switchback_tests` a
WHERE TRUE -- Filter for orders from the right parent vertical (restuarants, shop, darkstores, etc.) per experiment
    AND (
      CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', vendor_vertical_parent) IN ( -- If the parent vertical exists, filter for the right one belonging to the experiment
        SELECT DISTINCT CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', first_parent_vertical)
        FROM `dh-logistics-product-ops.pricing.ab_test_target_groups_switchback_tests`
        WHERE CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', first_parent_vertical) IS NOT NULL
      )
      OR
      CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', vendor_vertical_parent) IN ( -- If the parent vertical exists, filter for the right one belonging to the experiment
        SELECT DISTINCT CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', second_parent_vertical)
        FROM `dh-logistics-product-ops.pricing.ab_test_target_groups_switchback_tests`
        WHERE CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', second_parent_vertical) IS NOT NULL
      )
    );

-- Step 7.2: Get the vendor locations
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.vendor_locations_switchback_tests` AS
WITH vendor_list AS (
  SELECT DISTINCT
    entity_id,
    test_id,
    vendor_id,
  FROM `dh-logistics-product-ops.pricing.ab_test_individual_orders_augmented_switchback_tests`
)

SELECT
  a.*,
  ST_ASTEXT(b.location) AS vendor_location,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT zn.name ORDER BY zn.name), ', ') AS zone_name,
FROM vendor_list a
LEFT JOIN `fulfillment-dwh-production.cl.vendors_v2` b ON a.entity_id = b.entity_id AND a.vendor_id = b.vendor_code
LEFT JOIN UNNEST(b.zones) zn
GROUP BY 1,2,3,4;

-- Step 7.3: Append the vendor locations to "ab_test_individual_orders_augmented_switchback_tests"
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_individual_orders_augmented_switchback_tests` AS
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
FROM `dh-logistics-product-ops.pricing.ab_test_individual_orders_augmented_switchback_tests` a
LEFT JOIN `dh-logistics-product-ops.pricing.vendor_locations_switchback_tests` b USING (entity_id, test_id, vendor_id);

###----------------------------------------------------------END OF RAW ORDERS EXTRACTION PART----------------------------------------------------------###

-- Step 8: Clean the orders data by filtering for records where keep_drop_flag = 'Keep' (refer to the code above to see how this field was constructed)
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_individual_orders_cleaned_staging_switchback_tests` AS
SELECT
    *
FROM `dh-logistics-product-ops.pricing.ab_test_individual_orders_augmented_switchback_tests`
WHERE TRUE
    AND keep_drop_flag = 'Keep'; -- Filter for the orders that have the correct target_group, variant, and scheme ID based on the configuration of the experiment

###----------------------------------------------------------END OF CLEAN ORDERS EXTRACTION PART----------------------------------------------------------###

-- Step 9: Append new rows to the final table that is used in the dashboard and Py script (`dh-logistics-product-ops.pricing.ab_test_individual_orders_cleaned_switchback_tests`)
INSERT `dh-logistics-product-ops.pricing.ab_test_individual_orders_cleaned_switchback_tests`
SELECT *
FROM `dh-logistics-product-ops.pricing.ab_test_individual_orders_cleaned_staging_switchback_tests`
WHERE created_date_utc > (SELECT MAX(created_date_utc) FROM `dh-logistics-product-ops.pricing.ab_test_individual_orders_cleaned_switchback_tests`) -- SELECT ALL records from the staging table that have a created_date > MAX(created_date) in the table used in the dashboard and Py script