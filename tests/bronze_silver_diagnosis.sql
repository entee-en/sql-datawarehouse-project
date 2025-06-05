/*
====================================================================================================
Data diagnosis and Quality Check (Bronze --> Silver)
====================================================================================================
Script Purpose:
	This script performs various diagnosis for data consistency, accuracy,
	and standardization across the 'Bronze' schema and final quality check
	after inserting into 'Silver' schema. It includes:
		- Checks for NULL and duplicates in primary keys
		- Checks unwanted spaces for string data
		- Detect syntax anomalies, invalid date and chronological records


Warning: 
	- This script showcase my thinking process when conducting data diagnosis.
	- For future updates of raw sources or bronze schema, I recommend use this 
		script as a reference, not a fixed material.
	- Different diagnosis results leads to different transformation methods.
		Therefore, prior discussing with relevants department or experts is
		highly recommended before making any adjustment and execution. 
*/


/*==================================================================================================
--------------------------------bronze.crm_cust_info-------------------------------------------------
*/

-- Check data quality & clean
-- (1) Check for NULL and duplicates in primary key
-- Expected outcome: No result

USE data_warehouse;
GO

EXEC bronze.load_bronze;
GO

SELECT
	cst_id,
	COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) >1 OR cst_id IS NULL;

--Validate the problem: why is it duplicated
SELECT * FROM bronze.crm_cust_info
WHERE cst_id = 29466;
/*
We identified that the latest create date of duplicated cst_id has the most completed info. 
Next step: Use Window function ROW NUMBER() to rank the cst_id by create date
*/

SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
WHERE cst_id = 29466;

/*
cst_id with flag_last==1 may have the most completed info
=> remove cst_id with flag_last=! 1 will solve the problem. 
*/

SELECT * 
FROM (
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info )t
WHERE flag_last = 1;

-- (2) Check redundant spacing for string values
/* Use TRIM(column) to remove spacing character from string*/
--Expected Outcome: No result
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);
/*
If the orgininal value is not equal to the same value after trimming => spaces exist
*/

-- (3) Check Data Standardization & Consistency

SELECT cst_gndr, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_gndr;

/*
Use CASE WHEN [] THEN [] 
		ELSE[] 
	END var 
to standardize the values.
*/


--- Validate 
-- (1) Check for NULL and duplicates in primary key
-- Expected outcome: No result

SELECT
	cst_id,
	COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) >1 OR cst_id IS NULL;

SELECT
	cst_key,
	COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_key
HAVING COUNT(*) >1 OR cst_key IS NULL;

-- (2) Check redundant spacing for string values
/* Use TRIM(column) to remove spacing character from string*/
--Expected Outcome: No result
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- (3) Check Data Standardization & Consistency after transform

SELECT cst_gndr, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_gndr;

--- Check final outcome
SELECT * FROM silver.crm_cust_info;

/*==================================================================================================
--------------------------------bronze.crm_prd_info-------------------------------------------------
*/

-- Check data quality & clean
-- (1) Check for NULL, negative number, and duplicates in primary key
-- Expected outcome: No result

SELECT
	prd_id,
	COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) >1 OR prd_id IS NULL;

SELECT
	prd_key,
	prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost <0 OR prd_cost IS NULL;
/*
There are two NULL prd_cost of prd_key 'FR-R92%'
Validate if there is sales info about these products
*/
SELECT *
FROM bronze.crm_prd_info
WHERE prd_cost <0 OR prd_cost IS NULL;

SELECT * FROM bronze.crm_sales_details
WHERE sls_prd_key LIKE 'FR-R92%';

SELECT *
FROM bronze.crm_prd_info
WHERE prd_key LIKE 'CO-RF-FR-R92%-5%';

/*
After validate, prd_nm 'Road Frame - 58' was not in sales data and produced before 2010
=> Ask relevant dept whether NULL can be treated as 0 => Assumed yes
*/

-- (2) Check redundant spacing for string values
/* Use TRIM(column) to remove spacing character from string*/
--Expected Outcome: No result
SELECT prd_key
FROM bronze.crm_prd_info
WHERE prd_key != TRIM(prd_key)

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

/*
prd_key has two information: cat_id - prd_id for sales
=> Use SUBSTRING() to extract two information into two column
*/
-- Extract cat_id
SELECT 
	prd_key,
	SUBSTRING(prd_key,1,5) AS cat_id
FROM bronze.crm_prd_info;
-- Check data in erp sources
SELECT distinct id FROM bronze.erp_px_cat_g1v2;
/*
There is a difference in delimeter btw '-' in crm and '_' in erp source
=> use REPLACE(var, old string, new string) to replace delimeter for further join
*/

-- Check matching key for further join
SELECT 
	prd_key,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key,1,5),'-','_') NOT IN (
	SELECT distinct id FROM bronze.erp_px_cat_g1v2);

/*
Only CO_PE is not in erp source
=> Can be handled when joining table
*/

-- Extract prd_code for sales
SELECT 
	prd_key,
	SUBSTRING(prd_key,1,5) AS cat_id,
	TRIM(SUBSTRING(prd_key,7,LEN(prd_key))) AS prd_code --Use TRIM() to ensure no end-spacing
FROM bronze.crm_prd_info;

-- Check data in sales data
SELECT sls_prd_key FROM bronze.crm_sales_details;

-- Check matching for furthur join
SELECT 
	prd_key,
	SUBSTRING(prd_key,1,5) AS cat_id,
	TRIM(SUBSTRING(prd_key,7,LEN(prd_key))) AS prd_code
FROM bronze.crm_prd_info
WHERE TRIM(SUBSTRING(prd_key,7,LEN(prd_key))) NOT IN (
	SELECT sls_prd_key FROM bronze.crm_sales_details);
/*
Result shows that these may be not in sales order
=> Join with caution
*/

-- (3) Check Data Standardization & Consistency

SELECT prd_line, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_line


-- Check for invalid date
-- Expected Outcome: No result

SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;
/*
Validate with relevant departments to propose solutions based on business context
*/

--Proposed rules:
/*
(1) The end_dt will be 'the previous day' of the next production date record for the same products
(2) In case the product was produced only once => The estimated end_dt will be 1 year after start_dt
*/
-- Test logic: Using COALESCE (outcome 1, outcome 2 if NULL) to handle NULL outcome for the (2) rules
SELECT
	prd_key,
	prd_cost,
	prd_start_dt,
	prd_end_dt,
	prd_end_dt_test,
	COALESCE(
		prd_end_dt_test,
		CAST(CAST(prd_start_dt AS DATETIME)+364 AS DATE)) AS prd_end_dt_test_2
FROM (
SELECT
	prd_key,
	prd_cost,
	prd_start_dt,
	prd_end_dt,
	CAST(LEAD(CAST(prd_start_dt AS DATETIME)) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt_test
FROM bronze.crm_prd_info)t;

-- Final Solution:
SELECT 
	prd_key,
	prd_cost,
	prd_start_dt,
	prd_end_dt,
	COALESCE(
		CAST(LEAD(CAST(prd_start_dt AS DATETIME)) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE),
		CAST(CAST(prd_start_dt AS DATETIME)+364 AS DATE)) 
	AS prd_end_dt_test
FROM bronze.crm_prd_info
ORDER BY prd_key, prd_start_dt;

--- Validate 
-- (1) Check for NULL and duplicates in primary key
-- Expected outcome: No result

SELECT
	prd_id,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) >1 OR prd_id IS NULL;

SELECT
	prd_key,
	prd_cost
FROM silver.crm_prd_info
WHERE prd_cost <0 OR prd_cost IS NULL;

-- (2) Check redundant spacing for string values
/* Use TRIM(column) to remove spacing character from string*/
--Expected Outcome: No result
SELECT cat_id
FROM silver.crm_prd_info
WHERE cat_id != TRIM(cat_id);

SELECT prd_code
FROM silver.crm_prd_info
WHERE prd_code != TRIM(prd_code);

-- (3) Check Data Standardization & Consistency after transform


--- Check final outcome
SELECT * FROM bronze.crm_prd_info;
SELECT * FROM silver.crm_prd_info;

/*==================================================================================================
--------------------------------bronze.crm_sales_details-------------------------------------------------
*/

--- Validate 
-- (1) Check for NULL and duplicates
-- Expected outcome: No result
SELECT * FROM bronze.crm_sales_details
WHERE sls_ord_num = 'SO58845';

SELECT sls_ord_num as ord_code, COUNT(sls_ord_num)
FROM (
SELECT 
	sls_ord_num,
	sls_ship_dt,
	COUNT(sls_ord_num) AS 'count'
FROM bronze.crm_sales_details
GROUP BY sls_ord_num, sls_ship_dt
HAVING COUNT(sls_ord_num) >1 OR sls_ord_num IS NULL)t
GROUP BY sls_ord_num
HAVING COUNT(sls_ord_num) >1;

-- (2) Check for unwanted space and join capability
-- Expected Outcome: No result
SELECT sls_ord_num FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

SELECT sls_prd_key FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (
	SELECT prd_code FROM silver.crm_prd_info);

SELECT sls_cust_id FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (
	SELECT cst_id FROM silver.crm_cust_info);

-- (3) Check for invalid date
SELECT sls_ord_num, sls_order_dt FROM bronze.crm_sales_details
WHERE sls_order_dt =0 OR 
	sls_order_dt IS NULL OR 
	LEN(sls_order_dt) !=8 OR 
	sls_order_dt > 20500101 OR 
	sls_order_dt < 19000101;

SELECT sls_ord_num, sls_ship_dt FROM bronze.crm_sales_details
WHERE sls_ship_dt =0 OR 
	sls_ship_dt IS NULL OR 
	LEN(sls_ship_dt) !=8 OR 
	sls_ship_dt > 20500101 OR 
	sls_ship_dt < 19000101;

SELECT sls_ord_num, sls_due_dt FROM bronze.crm_sales_details
WHERE sls_due_dt =0 OR 
	sls_due_dt IS NULL OR 
	LEN(sls_due_dt) !=8 OR 
	sls_due_dt > 20500101 OR 
	sls_due_dt < 19000101;

SELECT * FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- (4) Check business logic for sales price/quantity/total sales
SELECT
	sls_sales,
	sls_price,
	sls_quantity
FROM bronze.crm_sales_details
WHERE 
	sls_sales != sls_price * sls_quantity OR
	sls_sales IS NULL OR sls_price IS NULL OR sls_quantity IS NULL OR
	sls_sales <=0 OR sls_price <=0 OR sls_quantity <=0
ORDER BY sls_sales, sls_quantity, sls_price;

/*
Assuming that sls_quantity is well-recorded since there is no NULL or zero/negative
Solutions:
(1) sls price = ABS(sls_sales)/ABS(sls_quantity)
(2) sls sales = ABS(sls_price) * ABS(sls_quantity)
*/
SELECT
	sls_sales AS old_sales,
	sls_quantity AS quantity,
	sls_price AS old_price,
	CASE WHEN sls_sales IS NULL OR 
		sls_sales != ABS(sls_price)* sls_quantity THEN ABS(sls_price)* sls_quantity
		ELSE sls_sales
	END new_sls_sales,
	sls_quantity,
	CASE WHEN sls_price IS NULL OR sls_price <=0 THEN ABS(sls_sales) / sls_quantity
		ELSE sls_price
	END new_sls_price
FROM bronze.crm_sales_details
WHERE
	sls_sales != sls_price * sls_quantity OR
	sls_sales IS NULL OR sls_price IS NULL OR sls_quantity IS NULL OR
	sls_sales <=0 OR sls_price <=0 OR sls_quantity <=0
ORDER BY sls_sales, sls_quantity, sls_price;

--- Validate
SELECT
	sls_sales,
	sls_price,
	sls_quantity
FROM silver.crm_sales_details
WHERE 
	sls_sales != sls_price * sls_quantity OR
	sls_sales IS NULL OR sls_price IS NULL OR sls_quantity IS NULL OR
	sls_sales <=0 OR sls_price <=0 OR sls_quantity <=0
ORDER BY sls_sales, sls_quantity, sls_price;
--- Check final outcome
SELECT * FROM bronze.crm_sales_details;
SELECT * FROM silver.crm_sales_details;

/*==================================================================================================
--------------------------------bronze.erp_cust_az12-------------------------------------------------
*/
-- Check NULL and duplicates
-- Expected Outcome: No results
SELECT cid, COUNT(*) FROM bronze.erp_cust_az12
GROUP BY cid
HAVING COUNT(*) >1 or cid IS NULL;

-- Check data consistency for future join
SELECT * FROM bronze.erp_cust_az12
WHERE cid NOT LIKE 'AW%';

SELECT * FROM silver.crm_cust_info;

-- Check invalid date
-- Validate minimum bdate and rule-out bdate that is after present date
-- Expected outcome; No result
SELECT MIN(bdate) FROM bronze.erp_cust_az12;

SELECT * FROM bronze.erp_cust_az12
WHERE TRY_CAST(bdate AS DATE) IS NULL OR
	bdate < '1916'  OR bdate > GETDATE();

-- Check data consistency and standardization
SELECT DISTINCT gen FROM bronze.erp_cust_az12;

-- Validate
-- Check data consistency for future join
SELECT * FROM silver.erp_cust_az12
WHERE cid NOT LIKE 'AW%';

-- Check invalid date
SELECT * FROM silver.erp_cust_az12
WHERE bdate < '1916'  OR bdate > GETDATE();

-- Check data consistency and standardization
SELECT DISTINCT gen FROM silver.erp_cust_az12;

/*==================================================================================================
--------------------------------bronze.erp_loc_a101-------------------------------------------------
*/
SELECT * FROM bronze.erp_loc_a101
WHERE cid NOT LIKE '%-%';

SELECT * FROM bronze.erp_loc_a101
WHERE cid != UPPER(TRIM(cid));

SELECT * FROM bronze.erp_loc_a101
WHERE REPLACE(cid,'-','') NOT IN (
	SELECT cst_key FROM silver.crm_cust_info);

SELECT REPLACE(cid,'-','') cid FROM bronze.erp_loc_a101;

SELECT DISTINCT cntry FROM bronze.erp_loc_a101;

SELECT DISTINCT cntry FROM silver.erp_loc_a101;

/*==================================================================================================
--------------------------------bronze.erp_px_cat_g1v2-------------------------------------------------
*/
SELECT * FROM bronze.erp_px_cat_g1v2;

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE id != UPPER(TRIM(id));

SELECT cat_id FROM silver.crm_prd_info;

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat);
SELECT DISTINCT maintainance FROM bronze.erp_px_cat_g1v2;

