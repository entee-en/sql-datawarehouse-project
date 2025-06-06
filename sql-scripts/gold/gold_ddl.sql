/*
===========================================================================
DDL Script: Create Views for Gold Layer
===========================================================================
Script Purpose:
	This script create view for 2 dimension views (Customer & Product)
	and 1 fact view (Sales) in the 'Gold' schema for business usage.
		- If the view exists, it will be dropped and recreated with new view. 
		- Create surrogate key for database connection following star schema model.
		- Integrate data from 'Silver' schema with meaningful column name
			to produce cleaned and business-ready datasets.

Warning: 
	Running this script will drop the existed database. 
	Proceed with caution and ensure you have proper backups before executions.
*/

USE data_warehouse;
GO

IF OBJECT_ID('gold.dim_customer','V') IS NOT NULL
	DROP VIEW gold.dim_customer;
GO

CREATE VIEW gold.dim_customer AS
SELECT
	ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_code,
	CONCAT(ci.cst_firstname, ' ',ci.cst_lastname) AS customer_name,
	CASE WHEN ci.cst_gndr != 'n/a' THEN TRIM(ci.cst_gndr)
		ELSE COALESCE(TRIM(ca.gen),'n/a')
	END AS customer_gender,
	ca.bdate AS birthdate,
	la.cntry AS country,
	ci.cst_marital_status AS marital_stauts
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
	ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
	ON ci.cst_key = la.cid;
GO

IF OBJECT_ID('gold.dim_product','V') IS NOT NULL
	DROP VIEW gold.dim_product;
GO

CREATE VIEW gold.dim_product AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY pd.prd_id) AS product_key,
	pd.prd_id AS product_id,
	CASE WHEN cat.id IS NULL THEN pd.cat_id
		ELSE cat.id
	END AS category_id,
	CASE WHEN cat.cat IS NULL AND SUBSTRING(pd.cat_id,1,2) ='CO' THEN 'Components'
		ELSE cat.cat
	END AS category,
	CASE WHEN cat.subcat IS NULL AND SUBSTRING(pd.prd_code,1,4) = 'PD-T' THEN CONCAT(pd.prd_nm,'s')
		WHEN cat.subcat IS NULL THEN CONCAT(SUBSTRING(pd.prd_nm,4,LEN(pd.prd_nm)),'s')
		ELSE cat.subcat
	END AS sub_category,
	pd.prd_code AS product_code,
	pd.prd_line AS production_line,
	pd.prd_nm AS product_name,
	pd.prd_cost AS production_cost,
	cat.maintainance AS maintainance
FROM silver.crm_prd_info AS pd
LEFT JOIN silver.erp_px_cat_g1v2 AS cat
	ON pd.cat_id=cat.id;
GO

IF OBJECT_ID('gold.fact_sales','V') IS NOT NULL
	DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
	sl.sls_order_dt AS order_date,
	sl.sls_ord_num AS order_no,
	sl.sls_prd_key AS product_code,
	dp.product_key AS product_key,
	sl.sls_cust_id AS customer_id,
	dc.customer_key AS customer_key,
	sl.sls_ship_dt AS shipping_date,
	sl.sls_due_dt AS due_date,
	sl.sls_price AS price,
	sl.sls_quantity AS quantity,
	sl.sls_sales AS total_sales
FROM silver.crm_sales_details sl
LEFT JOIN gold.dim_customer dc
	ON sl.sls_cust_id = dc.customer_id
LEFT JOIN gold.dim_product dp
	ON sl.sls_prd_key = dp.product_code;
GO
