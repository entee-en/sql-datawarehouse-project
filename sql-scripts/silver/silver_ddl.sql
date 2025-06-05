/*
===========================================================================
DDL Script: Create Tables for Silver Layer
===========================================================================
Script Purpose:
	This script create tables in the 'Silver' schema for future loading procedure.
	If the table exists, it will be dropped and recreated with new DDL structure

Warning: 
	Running this script will drop the existed database. 
	Proceed with caution and ensure you have proper backups before executions.
*/

USE data_warehouse;
GO

IF OBJECT_ID('silver.crm_cust_info','U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
cat_id NVARCHAR(50), -- Create after done data transformation before inserting
prd_code NVARCHAR(50), -- Create after done data transformation before inserting
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id NVARCHAR(50),
sls_order_dt DATE, -- Change from INT to DATE after done data transformation before inserting
sls_ship_dt DATE, -- Change from INT to DATE after done data transformation before inserting
sls_due_dt DATE, -- Change from INT to DATE after done data transformation before inserting
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_cust_az12','U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_loc_a101','U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
cid NVARCHAR(50),
cntry NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2(
id NVARCHAR (50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintainance NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
