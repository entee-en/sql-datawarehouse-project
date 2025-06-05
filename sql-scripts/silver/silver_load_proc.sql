/*
===========================================================================
Stored Procedure: Extract, Transform, and Load (ETL) Silver Layer (Bronze --> Silver)
===========================================================================
Script Purpose:
	This stored procedure Extract, Transform, and Load data (ETL) into the 'Silver' schema from 'Bronze' schema.
	For each tables, it performs the following actions:
		- Truncates the existed silver tables before loading updated data;
		- Insert transformed and cleansed data from Bronze schema into Silver tables. 

Parameter: None
	This stored procedure does not accept any parameters or return any values

Execution: 
	EXEC silver.load_silver
*/


USE data_warehouse;
GO

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN -- Start loading procedure
	DECLARE @start_time DATETIME, @end_time DATETIME, @procedure_start_time DATETIME, @procedure_end_time DATETIME;
	BEGIN TRY -- Start trying procedure for handling error
		SET @procedure_start_time = GETDATE();
		PRINT '===================================================';
		PRINT 'Begining loading Silver Layer';
		PRINT '===================================================';

		PRINT '---------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '---------------------------------------------------';

		SET @start_time = GETDATE()
		PRINT '>>> TRUNCATING TABLES: silver.crm_cust_info ';
		TRUNCATE TABLE silver.crm_cust_info;

		PRINT '>>> INSERTING TABLES: silver.crm_cust_info ';
		PRINT '>> BEGIN TRANSFORMATION';
		PRINT '........Removing duplicates';
		PRINT '........Trimming spaces';
		PRINT '........Handling NULL values';
		PRINT '........Standardizing data';
		PRINT '>> TRANSFORMATION COMPLETED';
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		--- Start Cleaning & Standardizing data
		SELECT
			cst_id,
			TRIM(cst_key) AS cst_key,
			TRIM(cst_firstname) AS cst_firstname, -- Remove unwanted spacing
			TRIM(cst_lastname) AS cst_lastname,
			CASE UPPER(TRIM(cst_marital_status))
				WHEN 'S' THEN 'Single'
				WHEN 'M' THEN 'Married'
				ELSE 'n/a'
			END cst_marital_status, -- Normalize values to readable and meaningful format
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END cst_gndr,
			cst_create_date
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
			FROM bronze.crm_cust_info )t
		WHERE flag_last = 1 AND  cst_id IS NOT NULL; -- Remove null and filter the most recent & relevant data
		SET @end_time = GETDATE()
		PRINT '>> ETL Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-------------------------------------'
		
		SET @start_time = GETDATE()
		PRINT '>>> TRUNCATING TABLES: silver.crm_prd_info ';
		TRUNCATE TABLE silver.crm_prd_info;

		PRINT '>>> INSERTING TABLES: silver.crm_prd_info ';
		PRINT '>> BEGIN TRANSFORMATION';
		PRINT '........Removing duplicates';
		PRINT '........Trimming spaces';
		PRINT '........Deriving columns';
		PRINT '........Handling NULL values';
		PRINT '........Handling ill-logical date data';
		PRINT '........Standardizing data';
		PRINT '>> TRANSFORMATION COMPLETED';
		INSERT INTO silver.crm_prd_info(
			prd_id,
			prd_key,
			cat_id,
			prd_code,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		--- Start Cleaning & Standardizing data
		SELECT
			prd_id,
			TRIM(prd_key) AS prd_key,
			SUBSTRING(prd_key,1,5) AS cat_id,
			TRIM(SUBSTRING(prd_key,7,LEN(prd_key))) AS prd_code, -- Split old prd_key into cat_id and prd_code
			TRIM(prd_nm) AS prd_nm,
			ISNULL(prd_cost,0) AS prd_cost, -- Handle null values for prd_cost
			CASE UPPER(TRIM(prd_line)) 
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END prd_line, -- Normalize values to readable and meaningful format
			prd_start_dt,
			COALESCE(
				CAST(LEAD(CAST(prd_start_dt AS DATETIME)) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE),
				CAST(CAST(prd_start_dt AS DATETIME)+364 AS DATE)
			) AS prd_end_dt -- Handle ill-logical date data
		FROM bronze.crm_prd_info ;
		SET @end_time = GETDATE()
		PRINT '>> ETL Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-------------------------------------'

		SET @start_time = GETDATE()
		PRINT '>>> TRUNCATING TABLES: silver.crm_sales_details ';
		TRUNCATE TABLE silver.crm_sales_details;

		PRINT '>>> INSERTING TABLES: silver.crm_sales_details ';
		PRINT '>> BEGIN TRANSFORMATION';
		PRINT '........Removing duplicates';
		PRINT '........Trimming spaces';
		PRINT '........Handling NULL values';
		PRINT '........Converting date data';
		PRINT '........Handling mathematical errors';
		PRINT '........Standardizing data';
		PRINT '>> TRANSFORMATION COMPLETED';
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		--- Start Cleaning & Standardizing data
		SELECT 
			TRIM(sls_ord_num) AS sls_ord_num,
			TRIM(sls_prd_key) AS sls_prd_key,
			TRIM(sls_cust_id) AS sls_cust_id,
			CASE WHEN sls_order_dt =0 OR -- Handling NULL and invalid data 
				LEN(sls_order_dt) !=8 OR
				sls_order_dt > 20500101 OR 
				sls_order_dt < 19000101 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END sls_order_dt,
			CASE WHEN sls_ship_dt =0 OR -- Handling NULL and invalid data
				LEN(sls_ship_dt) !=8 OR
				sls_ship_dt > 20500101 OR 
				sls_ship_dt < 19000101 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END sls_ship_dt,
			CASE WHEN sls_due_dt =0 OR -- Handling NULL and invalid data
				LEN(sls_due_dt) !=8 OR
				sls_due_dt > 20500101 OR 
				sls_due_dt < 19000101 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END sls_due_dt,
			CASE WHEN sls_sales IS NULL OR -- Recalculating to handle mathematical errors and NULL
				sls_sales != ABS(sls_price)* ABS(sls_quantity) THEN ABS(sls_price)* ABS(sls_quantity)
				ELSE sls_sales
			END sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR -- Recalculating to handle mathematical errors and NULL
				sls_price <=0 THEN ABS(sls_sales) / NULLIF(ABS(sls_quantity),0)
				ELSE sls_price
			END sls_price
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE()
		PRINT '>> ETL Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-------------------------------------'

		PRINT '---------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '---------------------------------------------------';

		SET @start_time = GETDATE()
		PRINT '>>> TRUNCATING TABLES: silver.erp_cust_az12 ';
		TRUNCATE TABLE silver.erp_cust_az12;

		PRINT '>>> INSERTING TABLES: silver.erp_cust_az12 ';
		PRINT '>> BEGIN TRANSFORMATION';
		PRINT '........Removing duplicates';
		PRINT '........Trimming spaces';
		PRINT '........Handling NULL values';
		PRINT '........Standardizing data';
		PRINT '>> TRANSFORMATION COMPLETED';
		
		INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
		--- Start Cleaning & Standardizing data
		SELECT
			CASE WHEN UPPER(TRIM(cid)) LIKE 'NAS%'THEN TRIM(SUBSTRING(cid,4,LEN(cid)))
				ELSE TRIM(cid)
			END cid,
			CASE WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END bdate,
			CASE 
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				ELSE 'n/a'
			END gen -- Normalize values to readable and meaningful format
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE()
		PRINT '>> ETL Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-------------------------------------'


		SET @start_time = GETDATE()
		PRINT '>>> TRUNCATING TABLES: silver.erp_loc_a101 ';
		TRUNCATE TABLE silver.erp_loc_a101;

		PRINT '>>> INSERTING TABLES: silver.erp_loc_a101 ';
		PRINT '>> BEGIN TRANSFORMATION';
		PRINT '........Removing duplicates';
		PRINT '........Trimming spaces';
		PRINT '........Handling NULL values';
		PRINT '........Standardizing data';
		PRINT '>> TRANSFORMATION COMPLETED';
		
		INSERT INTO silver.erp_loc_a101(cid, cntry)
		--- Start Cleaning & Standardizing data
		SELECT
			TRIM(REPLACE(cid,'-','')) AS cid,
			CASE WHEN UPPER(TRIM(cntry)) IN ('US', 'USA', 'UNITED STATES') THEN 'United States'
				WHEN UPPER(TRIM(cntry)) IN ('DE', 'GERMANY') THEN 'Germany'
				WHEN cntry IS NULL OR cntry = '' THEN 'n/a'
				ELSE TRIM(cntry)
			END ctry -- Normalize values to readable and meaningful format
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE()
		PRINT '>> ETL Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-------------------------------------'


		SET @start_time = GETDATE()
		PRINT '>>> TRUNCATING TABLES: silver.erp_px_cat_g1v2 ';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;

		PRINT '>>> INSERTING TABLES: silver.erp_px_cat_g1v2 ';
		PRINT '>> BEGIN TRANSFORMATION';
		PRINT '........Removing duplicates';
		PRINT '........Trimming spaces';
		PRINT '........Handling NULL values';
		PRINT '........Standardizing data';
		PRINT '>> TRANSFORMATION COMPLETED';
		
		INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintainance)
		--- Start Cleaning & Standardizing data
		SELECT
			REPLACE(TRIM(id),'_','-') AS id, -- Standardize syntax for future join
			cat,
			subcat,
			maintainance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE()
		PRINT '>> ETL Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-------------------------------------'
		
		SET @procedure_end_time = GETDATE();

		PRINT '===================================================';
		PRINT 'Loading procedure is completed'
		PRINT '		Total Duration:' + CAST(DATEDIFF(SECOND, @procedure_start_time, @procedure_end_time) AS NVARCHAR) + 'seconds';
		PRINT '===================================================';
	END TRY
	BEGIN CATCH --Start handling errors
		PRINT 'ERROR OCCURRED DURING EXECUTION:'
		PRINT 'Number of errors' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'State' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT 'Error Message' + ERROR_MESSAGE();
	END CATCH
END; -- End loading procedure
