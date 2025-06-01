/*
===========================================================================
Stored Procedure: Load Bronze Layer (Source --> Bronze)
===========================================================================
Script Purpose:
	This stored procedure loads raw data into the 'Bronze' schema from external CSV files.
	For each tables, it performs the following actions:
		- Truncates the existed bronze table before loading updated data;
		- Applied 'BULK INSERT' to load updated data from CSV file to the bronze table.

Parameter: None
	This stored procedure does not accept any parameters or return any values

Execution: 
	EXEC bronze.load_bronze
*/
USE data_warehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN -- Start loading procedure
	DECLARE @start_time DATETIME, @end_time DATETIME, @procedure_start_time DATETIME, @procedure_end_time DATETIME;
	BEGIN TRY -- Start trying procedure for handling error
		SET @procedure_start_time = GETDATE();
		PRINT '===================================================';
		PRINT 'Begining loading Bronze Layer';
		PRINT '===================================================';

		PRINT '---------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '---------------------------------------------------';

		SET @start_time = GETDATE()
		PRINT '>>> TRUNCATING TABLES: bronze.crm_cust_info ';
		TRUNCATE TABLE bronze.crm_cust_info; -- Clear existed data

		PRINT '>>> INSERTING TABLES: bronze.crm_cust_info ';
		BULK INSERT bronze.crm_cust_info -- Load updated data into assigned tables
		FROM 'D:\Workspace\DATA ANALYTICS\SQL\1. SQL Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, -- Skip the header row
			FIELDTERMINATOR = ',', -- Identify the seperator/delimeter of each column
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-------------------------------------'

		SET @start_time = GETDATE()
		PRINT '>>> TRUNCATING TABLES: bronze.prd_cust_info ';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>>> INSERTING TABLES: bronze.crm_prd_info ';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\Workspace\DATA ANALYTICS\SQL\1. SQL Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-------------------------------------'

		SET @start_time = GETDATE()
		PRINT '>>> TRUNCATING TABLES: bronze.crm_sales_details ';
		TRUNCATE TABLE bronze.crm_sales_details;
	
		PRINT '>>> INSERTING TABLES: bronze.crm_sales_details ';
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\Workspace\DATA ANALYTICS\SQL\1. SQL Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-------------------------------------'

		PRINT '---------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '---------------------------------------------------';

		SET @start_time = GETDATE()
		PRINT '>>> TRUNCATING TABLES: bronze.erp_cust_az12 ';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>>> INSERTING TABLES: bronze.erp_cust_az12 ';
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\Workspace\DATA ANALYTICS\SQL\1. SQL Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-------------------------------------'

		SET @start_time = GETDATE()
		PRINT '>>> TRUNCATING TABLES: bronze.erp_loc_a101 ';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>>> INSERTING TABLES: bronze.erp_loc_a101 ';
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\Workspace\DATA ANALYTICS\SQL\1. SQL Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-------------------------------------'

		SET @start_time = GETDATE()
		PRINT '>>> TRUNCATING TABLES: bronze.erp_px_cat_g1v2 ';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>>> TRUNCATING TABLES: bronze.erp_px_cat_g1v2 ';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\Workspace\DATA ANALYTICS\SQL\1. SQL Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
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
