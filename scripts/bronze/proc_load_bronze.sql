/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '========================';
		PRINT 'Loading Bronze Layer';
		PRINT '========================';

		PRINT '------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------';

		SET @start_time = GETDATE();
		PRINT 'Loading Bronze Customer Information Data';
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\18dee\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
		PRINT '-----------------'

		SET @start_time = GETDATE();
		PRINT 'Loading Bronze Product Information Data';
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\18dee\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
		PRINT '-----------------'

		SET @start_time = GETDATE();
		PRINT 'Loading Bronze Sales Details Data';
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\18dee\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
		PRINT '-----------------'

		PRINT '------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------';

		SET @start_time = GETDATE();
		PRINT 'Loading Bronze Customer  Data';
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\18dee\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
		PRINT '-----------------'

		SET @start_time = GETDATE();
		PRINT 'Loading Bronze Location  Data';
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\18dee\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
		PRINT '-----------------'

		SET @start_time = GETDATE();
		PRINT 'Loading Bronze  Data';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\18dee\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
		PRINT '-----------------'

		PRINT '===================================';
		SET @batch_end_time = GETDATE();
		PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
		PRINT '===================================';
	END TRY
	BEGIN CATCH
		PRINT('================================');
		PRINT('ERROR OCCURED DURING EXECUTION');
		PRINT('=================================');
		PRINT('ERROR MESSAGE =' + ERROR_MESSAGE());
		PRINT('ERROR MESSAGE =' + CAST(ERROR_NUMBER() AS NVARCHAR));
		PRINT('ERROR MESSAGE =' + CAST(ERROR_LINE() AS NVARCHAR));
	END CATCH
END
