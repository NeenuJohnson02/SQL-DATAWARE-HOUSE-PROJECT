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
DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME, @batch_end_time DATETIME;
BEGIN TRY
	SET @batch_start_time=getdate()
	PRINT'=============================================================================';
	PRINT 'Loading bronze layer';
	PRINT'=============================================================================';

	PRINT'-----------------------------------------------------------------------------';
	PRINT'Loading CRM Tables'
	PRINT'-----------------------------------------------------------------------------';
		SET @start_time = getdate();
		PRINT '>> Truncating table : bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info
		PRINT '>> Inserting data into : bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\neenu\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time = getdate();
		PRINT '>>Load Duration :'+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>---------------------';
		
		SET @start_time=getdate()
		PRINT '>> Truncating table : bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info
		PRINT '>> Inserting data into :bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\neenu\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time=getdate();
		PRINT '>>Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>--------------------';

		SET @start_time=getdate();
		PRINT '>> Truncating table :bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details
		PRINT '>> Inserting data into :bronze.crm_sales_details';
		BULK INSERT  bronze.crm_sales_details
		from 'C:\Users\neenu\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time=getdate();
		PRINT '>> Load duration:' +CAST( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
		PRINT '>>---------------------';

		PRINT'-----------------------------------------------------------------------------';
		PRINT 'Loading ERP Tables'
		PRINT'-----------------------------------------------------------------------------';

		SET @start_time = getdate()
		PRINT '>> Truncating table :bronze.erp_CUSTAZ12';
		TRUNCATE TABLE bronze.erp_CUSTAZ12
		PRINT '>> Inserting data into :bronze.erp_CUSTAZ12';
		BULK INSERT bronze.erp_CUSTAZ12
		FROM 'C:\Users\neenu\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time=getdate();
		PRINT '>>Load duration:' +CAST( DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>-------------';

		SET @start_time = getdate();
		PRINT '>> Truncating table :bronze.erp_LOCA101';
		TRUNCATE TABLE bronze.erp_LOCA101
		PRINT '>> Inserting data into :bronze.erp_LOCA101';
		BULK INSERT bronze.erp_LOCA101
		FROM 'C:\Users\neenu\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time=getdate();
		PRINT '>>Load duration:' +CAST( DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>-------------';

		SET @start_time = getdate();
		PRINT '>> Truncating table :bronze.erp_PX_CAT_G1V2';
		TRUNCATE TABLE  bronze.erp_PX_CAT_G1V2
		PRINT '>> Inserting data into :bronze.erp_PX_CAT_G1V2';
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\neenu\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time=getdate();
		PRINT '>>Load duration:' +CAST( DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>-------------';
		SET @batch_end_time=getdate();
		PRINT'===================================================================';
		PRINT'Loading bronze layer is completed';
		PRINT'===================================================================';
		PRINT 'Total load duration:'+ CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT'===================================================================';
    
	END TRY
	
	BEGIN CATCH
		PRINT '=================================================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '=================================================================='
		END CATCH
END
