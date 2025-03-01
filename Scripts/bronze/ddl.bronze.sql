/*
===================================================================================
DDL Scripts: Create Bronze Tables
===================================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables
    if they already exist.
    Run this script to re-define the DDL structure of 'Bronze' Tables
===================================================================================
*/

if object_id ('bronze.crm_cust_info', 'u') is not null
   drop table bronze.crm_cust_info

go

create table bronze.crm_cust_info (
     cst_id int,
     cst_key nvarchar(50),
     cst_firstname nvarchar(50),
     cst_lastname nvarchar(50),
     cst_material_status nvarchar(50),
     cst_gndr nvarchar(50),
     cst_create_date date
)

if object_id ('bronze.crm_prd_info', 'u') is not null
   drop table bronze.crm_prd_info

go

create table bronze.crm_prd_info (
     prd_id int,
     prd_key nvarchar(50),
     prd_nm nvarchar(50),
     prd_cost int,
     prd_line nvarchar(50),
     prd_start_dt datetime,
     prd_end_dt datetime
)

if object_id ('bronze.crm_sales_details', 'u') is not null
   drop table bronze.crm_sales_details

go

create table bronze.crm_sales_details (
     sls_ord_num nvarchar(50),
     sls_prd_key nvarchar(50),
     sls_cust_id int,
     sls_order_dt int,
     sls_ship_dt int,
     sls_due_dt int,
     sls_sales int,
     sls_quantity int,
     sls_price int
)

if object_id ('bronze.erp_loc_a101', 'u') is not null
   drop table bronze.erp_loc_a101

go

create table bronze.erp_loc_a101 (
     cid nvarchar(50),
     cntry nvarchar(50)
)

if object_id ('bronze.erp_cust_az12', 'u') is not null
   drop table bronze.erp_cust_az12

go

create table bronze.erp_cust_az12 (
     cid nvarchar(50),
     bdate date,
     gen nvarchar(50)
)

if object_id ('bronze.erp_px_cat_g1v2', 'u') is not null
   drop table bronze.erp_px_cat_g1v2

go

create table bronze.erp_px_cat_g1v2 (
     id nvarchar(50),
     cat nvarchar(50),
     subcat nvarchar(50),
     maintenance nvarchar(50)
)

exec bronze.load_bronze

create or alter procedure bronze.load_bronze as
begin
 declare @start_time datetime , @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
 begin try
     print '==========================================';
	 print 'loading bronze layer';
	 print '==========================================';

	 print '------------------------------------------';
	 print 'loading crm tables';
	 print '------------------------------------------';

	 set @start_time = getdate();
	 print '>> truncating table: bronze.crm_cust_info';
     truncate table bronze.crm_cust_info

	 print '>> inserting data into: bronze.crm_cust_info';
     bulk insert bronze.crm_cust_info
     from 'D:\sql dataset\source_crm\cust_info.csv'
     with (
         firstrow = 2,
	     fieldterminator = ',',
	     tablock
	  )
	 set @end_time = getdate();
	 print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
	 print '>> ---------------';

	 set @start_time = getdate();
	 print '>> truncating table: bronze.crm_prd_info';
     truncate table bronze.crm_prd_info

	 print 'inserting data into: bronze.crm_prd_info';
     bulk insert bronze.crm_prd_info
     from 'C:\sql dataset\source_crm\prd_info.csv'
     with (
         firstrow = 2,
	     fieldterminator = ',',
	     tablock
	 )
	 set @end_time = getdate();
	 print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
	 print '>> ---------------';

	 set @start_time = getdate();
	 print 'truncating table: bronze.crm_sales_details';
     truncate table bronze.crm_sales_details

	 print 'inserting data into: bronze.crm_sales-details';
	 bulk insert bronze.crm_sales_details
     from 'D:\sql dataset\source_crm\sales_details.csv'
     with (
         firstrow = 2,
	     fieldterminator = ',',
	     tablock
	 )
	 set @end_time = getdate();
	 print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
	 print '>> ---------------';



	 print '------------------------------------------';
	 print 'loading erp tables';
	 print '------------------------------------------';

	 set @start_time = getdate();
	 print '>> truncating table: bronze.erp_cust_az12';
     truncate table bronze.erp_cust_az12

	 print '>> inserting data into: bronze.erp_cust_az12'; 
	 bulk insert bronze.erp_cust_az12
     from 'D:\sql dataset\source_erp\CUST_AZ12.csv'
     with (
         firstrow = 2,
	     fieldterminator = ',',
	     tablock
	 )
	 set @end_time = getdate();
	 print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
	 print '>> ---------------';

	 set @start_time = getdate();
	 print 'truncating table: bronze.erp_loc_a101';
	 truncate table bronze.erp_loc_a101

	 print 'inserting data into: bronze.erp_loc_a101';
	 bulk insert bronze.erp_loc_a101
     from 'D:\sql dataset\source_erp\LOC_A101.csv'
     with (
         firstrow = 2,
	     fieldterminator = ',',
	     tablock
	 )
	 set @end_time = getdate();
	 print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
	 print '>> ---------------';

	 set @start_time = getdate();
	 print '>> truncating table: bronze.erp_px_cat_g1v2';
	 truncate table bronze.erp_px_cat_g1v2

	 print '>> inserting data into: bronze.erp_px_cat_g1v2';
	 bulk insert bronze.erp_px_cat_g1v2
     from 'D:\sql dataset\source_erp\PX_CAT_G1V2.csv'
     with (
         firstrow = 2,
	     fieldterminator = ',',
	     tablock
	 )
	 set @end_time = getdate();
	 print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
	 print '>> ---------------';

	 set @batch_end_time = getdate();
	 print '==========================================================='
	 print 'loading bronze layer is completed';
	 print 'total load duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds';
	 end try
	 begin catch
	     print '========================================================'
		 print 'error occured during loading bronze layer'
		 print 'error message' + error_message();
		 print 'error message' + cast (error_number() as nvarchar);
		 print 'error message' + cast (error_state() as nvarchar);
		 print '========================================================'
	 end catch
end
