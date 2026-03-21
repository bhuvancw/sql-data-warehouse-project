create or alter procedure silver.load_silver as 
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;	
	begin try
		set @batch_start_time = GETDATE();
		print '=======================================================';
		print 'Loading Silver Layer';
		print '=======================================================';
		print '-------------------------------------------------------';
		print 'Loading CRM Tables';
		print '-------------------------------------------------------'

		set @start_time = GETDATE();
		print '>>Truncating silver.crm_cust_info';
		truncate table silver.crm_cust_info;
		print '>>Inserting data into silver.crm_cust_info';
		insert into silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		)
		select
		cst_id,
		cst_key,
		trim(cst_firstname),
		trim(cst_lastname),
		case
			when upper(trim(cst_marital_status)) = 'M' then 'Married'
			when upper(trim(cst_marital_status)) = 'S' then 'Single  '
			else 'n/a'
		end cst_marital_status,
		case
			when upper(trim(cst_gndr)) = 'F' then 'Female'
			when upper(trim(cst_gndr)) = 'M' then 'Male'
			else 'n/a'
		end cst_gndr,
		cst_create_date
		from
		(
		select
		*,
		row_number() over(partition by cst_id order by cst_create_date desc) as rnk
		from bronze.crm_cust_info
		where cst_id is not null
		)t where rnk = 1;

		set @end_time = GETDATE();
		print '>>Load Duration: '+cast(datediff(second,@start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------------------';
		-------------------------------------------------------------
		
		set @start_time = GETDATE();
		print '>>Truncating silver.crm_prd_info';
		truncate table silver.crm_prd_info;
		print '>>Inserting data into silver.crm_prd_info';
		insert into silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		select
		prd_id,
		replace(substring(prd_key,1,5),'-','_') cat_id,
		substring(prd_key, 7, len(prd_key)) prd_key,
		prd_nm,
		isnull(prd_cost, 0) prd_cost,
		case upper(trim(prd_line))
			when 'M' then 'Mountain'
			when 'R' then 'Road'
			when 'S' then 'Other Sales'
			when 'T' then 'Touring'
			else 'n/a'
		end as prd_line,
		cast(prd_start_dt as date),
		cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) - 1 as date) as prd_end_dt
		from bronze.crm_prd_info;

		set @end_time = GETDATE();
		print '>>Load Duration: '+cast(datediff(second,@start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------------------';
		-------------------------------------------------------------------------------
		set @start_time = GETDATE();
		print '>>Truncating silver.crm_sales_details';
		truncate table silver.crm_sales_details;
		print '>>Inserting data into silver.crm_sales_details';
		insert into silver.crm_sales_details(
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
		select
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			case
				when sls_order_id = 0 or len(sls_order_id) != 8 then null
				else cast(cast(sls_order_id as varchar) as date)
			end as sls_order_dt,
			case
				when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
				else cast(cast(sls_ship_dt as varchar) as date)
			end as sls_ship_dt,
			case
				when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
				else cast(cast(sls_due_dt as varchar) as date)
			end as sls_due_dt,
			case
				when sls_sales <> sls_quantity * abs(sls_price) or sls_sales <= 0 or sls_sales is null 
					then sls_quantity * abs(sls_price)
				else sls_sales
			end as sls_sales,
			sls_quantity,
			case
				when sls_price is null or sls_price <= 0
					then sls_quantity/nullif(sls_price,0)
				else sls_price
			end as sls_price
		from bronze.crm_sales_details;
		set @end_time = GETDATE();
		print '>>Load Duration: '+cast(datediff(second,@start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------------------';

		---------------------------------------------------------
		print '-------------------------------------------------------';
		print 'Loading ERP Tables';
		print '-------------------------------------------------------'


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		SELECT
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
				ELSE cid
			END AS cid, 
			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate, -- Set future birthdates to NULL
			CASE
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen -- Normalize gender values and handle unknown cases
		FROM bronze.erp_cust_az12;
	    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		--------------------------------------------------------------------

		set @start_time = GETDATE();
		print '>>Truncating silver.erp_loc_a101';
		truncate table silver.erp_loc_a101;
		print '>>Inserting data into silver.erp_loc_a101';
		insert into silver.erp_loc_a101( cid, cntry)
		select
			replace(cid,'-','') cid,
			case
				when trim(cntry) = 'DE' then 'Germany'
				when trim(cntry) in ('US','USA') then 'United States'
				when trim(cntry) = '' or cntry is null then 'n/a'
				else trim(cntry)
			end as cntry
		from bronze.erp_loc_a101;
		set @end_time = GETDATE();
		print '>>Load Duration: '+cast(datediff(second,@start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------------------';

		-----------------------------------------------------------
		
		set @start_time = GETDATE();	
		print '>>Truncating silver.erp_px_cat_g1v2';
		truncate table silver.erp_px_cat_g1v2;
		print '>>Inserting data into silver.erp_px_cat_g1v2';
		insert into silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
		)
		select
			id,
			cat,
			subcat,
			maintenance
		from bronze.erp_px_cat_g1v2;
		set @end_time = GETDATE();
		print '>>Load Duration:'+ cast(datediff(second,@start_time, @end_time) as nvarchar) + 'seconds';
		print '---------------------------------';

		set @batch_end_time = getdate();
		print '>>Loading Silver is completed';
		print 'Total duration: ' +cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds';
		print '===========================================================';

	end try
	begin catch
		print '===========================================================';
		print 'ERROR OCCURED DURING LOADING SILVER LATER';
		print 'ERROR MESSAGE ' + ERROR_MESSAGE();
		print 'ERROR NUMBER ' + cast(error_number() as nvarchar);
		print 'ERROR STATE ' + cast(error_state() as nvarchar);
	end catch
end;
