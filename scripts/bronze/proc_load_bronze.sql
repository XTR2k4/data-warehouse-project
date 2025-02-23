/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables if it exists before loading data.
    - The `COPY` command is used for fast bulk data loading from csv files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();
===============================================================================
*/
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$ 
BEGIN
	DECLARE 
		batch_start_time TIMESTAMP;
		batch_end_time TIMESTAMP;
		start_time TIMESTAMP;
		end_time TIMESTAMP;
    BEGIN
      batch_start_time = NOW();
      RAISE NOTICE '================================================';
      RAISE NOTICE 'Loading Bronze Layer';
      RAISE NOTICE '================================================';

      RAISE NOTICE '------------------------------------------------';
      RAISE NOTICE 'Loading CRM Tables';
      RAISE NOTICE '------------------------------------------------';

      start_time = NOW();
      RAISE NOTICE '>>Truncating Table: bronze.crm_cust_info';
      TRUNCATE bronze.crm_cust_info;

      RAISE NOTICE '>>Inserting Data into: bronze.crm_cust_info';
      COPY bronze.crm_cust_info
      FROM 'D:\Data Engineering\Data Warehouse project\Data\source_crm\cust_info.csv'
      DELIMITER ','
      CSV HEADER;
      end_time = NOW();
      RAISE NOTICE '>>Load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time))::NUMERIC, 3);
      RAISE NOTICE '------------------------------------------------';

      start_time = NOW();
      RAISE NOTICE '>>Truncating Table: bronze.crm_prd_info';
      TRUNCATE bronze.crm_prd_info;

      RAISE NOTICE '>>Inserting Data into: bronze.crm_prd_info';
      COPY bronze.crm_prd_info
      FROM 'D:\Data Engineering\Data Warehouse project\Data\source_crm\prd_info.csv'
      DELIMITER ','
      CSV HEADER;
      end_time = NOW();
      RAISE NOTICE '>>Load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time))::NUMERIC, 3);
      RAISE NOTICE '------------------------------------------------';

      start_time = NOW();
      RAISE NOTICE '>>Truncating Table: bronze.crm_sales_details';
      TRUNCATE bronze.crm_sales_details;

      RAISE NOTICE '>>Inserting Data into: bronze.crm_sales_details';
      COPY bronze.crm_sales_details
      FROM 'D:\Data Engineering\Data Warehouse project\Data\source_crm\sales_details.csv'
      DELIMITER ','
      CSV HEADER;
      end_time = NOW();
      RAISE NOTICE '>>Load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time))::NUMERIC, 3);


      RAISE NOTICE '------------------------------------------------';
      RAISE NOTICE 'Loading ERP Tables';
      RAISE NOTICE '------------------------------------------------';

      start_time = NOW();
      RAISE NOTICE '>>Truncating Table: bronze.erp_cust_az12';
      TRUNCATE bronze.erp_cust_az12;

      RAISE NOTICE '>>Inserting Data into: bronze.erp_cust_az12';
      COPY bronze.erp_cust_az12
      FROM 'D:\Data Engineering\Data Warehouse project\Data\source_erp\CUST_AZ12.csv'
      DELIMITER ','
      CSV HEADER;
      end_time = NOW();
      RAISE NOTICE '>>Load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time))::NUMERIC, 3);
      RAISE NOTICE '------------------------------------------------';
  

      start_time = NOW();
      RAISE NOTICE '>>Truncating Table: bronze.erp_loc_a101';
      TRUNCATE bronze.erp_loc_a101;

      RAISE NOTICE '>>Inserting Data into: bronze.erp_loc_a101';
      COPY bronze.erp_loc_a101
      FROM 'D:\Data Engineering\Data Warehouse project\Data\source_erp\LOC_A101.csv'
      DELIMITER ','
      CSV HEADER;
      end_time = NOW();
      RAISE NOTICE '>>Load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time))::NUMERIC, 3);
      RAISE NOTICE '------------------------------------------------';
  

      start_time = NOW();
      RAISE NOTICE '>>Truncating Table: bronze.erp_px_cat_g1v2';
      TRUNCATE bronze.erp_px_cat_g1v2;

      RAISE NOTICE '>>Inserting Data into: bronze.erp_px_cat_g1v2';
      COPY bronze.erp_px_cat_g1v2
      FROM 'D:\Data Engineering\Data Warehouse project\Data\source_erp\PX_CAT_G1V2.csv'
      DELIMITER ','
      CSV HEADER;
      end_time = NOW();
      RAISE NOTICE '>>Load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time))::NUMERIC, 3);
      RAISE NOTICE '------------------------------------------------';
      batch_end_time = NOW();

      RAISE NOTICE '================================================';
      RAISE NOTICE 'Loading Bronze Layer Completed';
      RAISE NOTICE '================================================';		
      RAISE NOTICE '>>Total Load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM (end_time - start_time))::NUMERIC, 3);
			
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'ERROR OCCURED WHILE LOADING THE BRONZE LAYER: %, SQLSTATE: %', SQLERRM, SQLSTATE;
    END;
END;
$$;
