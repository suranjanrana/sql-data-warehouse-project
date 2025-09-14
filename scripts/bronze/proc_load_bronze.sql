CREATE OR REPLACE PROCEDURE bronze.proc_load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP := current_timestamp(3);
    batch_end_time TIMESTAMP;

BEGIN
    BEGIN
        RAISE NOTICE '================================================';
        RAISE NOTICE 'Loading Bronze Layer';
        RAISE NOTICE '================================================';


        RAISE NOTICE '------------------------------------------------';
        RAISE NOTICE 'Loading CRM Tables';
        RAISE NOTICE '------------------------------------------------';

        start_time := current_timestamp(3);
        RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
        COPY bronze.crm_cust_info
        FROM 'D:/Projects/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
        WITH (
            FORMAT csv,
            DELIMITER ',',
            HEADER
        );
        end_time := current_timestamp(3);
        RAISE NOTICE '>> Load Duration: % ms', extract(milliseconds from (end_time - start_time));
        RAISE NOTICE '>> -------------';



        start_time := current_timestamp(3);
        RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
        COPY bronze.crm_prd_info
        FROM 'D:/Projects/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
        WITH (
            FORMAT csv,
            DELIMITER ',',
            HEADER
        );
        end_time := current_timestamp(3);
        RAISE NOTICE '>> Load Duration: % ms', extract(milliseconds from (end_time - start_time));
        RAISE NOTICE '>> -------------';



        start_time := current_timestamp(3);
        RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
        COPY bronze.crm_sales_details
        FROM 'D:/Projects/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
        WITH (
            FORMAT csv,
            DELIMITER ',',
            HEADER
        );
        end_time := current_timestamp(3);
        RAISE NOTICE '>> Load Duration: % ms', extract(milliseconds from (end_time - start_time));
        RAISE NOTICE '>> -------------';



        RAISE NOTICE '------------------------------------------------';
        RAISE NOTICE 'Loading CRM Tables';
        RAISE NOTICE '------------------------------------------------';


        start_time := current_timestamp(3);
        RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
        COPY bronze.erp_loc_a101
        FROM 'D:/Projects/sql-data-warehouse-project/datasets/source_erp/loc_a101.csv'
        WITH (
            FORMAT csv,
            DELIMITER ',',
            HEADER
        );
        end_time := current_timestamp(3);
        RAISE NOTICE '>> Load Duration: % ms', extract(milliseconds from (end_time - start_time));
        RAISE NOTICE '>> -------------';



        start_time := current_timestamp(3);
        RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
        COPY bronze.erp_cust_az12
        FROM 'D:/Projects/sql-data-warehouse-project/datasets/source_erp/cust_az12.csv'
        WITH (
            FORMAT csv,
            DELIMITER ',',
            HEADER
        );
        end_time := current_timestamp(3);
        RAISE NOTICE '>> Load Duration: % ms', extract(milliseconds from (end_time - start_time));
        RAISE NOTICE '>> -------------';



        start_time := current_timestamp(3);
        RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        COPY bronze.erp_px_cat_g1v2
        FROM 'D:/Projects/sql-data-warehouse-project/datasets/source_erp/px_cat_g1v2.csv'
        WITH (
            FORMAT csv,
            DELIMITER ',',
            HEADER
        );
        end_time := current_timestamp(3);
        RAISE NOTICE '>> Load Duration: % ms', extract(milliseconds from (end_time - start_time));
        RAISE NOTICE '>> -------------';


        batch_end_time := current_timestamp(3);
        RAISE NOTICE '================================================';
        RAISE NOTICE 'Bronze Layer Load Completed';
        RAISE NOTICE 'Total Batch Duration: % ms', extract(milliseconds from (batch_end_time - batch_start_time));
        RAISE NOTICE '================================================';

    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '================================================';
            RAISE NOTICE 'Error Occurred During Bronze Layer Load';
            RAISE NOTICE 'Error Code: %', SQLSTATE;
            RAISE NOTICE 'Error Message: %', SQLERRM;
            RAISE NOTICE 'Batch Aborted';
            RAISE NOTICE '================================================';
    END;
END; 
$$;
