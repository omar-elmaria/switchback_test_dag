# Step 1: Load
import pandas as pd
import numpy as np
import seaborn as sns
import scipy.stats
from google.cloud import bigquery
from google.cloud import bigquery_storage
import datetime as dt
import re
import warnings
warnings.filterwarnings(action = 'ignore') # Suppresses pandas warnings

###--------------------------------------------------------END OF STEP 1--------------------------------------------------------###

def analysis_script_func():
    # Step 2: Define some input parameters to query the relevant test data
    df_raw_data_tbl_name = 'ab_test_individual_orders_cleaned_switchback_tests' # This is the table that contains the cleaned data of switchback tests
    p_val_tbl_id = 'dh-logistics-product-ops.pricing.p_vals_switchback_tests' # The table containing the p-values of metrics. This table will be uploaded to BQ at the end of the script

    ###--------------------------------------------------------END OF STEP 2--------------------------------------------------------###

    # Step 3.1: Retrieve the switchback test configurations
    client = bigquery.Client(project = 'logistics-data-staging-flat') # Instantiate a BQ client and define the project
    bq_storage_client = bigquery_storage.BigQueryReadClient() # Instantiate a BQ storage client

    # The switchback_test_configs_bq table gets updated every hour via a scheduled query
    sb_test_configs = client.query("""SELECT * FROM `dh-logistics-product-ops.pricing.switchback_test_configs_bq`""")\
        .result()\
        .to_dataframe(bqstorage_client = bq_storage_client, progress_bar_type = 'tqdm_notebook')

    ###--------------------------------------------------------END OF STEP 3.1--------------------------------------------------------###

    # Step 3.2: Extract the scheme IDs from between the curly brackets
    # Apply the extraction function on the "scheme_id_on" and "scheme_id_off" columns
    sb_test_configs['scheme_id_on'] = sb_test_configs['scheme_id_on'].apply(lambda x: re.findall('\{(.*?)\}', x)[0])
    sb_test_configs['scheme_id_off'] = sb_test_configs['scheme_id_off'].apply(lambda x: re.findall('\{(.*?)\}', x)[0])

    ###--------------------------------------------------------END OF STEP 3.2--------------------------------------------------------###

    ### Step 3.3: Create a list of dicts storing the config info of the switchback tests
    # Declare an empty dict that will be contain the details of a particular switchback test in each for loop iteration
    # The keys of the dict are the column names of sb_test_configs
    test_config_dict = {}
    keys = list(sb_test_configs.columns)

    # Declare an empty list that will contain all the dicts storing the test config information
    test_config_lod = []

    # Populate the list of dicts (lod) with the test configuration info
    for i in range(0, len(sb_test_configs.index)): # Enumerate over the number of tests
        for key in keys: # Populate an intermediary dict with the config info of the test belonging to the current iteration
            test_config_dict[key] = sb_test_configs[key][i]
        test_config_lod.append(test_config_dict) # Append the intermediary dict to the list of dicts
        test_config_dict = {} # Empty the dict so that it can be populated again

    ###--------------------------------------------------------END OF STEP 3.3--------------------------------------------------------###

    ### Step 3.4: Amend the structure of the list of dicts so that "zone" and "scheme" columns contain lists instead of strings
    for test in range(0, len(test_config_lod)): # Iterate over the test dicts
        for key in ['zone_name_vendor_excl', 'zone_name_customer_excl', 'scheme_id_on', 'scheme_id_off']: # Iterate over these keys specifically to change their contents to a list
            if test_config_lod[test][key] == None: # If the value of the key is None, change it to an empty list
                test_config_lod[test][key] = []
            else: 
                test_config_lod[test][key] = test_config_lod[test][key].split(', ') # Split the components of the string into list elements
            
            if 'scheme_id' in key: # If the key being accessed contains the word "scheme_id", change the components of the list to integers using list comprehension
                test_config_lod[test][key] = [int(sch) for sch in test_config_lod[test][key]]
            else:
                pass

    ###--------------------------------------------------------END OF STEP 3.4--------------------------------------------------------###

    ### Step 4.1: Get the curated data from the resulting BQ table
    df_raw_data = client.query("""SELECT * FROM `dh-logistics-product-ops.pricing.{}`"""\
        .format(df_raw_data_tbl_name))\
        .result()\
        .to_dataframe(bqstorage_client = bq_storage_client, progress_bar_type = 'tqdm_notebook')

    ###--------------------------------------------------------END OF STEP 4.1--------------------------------------------------------###

    ### Step 4.2: Change the data types of columns in the dataset
    # Define the start of the data frame where the data types of columns need to be changed 
    col_start = np.where(df_raw_data.columns == 'exchange_rate')[0][0]

    # Change data types --> df[df.cols = specific cols].apply(pd.to_numeric)
    df_raw_data[df_raw_data.columns[col_start:]] = df_raw_data[df_raw_data.columns[col_start:]].apply(pd.to_numeric, errors = 'ignore')

    ###--------------------------------------------------------END OF STEP 4.2--------------------------------------------------------###

    ### Step 5: Filter the data frame for the relevant data, add a few supplementary columns, then calculate the agg metrics and p-values
    # Define the list of KPIs
    col_list = [
        'actual_df_paid_by_customer', 'gfv_local', 'gmv_local', 'commission_local', 'joker_vendor_fee_local', # Customer KPIs (1)
        'sof_local', 'service_fee_local', 'revenue_local', 'delivery_costs_local', 'gross_profit_local', # Customer KPIs (2)
        'dps_mean_delay', 'delivery_distance_m', 'actual_DT' # Logistics KPIs
    ]

    # Initialize empty lists for the total/per order metrics and p-values. These will be later changed to data frames
    df_final_per_order = []
    df_final_tot = []
    df_final_pval = []

    # Iterate over the original data frame, filtering for the relevant test data every time and computing the per-order metrics
    for i in range(0, len(test_config_lod)):
        df_temp = df_raw_data[
            (df_raw_data['target_group'] != 'Non_TG') &
            (df_raw_data['test_name'] == test_config_lod[i]['test_name']) &
            (~ df_raw_data['zone_name_vendor'].isin(test_config_lod[i]['zone_name_vendor_excl'])) &
            (~ df_raw_data['zone_name_customer'].isin(test_config_lod[i]['zone_name_customer_excl'])) &
            ((df_raw_data['scheme_id'].isin(test_config_lod[i]['scheme_id_on'])) | (df_raw_data['scheme_id'].isin(test_config_lod[i]['scheme_id_off']))) &
            (df_raw_data['order_placed_at_local'].dt.date.between(test_config_lod[i]['test_start'], test_config_lod[i]['test_end']))
        ] # Filter out Non_TG orders because they will contain irrelevant price schemes

        # We will add a supplementary column to "df_temp" in the for loop below, so we need to create a function with the conditions
        on_off_conditions = [
            (df_temp['scheme_id'].isin(test_config_lod[i]['scheme_id_on'])),
            (df_temp['scheme_id'].isin(test_config_lod[i]['scheme_id_off']))
        ]

        # Add a supplementary column to indicate if the order belonged to an 'On' or 'Off' day
        df_temp['on_or_off_day'] = np.select(on_off_conditions, ['On', 'Off'])

        # Calculate the "per order" metrics and rename the column label to "df_per_order_metrics"
        df_per_order = round(df_temp.groupby(['test_name', 'on_or_off_day'])[col_list].mean(), 2)
        df_per_order = df_per_order.rename_axis(['df_per_order_metrics'], axis = 1)
        
        # Calculate the "total" metrics and rename the column label to "df_tot_metrics"
        df_tot = round(df_temp.groupby(['test_name', 'on_or_off_day'])[col_list[:-3]].sum(), 2) # [:-3] excludes the logistics KPIs
        df_tot = df_tot.rename_axis(['df_tot_metrics'], axis = 1)

        # Append "df_per_order" and "df_tot" for the for loop's iteration to the previously initialized variables "df_final_per_order" and "df_tot"
        df_final_per_order.append(df_per_order)
        df_final_tot.append(df_tot)

        # Now, we need to calculate the p-values. Create two sub-data frames for the 'On' and 'Off' days
        df_on_days = df_temp[df_temp['on_or_off_day'] == 'On']
        df_off_days = df_temp[df_temp['on_or_off_day'] == 'Off']

        pval_dict = {} # Initialize an empty dict that will contain the p-value of each KPI
        for i in col_list:
            pval = round(scipy.stats.mannwhitneyu(x = df_on_days[i], y = df_off_days[i], alternative = 'two-sided', nan_policy = 'omit')[1], 4)
            pval_dict[i] = pval
        
        df_final_pval.append(pval_dict) # df_final_pval is a list of dicts

    # Concatenate "df_final_per_order" from all tests into one data frame
    df_final_per_order = pd.concat(df_final_per_order)

    # Concatenate "df_final_tot" from all tests into one data frame
    df_final_tot = pd.concat(df_final_tot)
    # Add a thousand separator
    for i in df_final_tot.columns:
        df_final_tot[i] = df_final_tot[i].map('{:,}'.format)

    # Change the list of dicts "df_final_pval" to a data frame
    df_final_pval = pd.DataFrame(df_final_pval)\
        .assign(
            test_name = [iter['test_name'] for iter in test_config_lod],
            upload_timestamp = dt.datetime.now()
        )\
        .set_index('test_name')

    ###--------------------------------------------------------END OF STEP 5--------------------------------------------------------###

    ### Step 6: Upload the data frame containing the p-values to BQ
    # Since string columns use the "object" dtype, pass in a (partial) schema to ensure the correct BigQuery data type.
    job_config = bigquery.LoadJobConfig(schema = [
        bigquery.SchemaField('test_name', 'STRING'),
    ])

    # Set the job_config to overwrite the data in the table
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    # Upload the p-values dataframe to BQ 
    job = client.load_table_from_dataframe(
        dataframe = df_final_pval.reset_index(),
        destination = p_val_tbl_id,
        job_config = job_config
    )

    job.result() # Wait for the load job to complete