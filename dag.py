import os
import logging
import requests
import pandas as pd
import shutil
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable


#########################################################
#
#   DAG Settings
#
#########################################################

dag_default_args = {
    'owner': 'BDE_LAB_6',
    'start_date': datetime.now() - timedelta(days=2),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='airbnb_bronze_ingestion_new',
    default_args=dag_default_args,
    schedule_interval=None,  # No set schedule interval
    catchup=True,
    max_active_runs=1,
    concurrency=5
)

#########################################################
#
#   Load Environment Variables
#
#########################################################
AIRFLOW_DATA = "/home/airflow/gcs/data"
DATA = AIRFLOW_DATA + "/facts/"

#########################################################
#
#   DAG Task Setup
#
#########################################################

def load_census_data_g01(**kwargs):

 # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    Census_G01_file_path = DATA + '2016Census_G01_NSW_LGA.csv'
    if not os.path.exists(Census_G01_file_path):
        logging.info("No 2016Census_G01_NSW_LGA.csv file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(Census_G01_file_path)

    if len(df) > 0:
        col_names = ['LGA_CODE_2016', 'Tot_P_M', 'Tot_P_F', 'Tot_P_P', 'Age_0_4_yr_M', 
        'Age_0_4_yr_F', 'Age_0_4_yr_P', 'Age_5_14_yr_M', 'Age_5_14_yr_F', 
        'Age_5_14_yr_P', 'Age_15_19_yr_M', 'Age_15_19_yr_F', 'Age_15_19_yr_P', 
        'Age_20_24_yr_M', 'Age_20_24_yr_F', 'Age_20_24_yr_P', 'Age_25_34_yr_M', 
        'Age_25_34_yr_F', 'Age_25_34_yr_P', 'Age_35_44_yr_M', 'Age_35_44_yr_F', 
        'Age_35_44_yr_P', 'Age_45_54_yr_M', 'Age_45_54_yr_F', 'Age_45_54_yr_P', 
        'Age_55_64_yr_M', 'Age_55_64_yr_F', 'Age_55_64_yr_P', 'Age_65_74_yr_M', 
        'Age_65_74_yr_F', 'Age_65_74_yr_P', 'Age_75_84_yr_M', 'Age_75_84_yr_F', 
        'Age_75_84_yr_P', 'Age_85ov_M', 'Age_85ov_F', 'Age_85ov_P', 
        'Counted_Census_Night_home_M', 'Counted_Census_Night_home_F', 
        'Counted_Census_Night_home_P', 'Count_Census_Nt_Ewhere_Aust_M', 
        'Count_Census_Nt_Ewhere_Aust_F', 'Count_Census_Nt_Ewhere_Aust_P', 
        'Indigenous_psns_Aboriginal_M', 'Indigenous_psns_Aboriginal_F', 
        'Indigenous_psns_Aboriginal_P', 'Indig_psns_Torres_Strait_Is_M', 
        'Indig_psns_Torres_Strait_Is_F', 'Indig_psns_Torres_Strait_Is_P', 
        'Indig_Bth_Abor_Torres_St_Is_M', 'Indig_Bth_Abor_Torres_St_Is_F', 
        'Indig_Bth_Abor_Torres_St_Is_P', 'Indigenous_P_Tot_M', 
        'Indigenous_P_Tot_F', 'Indigenous_P_Tot_P', 'Birthplace_Australia_M', 
        'Birthplace_Australia_F', 'Birthplace_Australia_P', 'Birthplace_Elsewhere_M', 
        'Birthplace_Elsewhere_F', 'Birthplace_Elsewhere_P', 'Lang_spoken_home_Eng_only_M', 
        'Lang_spoken_home_Eng_only_F', 'Lang_spoken_home_Eng_only_P', 
        'Lang_spoken_home_Oth_Lang_M', 'Lang_spoken_home_Oth_Lang_F', 
        'Lang_spoken_home_Oth_Lang_P', 'Australian_citizen_M', 'Australian_citizen_F', 
        'Australian_citizen_P', 'Age_psns_att_educ_inst_0_4_M', 
        'Age_psns_att_educ_inst_0_4_F', 'Age_psns_att_educ_inst_0_4_P', 
        'Age_psns_att_educ_inst_5_14_M', 'Age_psns_att_educ_inst_5_14_F', 
        'Age_psns_att_educ_inst_5_14_P', 'Age_psns_att_edu_inst_15_19_M', 
        'Age_psns_att_edu_inst_15_19_F', 'Age_psns_att_edu_inst_15_19_P', 
        'Age_psns_att_edu_inst_20_24_M', 'Age_psns_att_edu_inst_20_24_F', 
        'Age_psns_att_edu_inst_20_24_P', 'Age_psns_att_edu_inst_25_ov_M', 
        'Age_psns_att_edu_inst_25_ov_F', 'Age_psns_att_edu_inst_25_ov_P', 
        'High_yr_schl_comp_Yr_12_eq_M', 'High_yr_schl_comp_Yr_12_eq_F', 
        'High_yr_schl_comp_Yr_12_eq_P', 'High_yr_schl_comp_Yr_11_eq_M', 
        'High_yr_schl_comp_Yr_11_eq_F', 'High_yr_schl_comp_Yr_11_eq_P', 
        'High_yr_schl_comp_Yr_10_eq_M', 'High_yr_schl_comp_Yr_10_eq_F', 
        'High_yr_schl_comp_Yr_10_eq_P', 'High_yr_schl_comp_Yr_9_eq_M', 
        'High_yr_schl_comp_Yr_9_eq_F', 'High_yr_schl_comp_Yr_9_eq_P', 
        'High_yr_schl_comp_Yr_8_belw_M', 'High_yr_schl_comp_Yr_8_belw_F', 
        'High_yr_schl_comp_Yr_8_belw_P', 'High_yr_schl_comp_D_n_g_sch_M', 
        'High_yr_schl_comp_D_n_g_sch_F', 'High_yr_schl_comp_D_n_g_sch_P', 
        'Count_psns_occ_priv_dwgs_M', 'Count_psns_occ_priv_dwgs_F', 
        'Count_psns_occ_priv_dwgs_P', 'Count_Persons_other_dwgs_M', 
        'Count_Persons_other_dwgs_F', 'Count_Persons_other_dwgs_P']
        values = df[col_names].to_dict('split')['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO bronze.census_data_g01_raw(lga_code_2016, tot_p_m, tot_p_f, tot_p_p, age_0_4_yr_m, age_0_4_yr_f, age_0_4_yr_p, \
                    age_5_14_yr_m, age_5_14_yr_f, age_5_14_yr_p, age_15_19_yr_m, age_15_19_yr_f, age_15_19_yr_p, \
                    age_20_24_yr_m, age_20_24_yr_f, age_20_24_yr_p, age_25_34_yr_m, age_25_34_yr_f, age_25_34_yr_p, \
                    age_35_44_yr_m, age_35_44_yr_f, age_35_44_yr_p, age_45_54_yr_m, age_45_54_yr_f, age_45_54_yr_p, \
                    age_55_64_yr_m, age_55_64_yr_f, age_55_64_yr_p, age_65_74_yr_m, age_65_74_yr_f, age_65_74_yr_p, \
                    age_75_84_yr_m, age_75_84_yr_f, age_75_84_yr_p, age_85ov_m, age_85ov_f, age_85ov_p, \
                    counted_census_night_home_m, counted_census_night_home_f, counted_census_night_home_p, \
                    count_census_nt_ewhere_aust_m, count_census_nt_ewhere_aust_f, count_census_nt_ewhere_aust_p, \
                    indigenous_psns_aboriginal_m, indigenous_psns_aboriginal_f, indigenous_psns_aboriginal_p, \
                    indig_psns_torres_strait_is_m, indig_psns_torres_strait_is_f, indig_psns_torres_strait_is_p, \
                    indig_bth_abor_torres_st_is_m, indig_bth_abor_torres_st_is_f, indig_bth_abor_torres_st_is_p, \
                    indigenous_p_tot_m, indigenous_p_tot_f, indigenous_p_tot_p, birthplace_australia_m, \
                    birthplace_australia_f, birthplace_australia_p, birthplace_elsewhere_m, birthplace_elsewhere_f, \
                    birthplace_elsewhere_p, lang_spoken_home_eng_only_m, lang_spoken_home_eng_only_f, \
                    lang_spoken_home_eng_only_p, lang_spoken_home_oth_lang_m, lang_spoken_home_oth_lang_f, \
                    lang_spoken_home_oth_lang_p, australian_citizen_m, australian_citizen_f, australian_citizen_p, \
                    age_psns_att_educ_inst_0_4_m, age_psns_att_educ_inst_0_4_f, age_psns_att_educ_inst_0_4_p, \
                    age_psns_att_educ_inst_5_14_m, age_psns_att_educ_inst_5_14_f, age_psns_att_educ_inst_5_14_p, \
                    age_psns_att_edu_inst_15_19_m, age_psns_att_edu_inst_15_19_f, age_psns_att_edu_inst_15_19_p, \
                    age_psns_att_edu_inst_20_24_m, age_psns_att_edu_inst_20_24_f, age_psns_att_edu_inst_20_24_p, \
                    age_psns_att_edu_inst_25_ov_m, age_psns_att_edu_inst_25_ov_f, age_psns_att_edu_inst_25_ov_p, \
                    high_yr_schl_comp_yr_12_eq_m, high_yr_schl_comp_yr_12_eq_f, high_yr_schl_comp_yr_12_eq_p, \
                    high_yr_schl_comp_yr_11_eq_m, high_yr_schl_comp_yr_11_eq_f, high_yr_schl_comp_yr_11_eq_p, \
                    high_yr_schl_comp_yr_10_eq_m, high_yr_schl_comp_yr_10_eq_f, high_yr_schl_comp_yr_10_eq_p, \
                    high_yr_schl_comp_yr_9_eq_m, high_yr_schl_comp_yr_9_eq_f, high_yr_schl_comp_yr_9_eq_p, \
                    high_yr_schl_comp_yr_8_belw_m, high_yr_schl_comp_yr_8_belw_f, high_yr_schl_comp_yr_8_belw_p, \
                    high_yr_schl_comp_d_n_g_sch_m, high_yr_schl_comp_d_n_g_sch_f, high_yr_schl_comp_d_n_g_sch_p, \
                    count_psns_occ_priv_dwgs_m, count_psns_occ_priv_dwgs_f, count_psns_occ_priv_dwgs_p, \
                    count_persons_other_dwgs_m, count_persons_other_dwgs_f, count_persons_other_dwgs_p
                    )
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

        # Move the processed file to the archive folder
        archive_folder = os.path.join(DATA, 'archive')
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)
        shutil.move(Census_G01_file_path, os.path.join(archive_folder, '2016Census_G01_NSW_LGA.csv'))
    return None




def load_census_data_g02(**kwargs):

        # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    Census_G02_file_path = DATA + '2016Census_G02_NSW_LGA.csv'
    if not os.path.exists(Census_G02_file_path):
        logging.info("No 2016Census_G02_NSW_LGA.csv file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(Census_G02_file_path)

    if len(df) > 0:
        col_names = ['LGA_CODE_2016', 
        'Median_age_persons', 
        'Median_mortgage_repay_monthly', 
        'Median_tot_prsnl_inc_weekly', 
        'Median_rent_weekly', 
        'Median_tot_fam_inc_weekly', 
        'Average_num_psns_per_bedroom', 
        'Median_tot_hhd_inc_weekly', 
        'Average_household_size']
        values = df[col_names].to_dict('split')['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO bronze.census_data_g02_raw(lga_code_2016, median_age_persons, median_mortgage_repay_monthly, \
                    median_tot_prsnl_inc_weekly, median_rent_weekly, median_tot_fam_inc_weekly, \
                    average_num_psns_per_bedroom, median_tot_hhd_inc_weekly, average_household_size)
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

        # Move the processed file to the archive folder
        archive_folder = os.path.join(DATA, 'archive')
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)
        shutil.move(Census_G02_file_path, os.path.join(archive_folder, '2016Census_G02_NSW_LGA.csv'))
    return None


def load_nsw_lga_code(**kwargs):
        # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    NSW_LGA_CODE_file_path = DATA + 'NSW_LGA_CODE.csv'
    if not os.path.exists(NSW_LGA_CODE_file_path):
        logging.info("No NSW_LGA_CODE.csv file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(NSW_LGA_CODE_file_path)

    if len(df) > 0:
        col_names = ['LGA_CODE', 'LGA_NAME']
        values = df[col_names].to_dict('split')['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO bronze.nsw_lga_code_raw(lga_code, lga_name)
                    VALUES %s
                    """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

        # Move the processed file to the archive folder
        archive_folder = os.path.join(DATA, 'archive')
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)
        shutil.move(NSW_LGA_CODE_file_path, os.path.join(archive_folder, 'NSW_LGA_CODE.csv'))
    return None

                      
def load_nsw_lga_suburb(**kwargs):
    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Check if the file exists
    NSW_LGA_SUBURB_file_path = DATA + 'NSW_LGA_SUBURB.csv'
    if not os.path.exists(NSW_LGA_SUBURB_file_path):
        logging.info("No NSW_LGA_SUBURB.csv file found.")
        return None

    # Generate dataframe by reading the CSV file
    df = pd.read_csv(NSW_LGA_SUBURB_file_path)

    if len(df) > 0:
        col_names = ['LGA_NAME', 'SUBURB_NAME']

        # Use the updated column names to extract the data from the dataframe
        values = df[col_names].to_dict('split')['data']
        logging.info(values)

        # SQL insert statement, assuming the database column names are lowercase
        insert_sql = """
            INSERT INTO bronze.nsw_lga_suburb_raw(lga_name, suburb_name)
            VALUES %s
        """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

        # Move the processed file to the archive folder
        archive_folder = os.path.join(DATA, 'archive')
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)
        shutil.move(NSW_LGA_SUBURB_file_path, os.path.join(archive_folder, 'NSW_LGA_SUBURB.csv'))
    return None


def load_raw_facts(**kwargs):
    # Setup Postgres connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Get all files with '.csv' extension in the data directory
    filelist = [k for k in os.listdir(DATA) if k.endswith('.csv')]

    # Check if there are any files to process
    if len(filelist) == 0:
        logging.info("No CSV files found in the data directory.")
        return None  # Exit gracefully if no files are found

    # Generate dataframe by combining all files
    df = pd.concat([pd.read_csv(os.path.join(DATA, fname)) for fname in filelist], ignore_index=True)

    if len(df) > 0:
        col_names = ['LISTING_ID', 'SCRAPE_ID', 'SCRAPED_DATE', 'HOST_ID', 'HOST_NAME', 'HOST_SINCE',
                     'HOST_IS_SUPERHOST', 'HOST_NEIGHBOURHOOD', 'LISTING_NEIGHBOURHOOD', 'PROPERTY_TYPE',
                     'ROOM_TYPE', 'ACCOMMODATES', 'PRICE', 'HAS_AVAILABILITY', 'AVAILABILITY_30',
                     'NUMBER_OF_REVIEWS', 'REVIEW_SCORES_RATING', 'REVIEW_SCORES_ACCURACY',
                     'REVIEW_SCORES_CLEANLINESS', 'REVIEW_SCORES_CHECKIN', 'REVIEW_SCORES_COMMUNICATION',
                     'REVIEW_SCORES_VALUE']

        # Use the updated column names to extract the data from the dataframe
        values = df[col_names].to_dict('split')['data']
        logging.info(values)

        # SQL insert statement matching the new column names
        insert_sql = """
            INSERT INTO bronze.raw_facts(listing_id, scrape_id, scraped_date, host_id, host_name, host_since, 
            host_is_superhost, host_neighbourhood, listing_neighbourhood, property_type, room_type, 
            accommodates, price, has_availability, availability_30, number_of_reviews, review_scores_rating, 
            review_scores_accuracy, review_scores_cleanliness, review_scores_checkin, review_scores_communication, 
            review_scores_value)
            VALUES %s
        """
        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()

        # Move processed files to the archive folder
        archive_folder = os.path.join(DATA, 'archive')
        if not os.path.exists(archive_folder):
            os.makedirs(archive_folder)

        # Move each processed file to the archive folder
        for fname in filelist:
            shutil.move(os.path.join(DATA, fname), os.path.join(archive_folder, fname))
    return None


#########################################################
#
#   Function to trigger dbt Cloud Job
#
#########################################################

def trigger_dbt_cloud_job(**kwargs):
    # Get the dbt Cloud URL, account ID, and job ID from Airflow Variables
    dbt_cloud_url = Variable.get("DBT_CLOUD_URL")
    dbt_cloud_account_id = Variable.get("DBT_CLOUD_ACCOUNT_ID")
    dbt_cloud_job_id = Variable.get("DBT_CLOUD_JOB_ID")
    
    # Define the URL for the dbt Cloud job API dynamically using URL, account ID, and job ID
    url = f"https://{dbt_cloud_url}/api/v2/accounts/{dbt_cloud_account_id}/jobs/{dbt_cloud_job_id}/run/"
    
    # Get the dbt Cloud API token from Airflow Variables
    dbt_cloud_token = Variable.get("DBT_CLOUD_API_TOKEN")
    
    # Define the headers and body for the request
    headers = {
        'Authorization': f'Token {dbt_cloud_token}',
        'Content-Type': 'application/json'
    }
    data = {
        "cause": "Triggered via API"
    }
    
    # Make the POST request to trigger the dbt Cloud job
    response = requests.post(url, headers=headers, json=data)
    
    # Check if the response is successful
    if response.status_code == 200:
        logging.info("Successfully triggered dbt Cloud job.")
        return response.json()
    else:
        logging.error(f"Failed to trigger dbt Cloud job: {response.status_code}, {response.text}")
        raise AirflowException("Failed to trigger dbt Cloud job.")




#########################################################
#
#    DAG Operator Setup
#
#########################################################

load_census_g01_task = PythonOperator(
    task_id='load_census_g01',
    python_callable=load_census_data_g01,
    provide_context=True,
    dag=dag
)

load_census_g02_task = PythonOperator(
    task_id='load_census_g02',
    python_callable=load_census_data_g02,
    provide_context=True,
    dag=dag
)

load_nsw_lga_code_task = PythonOperator(
    task_id='load_nsw_lga_code',
    python_callable=load_nsw_lga_code,
    provide_context=True,
    dag=dag
)

load_nsw_lga_suburb_task = PythonOperator(
    task_id='load_nsw_lga_suburb',
    python_callable=load_nsw_lga_suburb,
    provide_context=True,
    dag=dag
)

load_raw_facts_task = PythonOperator(
    task_id='load_raw_facts',
    python_callable=load_raw_facts,
    provide_context=True,
    dag=dag
)

trigger_dbt_job_task = PythonOperator(
    task_id='trigger_dbt_job',
    python_callable=trigger_dbt_cloud_job,
    provide_context=True,
    dag=dag
)


# Task Dependencies
[load_census_g01_task, load_census_g02_task, load_nsw_lga_code_task, load_nsw_lga_suburb_task] >> load_raw_facts_task >> trigger_dbt_job_task

