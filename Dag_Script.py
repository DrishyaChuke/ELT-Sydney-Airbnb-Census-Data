import pandas as pd
import gcsfs
from datetime import datetime, timedelta
import requests
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.exceptions import AirflowException


#############################
#
#   PART 1
#
#############################

dag_default_args = {
    'owner': 'drishya_chuke',
    'start_date': datetime.now() - timedelta(days=2+4),
    'email': ['drishyachuke98@gmail.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

with DAG(
    dag_id='dag_at3',
    default_args=dag_default_args,
    schedule_interval=None,
    start_date=datetime.now() - timedelta(days=2),
    catchup=False,
) as dag:

    # Function to load CSV data from GCS to Postgres
    def load_csv(gcs_file_path, table_name, schema='bronze'):
        # Create a GCSFileSystem object
        fs = gcsfs.GCSFileSystem(project='meta-method-440103-q0')
        
        # Read the CSV file directly from GCS
        with fs.open(gcs_file_path) as file:
            df = pd.read_csv(file)
            
        df.columns = df.columns.str.lower()

        date_col = ['scraped_date', 'host_since']
        
        for col in date_col:
            if col in df.columns:
                # Convert to datetime, setting dayfirst=True for day-first format, and coerce errors
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')

        # Load data into Postgres
        pg_hook = PostgresHook(postgres_conn_id='postgres')
        engine = pg_hook.get_sqlalchemy_engine()
        df.to_sql(table_name, engine, schema=schema, if_exists='append', index=False)

    # Task 1: Load Airbnb data to Postgres
    def for_airbnb():
        load_csv('gs://australia-southeast1-bde-d78fa09f-bucket/data/listings/05_2020.csv', 'airbnb_listings')

    process_airbnb = PythonOperator(
        task_id='loading_airbnb_data',
        python_callable=for_airbnb,
    )

    # Task 2: Load Census G01 data to Postgres
    def for_census_g01():
        load_csv('gs://australia-southeast1-bde-d78fa09f-bucket/data/Census LGA/2016Census_G01_NSW_LGA.csv', 'census_g01')

    process_census_g01 = PythonOperator(
        task_id='load_census_g01_data',
        python_callable=for_census_g01,
    )

    # Task 3: Load Census G02 to Postgres
    def for_census_g02():
        load_csv('gs://australia-southeast1-bde-d78fa09f-bucket/data/Census LGA/2016Census_G02_NSW_LGA.csv', 'census_g02')

    process_census_g02 = PythonOperator(
        task_id='load_census_g02_data',
        python_callable=for_census_g02,
    )

    # Task 4: # Load NSW LGA Suburb data, keeping only LGA_NAME and SUBURB_NAME columns
    def for_lga_suburb():
        fs = gcsfs.GCSFileSystem(project='meta-method-440103-q0')
        with fs.open('gs://australia-southeast1-bde-d78fa09f-bucket/data/NSW_LGA/NSW_LGA_SUBURB.csv') as file:
            df = pd.read_csv(file, usecols=['LGA_NAME', 'SUBURB_NAME'])
        
        df.columns = df.columns.str.lower()
        
        pg_hook = PostgresHook(postgres_conn_id='postgres')
        engine = pg_hook.get_sqlalchemy_engine()
        df.to_sql('lga_suburb', engine, schema='bronze', if_exists='append', index=False)

    process_lga_suburb = PythonOperator(
        task_id='load_lga_suburb_data',
        python_callable=for_lga_suburb,
    )

    # Task 5: Load NSW LGA Code data to Postgres
    def for_lga_code():
        load_csv('gs://australia-southeast1-bde-d78fa09f-bucket/data/NSW_LGA/NSW_LGA_CODE.csv', 'lga_codes')

    process_lga_code = PythonOperator(
        task_id='load_lga_code_data',
        python_callable=for_lga_code,
    )

    # Task dependencies for loading initial data
    process_airbnb >> process_census_g01 >> process_census_g02 >> process_lga_suburb >> process_lga_code

#############################
#
#   PART 3
#
#############################
    
    def trigger_dbt_cloud_job(**kwargs):
        dbt_cloud_url = Variable.get("DBT_CLOUD_URL")
        dbt_cloud_account_id = Variable.get("DBT_CLOUD_ACCOUNT_ID")
        dbt_cloud_job_id = Variable.get("DBT_CLOUD_JOB_ID")

        # Define the URL for the dbt Cloud job API dynamically
        url = f"https://{dbt_cloud_url}/api/v2/accounts/{dbt_cloud_account_id}/jobs/{dbt_cloud_job_id}/run/"
        dbt_cloud_token = Variable.get("DBT_CLOUD_API_TOKEN")

        headers = {
            'Authorization': f'Token {dbt_cloud_token}',
            'Content-Type': 'application/json'
        }
        data = {
            "cause": "Triggered via API"
        }

        response = requests.post(url, headers=headers, json=data)

        if response.status_code == 200:
            logging.info("Successfully triggered dbt Cloud job.")
            return response.json()
        else:
            logging.error(f"Failed to trigger dbt Cloud job: {response.status_code}, {response.text}")
            raise AirflowException("Failed to trigger dbt Cloud job.")

    # Task 6: Trigger dbt Cloud job
    trigger_dbt_job = PythonOperator(
        task_id='trigger_job_dbtCloud',
        python_callable=trigger_dbt_cloud_job,
    )

    # Set the task dependencies including the dbt job trigger
    process_lga_code >> trigger_dbt_job


    def load_remaining_airbnb_data():
        months = ['05_2020', '06_2020', '07_2020', '08_2020', '09_2020', '10_2020', '11_2020', '12_2020',
                  '01_2021', '02_2021', '03_2021', '04_2021']

        for month in months:
            gcs_file_path = f'gs://australia-southeast1-bde-d78fa09f-bucket/data/listings/{month}.csv'
            load_csv(gcs_file_path, 'airbnb')

    # Task 7: Load remaining Airbnb data month by month
    load_remaining_airbnb_data_task = PythonOperator(
        task_id='load_all_airbnb_data',
        python_callable=load_remaining_airbnb_data,
    )

    # Set the dependency for loading remaining Airbnb data
    trigger_dbt_job >> load_remaining_airbnb_data_task