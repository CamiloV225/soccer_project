from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from datetime import datetime
#from etl import read_csv, read_db, transform_csv, transform_db, merge, load, store
from soccer_etl import read_csv1, read_csv2, read_api, transform_csv1, transform_api, merge,predicting_value, load, kafka

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 10),  # Update the start date to today or an appropriate date
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def func1():
    print(f"the date is: {datetime.now()}")

with DAG(
    'Soccer_Api_Dag',
    default_args=default_args,
    description='Final Project',
    schedule_interval='@daily',  # Set the schedule interval as per your requirements
) as dag:

    merge = PythonOperator(
        task_id='merge',
        python_callable=merge,
        provide_context = True,
        )

    read_csv1 = PythonOperator(
        task_id='read_csv1',
        python_callable=read_csv1,
        provide_context = True,
        )
    
    read_csv2 = PythonOperator(
        task_id='read_csv2',
        python_callable=read_csv2,
        provide_context = True,
        )
    
    read_api = PythonOperator(
        task_id='read_api',
        python_callable=read_api,
        provide_context = True,
        )

    transform_csv1 = PythonOperator(
        task_id='transform_csv1',
        python_callable=transform_csv1,
        provide_context = True,
        )


    transform_api = PythonOperator(
        task_id='transform_api',
        python_callable=transform_api,
        provide_context = True,
        )

    predicting_value = PythonOperator(
         task_id='predicting_value',
         python_callable=predicting_value,
         provide_context = True,
         )

    load = PythonOperator(
        task_id='load',
        python_callable=load,
        provide_context = True,
        )
    
    kafka = PythonOperator(
        task_id='kafka',
        python_callable=kafka,
        provide_context = True,
        )
    #>> load >> store

    read_csv1 >> transform_csv1 >> merge >> predicting_value >> load >> kafka
    read_api >> transform_api >> merge
    read_csv2 >> transform_api