from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='geo_analysis_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    events_base_path = "/user/master/data/geo/events"
    cities_base_path = "/user/malina692/data/geo/geo.csv"
    recommendations_path = "/user/malina692/geo/recommendations"
    geo_layer_path = "/user/malina692/geo/geo_layer"
    date = "2022-05-30"
    depth = "1"
    spark_script_path = "/lessons/geo_analysis.py"


    run_geo_analysis = SparkSubmitOperator(
        task_id='run_geo_analysis',
        application=spark_script_path,
        conn_id='spark_default',
        application_args=[
            date,
            depth,
            events_base_path,
            cities_base_path,
            recommendations_path,
            geo_layer_path,
        ],
        dag=dag,
    )
