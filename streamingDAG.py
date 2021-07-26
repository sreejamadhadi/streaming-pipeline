import os
import csv
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


def retrieve_data():
    conn = GCSHook(gcp_conn_id='my_gcp_conn')
    bucket = 'streaming_data_bucket'
    records_list = conn.list(bucket_name=bucket)
    with open('merged.csv', 'w') as file:
        writer = csv.writer(file)
        for record in records_list:
            raw_data = conn.download(bucket_name=bucket, object_name=record)
            data = raw_data.decode('utf-8')
            data_list = data.split(',')
            writer.writerow(data_list)
            conn.copy(source_bucket=bucket, source_object=record, destination_bucket='streamed_data_bucket')
            conn.delete(bucket_name=bucket, object_name=record)


def delete_file():
    os.remove('merged.csv')


with DAG("streaming_dag", start_date=days_ago(2), schedule_interval='0 * * * *', catchup=False) as dag:
    retrieve_task = PythonOperator(
        task_id='retrieve_task',
        python_callable=retrieve_data
    )

    upload_data_task = LocalFilesystemToGCSOperator(
        task_id='upload_data_task',
        gcp_conn_id='my_gcp_conn',
        src='merged.csv',
        dst='merged.csv',
        bucket='streamed_data_bucket'
    )

    remove_streamed_data_task = PythonOperator(
        task_id='remove_local_task',
        python_callable=delete_file
    )

    insert_to_bigquery_task = GCSToBigQueryOperator(
        task_id='insert_to_bigquery_task',
        google_cloud_storage_conn_id='my_gcp_conn',
        bigquery_conn_id='my_gcp_conn',
        bucket='streamed_data_bucket',
        source_objects=['merged.csv'],
        destination_project_dataset_table='streaming_data.twitter',
        schema_fields=[
            {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'created_date', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'tweet', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'source', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'reply_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'retweet_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'favorite_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'user_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'user_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'user_location', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'user_followers_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'user_friends_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'user_listed_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'user_favourites_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'user_statuses_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'profile_created_at', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        write_disposition='WRITE_APPEND'
    )

    retrieve_task >> upload_data_task >> remove_streamed_data_task >> insert_to_bigquery_task
