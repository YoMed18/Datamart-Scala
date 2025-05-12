# Import Python dependencies needed for the workflow
from urllib import request
from minio import Minio, S3Error
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pendulum
import os
import urllib.error


def download_parquet(**kwargs):
    # folder_path: str = r'..\..\data\raw'
    # Construct the relative path to the folder
    url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    filename: str = "yellow_tripdata"
    extension: str = ".parquet"

    month: str = pendulum.now().subtract(months=2).format('YYYY-MM')
    file_name = f"{filename}_{month}{extension}"
    full_url = url + file_name
    try:
        print(f"Downloading from {full_url}")
        request.urlretrieve(full_url, file_name)
        print(f"Downloaded file saved as {file_name}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"Failed to download the parquet file : {str(e)}") from e


# Python Function
def upload_file(**kwargs):
    ###############################################
    # Upload generated file to Minio

    client = Minio(
        "minio:9000",
        secure=False,
        access_key="vPNJQMl0tM2TfedU582C",
        secret_key="V2njsRt5phGd0sSpjwzWTT87Nz36KgmSrEiHVXLC"
    )
    bucket: str = 'yellow-taxi'

    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
        print(f"Created bucket: {bucket}")
    else:
        print(f"Bucket {bucket} already exists.")

    month: str = pendulum.now().subtract(months=2).format('YYYY-MM')
    file_name = f"yellow_tripdata_{month}.parquet"
    print(file_name)
    try:
        client.fput_object(
            bucket_name=bucket,
            object_name=file_name,
            file_path=file_name
        )
        print(f"Uploaded {file_name} to bucket {bucket}.")
    except S3Error as err:
        raise RuntimeError(f"Failed to upload file to MinIO: {err}") from err
    finally:
        if os.path.exists(file_name):
            os.remove(file_name)
            print(f"Deleted local file {file_name} after upload.")

    # On supprime le fichié récement téléchargés, pour éviter la redondance. On suppose qu'en arrivant ici, l'ajout est
    # bien réalisé
    # os.remove(os.path.join("./", "yellow_tripdata_" + month + ".parquet"))


###############################################
with DAG(dag_id='Grab NYC Data to Minio',
         start_date=days_ago(1),
         schedule_interval="@once",
         catchup=False,
         tags=['minio/read/write'],
         ) as dag:
    ###############################################
    # Create a task to call your processing function
    t1 = PythonOperator(
        task_id='download_parquet',
        provide_context=True,
        python_callable=download_parquet
    )
    t2 = PythonOperator(
        task_id='upload_file_task',
        provide_context=True,
        python_callable=upload_file
    )
###############################################

###############################################
# first upload the file, then read the other file.
t1 >> t2
###############################################
