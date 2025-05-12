from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago

from minio import Minio
from minio.error import S3Error

import pendulum
import pandas as pd
from sqlalchemy import create_engine
import os

# Connexion MinIO
MINIO_CLIENT = Minio(
    "minio:9000",
    access_key="vPNJQMl0tM2TfedU582C",
    secret_key="V2njsRt5phGd0sSpjwzWTT87Nz36KgmSrEiHVXLC",
    secure=False
)
MINIO_BUCKET = "yellow-taxi"

# Connexion PostgreSQL
PG_HOST = "host.docker.internal"
PG_PORT = "15432"
PG_DB = "datawarehouse"
PG_USER = "postgres"
PG_PASSWORD = "admin"

def download_from_minio(**kwargs):
    month = pendulum.now().subtract(months=3).format("YYYY-MM")
    file_name = f"yellow_tripdata_{month}.parquet"

    print(f"Téléchargement de {file_name} depuis MinIO...")
    try:
        MINIO_CLIENT.fget_object(
            bucket_name=MINIO_BUCKET,
            object_name=file_name,
            file_path=file_name
        )
        print(f"Fichier {file_name} téléchargé avec succès.")
    except S3Error as e:
        raise RuntimeError(f"Erreur lors du téléchargement depuis MinIO : {e}")

def load_parquet_to_postgres(**kwargs):
    import pyarrow.parquet as pq
    import pyarrow as pa

    month = pendulum.now().subtract(months=3).format("YYYY-MM")
    file_name = f"yellow_tripdata_{month}.parquet"

    if not os.path.exists(file_name):
        raise FileNotFoundError(f"Fichier {file_name} introuvable !")

    print(f"Lecture de {file_name} en streaming...")

    # Connexion à la base
    db_url = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    engine = create_engine(db_url)
    table_name = f"yellow_tripdata_{month.replace('-', '_')}"

    parquet_file = pq.ParquetFile(file_name)

    for i, batch in enumerate(parquet_file.iter_batches(batch_size=50000)):
        df = pa.Table.from_batches([batch]).to_pandas()

        df.to_sql(table_name, engine, if_exists="append" if i > 0 else "replace", index=False)
        print(f"Batch {i} inséré")

    os.remove(file_name)
    print(f"Fichier local {file_name} supprimé.")

# Définir le DAG
with DAG(
    dag_id="load_minio_parquet_to_postgres",
    start_date=days_ago(1),
    schedule_interval="@once",
    catchup=False,
    tags=["minio", "postgres", "etl"],
) as dag:

    download_task = PythonOperator(
        task_id="download_from_minio",
        python_callable=download_from_minio,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id="load_parquet_to_pg",
        python_callable=load_parquet_to_postgres,
        provide_context=True,
    )
    trigger_sql = TriggerDagRunOperator(
        task_id="trigger_execute_sql_scripts",
        trigger_dag_id="execute_sql_scripts",
        wait_for_completion=True
    )
    download_task >> load_task
    load_task >> trigger_sql