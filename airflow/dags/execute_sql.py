from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from sqlalchemy import create_engine, text
import pendulum

# PostgreSQL Connection
PG_HOST = "host.docker.internal"
PG_PORT = "15435"
PG_DB = "datamart"
PG_USER = "postgres"
PG_PASSWORD = "admin"

SQL_CREATION_PATH = "/opt/airflow/dags/creation.sql"
SQL_INSERTION_PATH = "/opt/airflow/dags/insertion.sql"

def execute_sql_script(script_path, table_name):
    # Lecture du contenu SQL avec remplacement
    with open(script_path, "r") as file:
        sql = file.read().format(table_name=table_name)

    # Connexion à PostgreSQL
    engine = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )

    with engine.connect() as conn:
        conn.execute(text(sql))
        print(f"Script {script_path} exécuté avec succès.")

def run_creation(**kwargs):
    month = pendulum.now().subtract(months=3).format("YYYY_MM")
    table_name = f"yellow_tripdata_{month}"
    execute_sql_script(SQL_CREATION_PATH, table_name)

def run_insertion(**kwargs):
    month = pendulum.now().subtract(months=3).format("YYYY_MM")
    table_name = f"yellow_tripdata_{month}"
    execute_sql_script(SQL_INSERTION_PATH, table_name)

# Définir le DAG
with DAG(
    dag_id="execute_sql_scripts",
    start_date=days_ago(1),
    schedule_interval="@once",
    catchup=False,
    tags=["sql", "postgres"],
) as dag:

    creation_task = PythonOperator(
        task_id="run_creation_script",
        python_callable=run_creation,
        provide_context=True
    )

    insertion_task = PythonOperator(
        task_id="run_insertion_script",
        python_callable=run_insertion,
        provide_context=True
    )

    creation_task >> insertion_task
