from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates
from google.cloud import bigquery
from pprint import pprint
import datetime
import random
from google.cloud import storage

nameDAG = 'DAG-GENERADOR-MOISES'
project = 'sp-te-frameworkventas-dev'
GBQ_CONNECTION_ID = 'bigquery_default'


def create_dag_file(ds, **kwargs):
    print(kwargs)
    client_storage = storage.Client()  # Conexion a storage
    client_gbq = bigquery.client.Client()  # Conexion a BQ

    bucket = client_storage.get_bucket(kwargs['bucket_name'])
    blob_base = bucket.get_blob(kwargs['archivo_base'])
    python_code = blob_base.download_as_string().decode("utf-8")
    numero_random = random.randint(1, 10)
    print('el numero random', numero_random)
    python_code = python_code.replace(
        "lista_fechas", f'listas_fechas_{numero_random}')
    blob_destino = bucket.blob(kwargs['archivo_destino'])
    blob_destino.upload_from_string(python_code)

    return True


default_args = {
    # Task instance should not rely on the previous task's schedule to succeed.
    'depends_on_past': False,
    # 'start_date': datetime.datetime.utcnow(),
    # 7 days ago # OJO: COMO ES EXTERNALLY TRIGGERED, EL START DATE DEBE SER VENCIDO!!!!
    'start_date': dates.days_ago(7),
    'retries': 1,  # Retry once before failing the task.
    'retry_delay': datetime.timedelta(minutes=5),  # Time between retries
    'project_id': project,  # Cloud Composer project ID.
}

with DAG(nameDAG, default_args=default_args, schedule_interval=None) as dag:
    t_begin = DummyOperator(task_id="begin")
    #######################################################################################
    # Aqui debe ir lo que tienes que hacer con el mensaje
    task_python = PythonOperator(task_id='task_python',
                                 provide_context=True,
                                 python_callable=create_dag_file,
                                 op_kwargs={'bucket_name': 'us-east1-fwk-vts-scheduler-d904e1ed-bucket',
                                            'archivo_base': 'dags/moises/dag_prueba.py',
                                            'archivo_destino': 'dags/dag_prueba_2.py'})

    #######################################################################################
    t_end = DummyOperator(task_id="end")

    t_begin >> task_python >> t_end
