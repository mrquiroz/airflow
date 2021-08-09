from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils import dates
from google.cloud import bigquery
from pprint import pprint
import datetime
import random
from google.cloud import storage


nameDAG = 'DAG-MAIN-MOISES'
project = 'sp-te-frameworkventas-dev'
GBQ_CONNECTION_ID = 'bigquery_default'
owner = 'MOISES'
nameDAG_base = 'DAG-dinamico-val-input-output'


lineas_costo = ['FOP', 'GDS']

comando_base = """
gcloud composer environments run fwk-vts-scheduler --location us-east1 trigger_dag -- DAG-dinamico-val-input-output-{nombre_dag}

"""

bash_command = '''
{{ ti.xcom_pull(task_ids="generador_bash") }}
'''


cola_ejecucion = []

default_args = {
    # Task instance should not rely on the previous task's schedule to succeed.
    'depends_on_past': False,
    # 'start_date': datetime.datetime.utcnow(),
    # 7 days ago # OJO: COMO ES EXTERNALLY TRIGGERED, EL START DATE DEBE SER VENCIDO!!!!
    'start_date': dates.days_ago(7),
    'owner': owner,
    'retries': 1,  # Retry once before failing the task.
    'retry_delay': datetime.timedelta(minutes=5),  # Time between retries
    'project_id': project,  # Cloud Composer project ID.
}


def create_comando(ds, **kwargs):
    comando_tmp = kwargs['comando_base']
    comando_tmp = ''
    for linea in kwargs['lineas_costo']:
        comando_tmp += kwargs['comando_base'].format(nombre_dag=linea)
    return comando_tmp


with DAG(nameDAG,
         default_args=default_args,
         description='DAG de forecast XGB con cross-validation',
         schedule_interval="30 14 * * 1",  # 10:30 los lunes
         catchup=False
         ) as dag:  # OJO: schedule_interval=None -> We only want the DAG to run when we POST to the api
    #############################################################
    t_begin = DummyOperator(task_id="begin")

    task_data_init = PythonOperator(task_id='generador_bash',
                                    provide_context=True,
                                    python_callable=create_comando,
                                    op_kwargs={
                                            'comando_base': comando_base,
                                            'lineas_costo': lineas_costo,
                                    }
                                    )

    task_python_bash = BashOperator(task_id='task_python_bash',
                                    bash_command=bash_command
                                    )

    t_end = DummyOperator(task_id="end")
    #############################################################
    t_begin >> task_data_init >> task_python_bash >> t_end
