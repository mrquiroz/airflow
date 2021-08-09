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
owner = 'MOISES'
nameDAG_base = 'DAG-dinamico-val-input-output'

config_data = [
    {
        "tabla_inicial": "bc-co-giperf-dev-bt6q.GI_ABCOSTING_DEV_VENTA.GDS_001_input",
        "nombre_linea": "GDS",
        "year_month_name":    "issuing_year_month",
        "year_month": '2021-06-01',
        "cost_name": 'cost_seg_usd'
    },
    {
        "tabla_inicial": "bc-co-giperf-dev-bt6q.GI_FOP_VENTA_TABLAS.FOP_VENTA_001_input",
        "nombre_linea": "FOP",
        "year_month_name":    "issuing_year_month",
        "year_month": '2021-06-01',
        "cost_name": 'cost_seg_usd'
    }
]

query_init = """SELECT sum({0}) as suma FROM `{1}` where 
{2} = '{3}'"""

query_fin = """SELECT sum(amount_od_usd) as suma FROM `bc-co-giperf-dev-bt6q.GI_ABCOSTING.all_costs` where type ='{0}'
and year_month = '{1}'"""


def read_initial_costo(ds, **kwargs):
    client_gbq = bigquery.client.Client()  # Conexion a BQ
    query = kwargs['query']
    query_proc = query.format(
        kwargs['cost_name'], kwargs['tabla_inicial'], kwargs['year_month_name'], kwargs['year_month'])
    client_gbq = bigquery.client.Client()
    df = client_gbq.query(query_proc, location='US').result().to_dataframe()
    print(df)
    suma = df.loc[0, 'suma']
    return str(suma)


def read_final_costo(ds, **kwargs):
    client_gbq = bigquery.client.Client()  # Conexion a BQ
    query = kwargs['query']
    query_proc = query.format(
        kwargs['nombre_linea'], kwargs['year_month'])
    client_gbq = bigquery.client.Client()
    df = client_gbq.query(query_proc, location='US').result().to_dataframe()
    print(df)
    suma = df.loc[0, 'suma']
    return str(suma)


def calculate_diff(ds, **kwargs):
    ti = kwargs['ti']
    print(ti)
    pulled_value_1 = float(ti.xcom_pull(key=None, task_ids='task_init'))
    pulled_value_2 = float(ti.xcom_pull(key=None, task_ids='task_final'))

    diff = str(round(abs(pulled_value_1-pulled_value_2) /
                     pulled_value_2, 2)*100)+'%'
    print('La diferencia es de ', diff)
    return diff


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

for dag_data in config_data:

    client_dag_id = nameDAG_base + "-" + dag_data["nombre_linea"]

    with DAG(dag_id=client_dag_id,
             default_args=default_args,
             catchup=False,  # Ver caso catchup = True
             max_active_runs=3,
             schedule_interval=None) as dag:
        #############################################################

        t_begin = DummyOperator(task_id="begin")

        task_data_init = PythonOperator(task_id='task_init',
                                        provide_context=True,
                                        python_callable=read_initial_costo,
                                        op_kwargs={
                                            'cost_name': dag_data["cost_name"],
                                            'query': query_init,
                                            'tabla_inicial': dag_data["tabla_inicial"],
                                            'year_month_name': dag_data["year_month_name"],
                                            'year_month': dag_data["year_month"],
                                        }
                                        )
        task_data_fin = PythonOperator(task_id='task_final',
                                       provide_context=True,
                                       python_callable=read_final_costo,
                                       op_kwargs={
                                           'cost_name': dag_data["cost_name"],
                                           'query': query_fin,
                                           'nombre_linea': dag_data["nombre_linea"],
                                           'year_month_name': dag_data["year_month_name"],
                                           'year_month': dag_data["year_month"],
                                       }
                                       )

        task_data_diff = PythonOperator(task_id='task_diff',
                                        provide_context=True,
                                        python_callable=calculate_diff,
                                        op_kwargs={
                                            'cost_name': dag_data["cost_name"],
                                            'query': query_fin,
                                            'nombre_linea': dag_data["nombre_linea"],
                                            'year_month_name': dag_data["year_month_name"],
                                            'year_month': dag_data["year_month"],
                                        }
                                        )

        t_end = DummyOperator(task_id="end")

        #############################################################
        t_begin >> task_data_init >> task_data_fin >> task_data_diff >> t_end

        # ULTIMA LÍNEA IMPORTANTE para correcto funcionamiento de DAG DINÁMICO!!!
        globals()[client_dag_id] = dag
