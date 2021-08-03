from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates
from google.cloud import bigquery
from pprint import pprint
import datetime
import json
import pandas as pd
import numpy as np


nameDAG = 'DAG-BQ-test'
project = 'sp-te-frameworkventas-dev'
GBQ_CONNECTION_ID = 'bigquery_default'


def get_data_df(ds, **kwargs):
    ti = kwargs['ti']
    query = kwargs['query']
    print(query)
    print(kwargs)
    query_proc = query.format(
        kwargs['proyecto'], kwargs['dataset'], kwargs['table'])
    client_gbq = bigquery.client.Client()
    df = client_gbq.query(query_proc, location='US').result().to_dataframe()
    return df.to_json()


def pull_data_df(ds, **kwargs):
    ti = kwargs['ti']
    print(ti)
    pulled_value_1 = ti.xcom_pull(key=None, task_ids='task_python')

    df = pd.DataFrame.from_dict(json.loads(pulled_value_1))
    print(df)
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

query1 = """

create or replace  table `{{ params.proyecto }}.{{ params.dataset }}.{{ params.toTable }}` 
as (
SELECT FORMAT_DATE('%Y%m',MONTH) AS MONTH
  from UNNEST(
      GENERATE_DATE_ARRAY(DATE_TRUNC('2016-09-01', MONTH), DATE_SUB( '{{ params.now }}', INTERVAL 1 month), INTERVAL 1 MONTH)
  ) AS MONTH)

"""

query2 = """select * from `{0}.{1}.{2}`"""


# OJO: schedule_interval=None -> We only want the DAG to run when we POST to the api
with DAG(nameDAG, default_args=default_args, schedule_interval=None) as dag:
    t_begin = DummyOperator(task_id="begin")
    #######################################################################################
    # Aqui debe ir lo que tienes que hacer con el mensaje
    task_python = PythonOperator(task_id='task_python',
                                 provide_context=True,
                                 python_callable=get_data_df,
                                 op_kwargs={'proyecto': project,
                                            'query': query2,
                                            'dataset': "test_001",
                                            'table': "lista_fechas"})

    task_python_pull = PythonOperator(task_id='task_python_pull',
                                      provide_context=True,
                                      python_callable=pull_data_df,
                                      op_kwargs={'proyecto': project,
                                                 'query': query2,
                                                 'dataset': "test_001",
                                                 'table': "lista_fechas"})

    task_bq1 = BigQueryOperator(
        task_id='task_bq1',
        sql=query1,
        use_legacy_sql=False,
        bigquery_conn_id=GBQ_CONNECTION_ID,
        params={
            'proyecto': project,
            'dataset': 'test_001',
            'toTable': "lista_fechas",
            'now': datetime.datetime.today().strftime('%Y-%m-%d')
        }
    )

    #######################################################################################
    t_end = DummyOperator(task_id="end")

    t_begin >> task_python >> task_python_pull >> t_end
    """ t_begin >> task_python >> task_bq1 >> t_end """
