import datetime as dt
import os
import sys

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

import pendulum

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__))))
from csv2stage import csv2stage_task
from stage2nds import stage2nds_task
from nds2dds import nds2dds_task

postgres_connection = BaseHook.get_connection('postgres_connection')
conf_vars = Variable.get('sm_s_etl_vars', deserialize_json=True)
airflow_local_tz = pendulum.timezone('Europe/Moscow')

default_args = {
    'owner': 'selezenev_aa',
    'start_date': dt.datetime(2023, 7, 21).astimezone(airflow_local_tz)
}

with DAG('supermarket_sales_etl',
         default_args=default_args,
         description='Supermarket sales ETL',
         tags=['gw', 'netology'],
         schedule_interval=conf_vars['base']['schedule_interval'],
         catchup=False
         ) as dag:

    csv2stage = PythonOperator(
        task_id="csv2stage_task",
        python_callable=csv2stage_task,
        op_kwargs={
            'filepath': conf_vars['csv2stage']['source']['filepath']
            , 'hostname': postgres_connection.host
            , 'login': postgres_connection.login
            , 'password': postgres_connection.password
            , 'db': conf_vars['base']['db']
            , 'target_schema': conf_vars['csv2stage']['target']['target_schema']
            , 'target_table': conf_vars['csv2stage']['target']['target_table']
            , 'today': conf_vars['base']['today']
            , 'is_manual': conf_vars['base']['is_manual']
        },
        provide_context=True,
    )

    stage2nds = PythonOperator(
        task_id="stage2nds_task",
        python_callable=stage2nds_task,
        op_kwargs={
            'hostname': postgres_connection.host
            , 'login': postgres_connection.login
            , 'password': postgres_connection.password
            , 'db': conf_vars['base']['db']
            , 'source_schema': conf_vars['stage2nds']['source']['source_schema']
            , 'source_table': conf_vars['stage2nds']['source']['source_table']
            , 'target_schema': conf_vars['stage2nds']['target']['target_schema']
            , 'target_tables': conf_vars['stage2nds']['target']['target_tables']
            , 'today': conf_vars['base']['today']
            , 'is_manual': conf_vars['base']['is_manual']
        },
        provide_context=True,
    )

    nds2dds = PythonOperator(
        task_id="nds2dds_task",
        python_callable=nds2dds_task,
        op_kwargs={
            'hostname': postgres_connection.host
            , 'login': postgres_connection.login
            , 'password': postgres_connection.password
            , 'db': conf_vars['base']['db']
            , 'source_schema': conf_vars['nds2dds']['source']['source_schema']
            , 'source_tables': conf_vars['nds2dds']['source']['source_tables']
            , 'target_schema': conf_vars['nds2dds']['target']['target_schema']
            , 'target_tables': conf_vars['nds2dds']['target']['target_tables']
            , 'holydays': conf_vars['base']['holydays']
            , 'today': conf_vars['base']['today']
            , 'is_manual': conf_vars['base']['is_manual']
        },
        provide_context=True,
    )

    csv2stage >> stage2nds >> nds2dds