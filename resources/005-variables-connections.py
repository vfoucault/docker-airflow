"""
Code that goes along with the Airflow tutorial located at:
https://github.com/airbnb/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from datetime import datetime, timedelta
from random import choice
import json

from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.operators.subdag_operator import SubDagOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now().replace(microsecond=0, second=0, minute=0) - timedelta(days=10),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


dag = DAG('dag_variable_connection', default_args=default_args)


def extract_currency(currency, **kwargs):
    ti = kwargs['ti']
    data = json.loads(ti.xcom_pull(key='return_value', task_ids='searcher'))
    chosen = data[currency]
    print("Change rate from BTC to {} is {}{}".format(currency, chosen['last'], chosen['symbol']))
    return chosen


# let's get a variable value
currency = Variable.get(???)

# We will use a simple http connection to -> blockchain.info
# Add a search task to get the btc change rate
search_task = SimpleHttpOperator(???)

# Add a display task that will get the xcom data from previous task
# And call the extract_currency method
display_task = PythonOperator(???)

# Order all tasks
