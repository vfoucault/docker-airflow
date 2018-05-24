"""
Code that goes along with the Airflow tutorial located at:
https://github.com/airbnb/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from datetime import datetime, timedelta
from random import choice

from airflow.operators.dummy_operator import DummyOperator
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

# We don't want to oversubscribe workers
dag = DAG(???, max_active_runs=1)

# for this one, let's add one first task and one last task
first_task = DummyOperator(???)

last_task = DummyOperator(???)
# For 3 sub elements, it's enough
list_sub_elements = range(0, 3)

# Let's create a subdag and a subdag_operator:
for element in list_sub_elements:
    # Add a subdag. It's id should be composed of its parent name in the form of dag_id.sub_dag_id
    #
    subdag = DAG(???)

    # a subdag will be linked to the parent dag via a subdag operator
    # This task ID should be the same as the subdag id
    subdag_operator = SubDagOperator(???)

    # Humm, let's add some tasks for this subdag:
    task1 = DummyOperator(???)
    task2 = DummyOperator(???)

    # Order these tasks

    # Mind to set the correct up/down streams tasks for the subdag task

