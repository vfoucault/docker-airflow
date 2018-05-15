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
dag = DAG('dag_subdags', default_args=default_args, max_active_runs=1)

# for this one, let's add one first task and one last task
first_task = DummyOperator(task_id='first_task',
                           dag=dag)

last_task = DummyOperator(task_id='last_task',
                          dag=dag)
# For 10 sub elements
list_sub_elements = range(0, 3)

# Let's create a subdag and a subdag_operator:
for element in list_sub_elements:
    # Add a subdag. It's id should be composed of its parent name
    #
    subdag = DAG(dag_id='dag_subdags.subdag_for_element_{}'.format(element),
                 default_args=default_args)

    # a subdag will be linked to the parent dag via a subdag operator
    # This task ID should be the same as the subdag id
    subdag_operator = SubDagOperator(dag=dag,
                                     subdag=subdag,
                                     task_id='subdag_for_element_{}'.format(element))

    # Humm, let's add some tasks for this subdag:
    task1 = DummyOperator(dag=subdag,
                          task_id='dummy_task_1')
    task2 = DummyOperator(dag=subdag,
                          task_id='dummy_task_2')

    # Order these tasks
    task1.set_downstream(task2)

    # Mind to set the correct up/down streams tasks for the subdag task
    subdag_operator.set_upstream(first_task)
    subdag_operator.set_downstream(last_task)

