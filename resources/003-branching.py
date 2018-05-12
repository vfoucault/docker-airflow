"""
Code that goes along with the Airflow tutorial located at:
https://github.com/airbnb/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from datetime import datetime, timedelta
from random import choice

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator

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

dag = DAG('branching', default_args=default_args)


def random_choice(tasks):
    """Will pick one entry randomly from tasks list"""
    return choice(tasks)


def task_action(path_name):
    print("I'm in path {}".format(path_name))


# Here is a list of all the tasks id (name)
tasks_ids = ['task_a', 'task_b', 'task_c']

# Let's add a BranchOperator that will choose a path
branch_operator = BranchPythonOperator(task_id='branch_operator',
                                       dag=dag,
                                       python_callable=random_choice,
                                       op_args=[tasks_ids])


# Create the last task that will be the downstream of all other tasks
# Mind the trigger rule
last_task = DummyOperator(task_id='last_task',
                          dag=dag,
                          trigger_rule='all_done')

# Loop over tasks_ids to create all tasks
for task in tasks_ids:
    # Create the PythonOperatorTask with the correct params
    path_task = PythonOperator(task_id=task,
                               dag=dag,
                               python_callable=task_action,
                               op_args=[task])

    # Add the upstream for this task
    path_task.set_upstream(branch_operator)

    # Create the next task for this path
    dummy_task = DummyOperator(task_id='dummy_{}'.format(task),
                               dag=dag)

    # Add the Downstream dummy task
    path_task.set_downstream(dummy_task)

    # Add the downstream last task for the dummy task
    dummy_task.set_downstream(last_task)

# Voila !


