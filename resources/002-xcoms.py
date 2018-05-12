"""
Code that goes along with the Airflow tutorial located at:
https://github.com/airbnb/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from random import choice

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

# Create a DAG with xcom DAG is
dag = DAG('tutorial', default_args=default_args)

def xcom_setter(key, value, **kwargs):
    ti = kwargs['TI']
    ti.xcom_push(key=key, value=value)

def xcom_getter(task_id, key, **kwargs):
    ti = kwargs['TI']
    value = ti.xcom_pull(task_id=task_id, key=key)
    print("The xcom value is {}".format(value))
    return value


# First task will call xcom_setter
task1 = PythonOperator(task_id='xcom_setter',
                             dag=dag,
                             python_callable=xcom_setter,
                             provide_context=True,
                             op_args=['my_key', choice(range(0,10000))])

# Second task will call xcom_getter
task2 = PythonOperator(task_id='xcom_getter',
                       dag=dag,
                       python_callable=xcom_getter,
                       provide_context=True,
                       op_args=['xcom_setter', 'my_key'])

# task1 is upstream for task2
task2.set_upstream(task1)   
# or task1 << task2
# or task2 >> task1
# or task1.set_downstream(task2)
