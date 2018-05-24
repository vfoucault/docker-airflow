"""
Code that goes along with the Airflow tutorial located at:
https://github.com/airbnb/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from datetime import datetime, timedelta

from airflow import DAG, settings
from airflow.models import Connection
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import HttpSensor

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


def add_http_connection(name, url, **kwargs):
    """"Add a airflow http connection"""
    new_conn = Connection(
        conn_id=name,
        conn_type='http',
        host=url
    )
    session = settings.Session()
    if not (session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).first()):
        session.add(new_conn)
        session.commit()


# dag sensor will be the on that run sensors and then run tasks
dag = DAG('sensors', default_args=default_args)

# Let's create one http Sensor that will Sense for this url
# Remember to create a http connection, which can be done like this
# https://fkjxmhikx4.execute-api.eu-west-1.amazonaws.com/stage/
# This api will randomly return true/false. 5 false for 1 true
# Let's create the connection. This is normaly done beforehand, but here let's demonstrate that everything is Python
# The response is a json payload true or false, that can easily be checked with a lamdba

add_http_connection(???)

sensor = HttpSensor(???)

# Create a Dummy Operator for this scenario
task = DummyOperator(???)

# Order all tasks
