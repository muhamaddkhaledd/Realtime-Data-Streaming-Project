import logging
import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests


default_args = {
    'owner':'airscholar',
    'start_date':datetime(2025,10,24,4,00)
}

def get_data():
    res = requests.get("https://randomuser.me/api/")
    data = res.json()
    user = data['results'][0]
    return user

def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data

def stream_data():
    from kafka import KafkaProducer
    import time

    producer = KafkaProducer(
        bootstrap_servers = 'broker:29092',
        max_block_ms = 5000

    )
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:  #1 minute
            break
        try:
            res = get_data()
            final = format_data(res)
            producer.send('user_created',json.dumps(final).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue







with DAG('user_automation',
         default_args = default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

