# Importing necessary modules
from airflow import DAG
from airflow.operators.python import PythonOperator
import gate_api
from gate_api.exceptions import ApiException, GateApiException
import pandas as pd
from datetime import datetime, timedelta
import os, time
import numpy as np

# Default arguments for the Airflow DAG
default_args = {
    'owner': 'airscholar',  # Owner of the DAG
    'start_date': datetime(2023, 12, 21, 12, 36)  # Start date of the DAG
}

# Setting up the start and end dates for data fetching
start_date=datetime.now() - timedelta(minutes=960)
end_date=datetime.now() - timedelta(minutes=70)
interval='5m'  # Interval for data fetching
pair = 'SWAP_USDT'  # Currency pair to fetch data for

# Function to fetch historical data from the API
def fetch_historical_data(api_instance, pair, interval, start, end):
    
    try:
        # Fetching candlestick data from the API
        candles = api_instance.list_candlesticks(currency_pair=pair, interval=interval, _from=start, to=end)
        # Creating a DataFrame from the fetched data
        data = pd.DataFrame(candles, columns=['timestamp', 'volume', 'close', 'high', 'low', 'open', 'amount'])
        data['timestamp'] = pd.to_datetime(data['timestamp'], unit='s')  # Converting timestamp to datetime
        return data
    except ApiException as e:
        print("An exception occurred when calling SpotApi->list_candlesticks: %s\n" % e)
        return pd.DataFrame()

# Function to aggregate pair data over a given interval
def get_pair_data(pair, interval, start_date, end_date):
    # Setting up the API configuration
    configuration = gate_api.Configuration(host="https://api.gateio.ws/api/v4")
    api_client = gate_api.ApiClient(configuration)
    api_instance = gate_api.SpotApi(api_client)

    all_data = pd.DataFrame()
    # Looping through the time segments to fetch data
    while start_date < end_date:
        # Creating 20-day segments to stay within the 1000 point limit
        segment_end = start_date + timedelta(days=20)  
        if segment_end > end_date:
            segment_end = end_date

        segment_data = fetch_historical_data(api_instance, pair, interval, int(start_date.timestamp()), int(segment_end.timestamp()))
        all_data = pd.concat([all_data, segment_data])
        start_date = segment_end

    all_data.sort_values('timestamp', inplace=True)
    # Renaming columns to include currency pair
    all_data.columns = [col + '_' + pair.split('_')[0].lower() if col != 'timestamp' else col for col in all_data.columns]

    # Setting timestamp as the index and converting data to JSON
    all_data['timestamp'] = pd.to_datetime(all_data['timestamp'])
    all_data.set_index('timestamp', inplace=True)    
    all_data = all_data.to_json(orient='table', indent=4).encode('utf-8')
    return all_data

# Function to stream data using Kafka
def stream_data(pair=pair, interval=interval, start_date=start_date, end_date=end_date):
    import json
    from kafka import KafkaProducer
    import time
    import requests as req
    import logging

    # Setting up the Kafka producer
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=50000)

    curr_time = time.time()

    # Loop to send data at regular intervals
    while True:
        if time.time() > curr_time + 1:
            break
        try:
            res = get_pair_data(pair, interval, start_date, end_date)
            producer.send('data_update', res)  # Sending data to the 'data_update' topic
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

# Defining the DAG for the Airflow task
with DAG('data_automation', 
         default_args=default_args, 
         schedule=timedelta(minutes=5),  # Setting the schedule for the DAG
         catchup=False) as dag:

         # Defining the PythonOperator to run the streaming task
         streaming_task = PythonOperator(
                 task_id= 'stream_data_from_api',
                 python_callable=stream_data
         )

# stream_data()
# data = get_pair_data(pair, interval, start_date, end_date)
# print(data)