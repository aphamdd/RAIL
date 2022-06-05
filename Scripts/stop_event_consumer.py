#!/usr/bin/env python3
import sys
import re
import json
import pandas as pd
import numpy as np
from datetime import date, timedelta, time
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
from pytz import timezone
import datetime
import itertools
import psycopg2


topic = "stop_event"
topic2 = "stop_event_num_of_msgs"

num_msgs = 0
actual_msgs = 0
total_list = []

DBname = "postgres"
DBuser = "postgres"
DBpwd = "Please Input password"
TableTripName = "Trip"
Datafile = "filedoesnotexist"
CreateDB = False

def drop_stop_event_columns(df):
    df = df.drop(columns=['leave_time','train','stop_time','arrive_time', 'dwell', 'location_id', 'door', 'lift', 'ons', 'offs', 'estimated_load', 'maximum_speed', 'train_mileage', 'pattern_distance', 'location_distance', 'x_coordinate', 'y_coordinate', 'data_source', 'schedule_status'])
    # remove all duplicates rows EXCEPT for first instance of that row
    df = df.drop_duplicates()
    return df

def upload_to_df():
    global total_list
    data = list(itertools.chain.from_iterable(total_list))
    return pd.DataFrame(data, columns=['trip_id', 'vehicle_number', 'leave_time', 'train', 'route_number', 'direction', 'service_key', 'stop_time', 'arrive_time', 'dwell', 'location_id', 'door', 'lift', 'ons', 'offs', 'estimated_load', 'maximum_speed', 'train_mileage', 'pattern_distance', 'location_distance', 'x_coordinate', 'y_coordinate', 'data_source', 'schedule_status'])

def create_Kafka_consumer_with(config):
    consumer = Consumer(config)
    return consumer


def consume_messages_with(consumer, topic, topic2):
    global actual_msgs
    global num_msgs 
    global total_list

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)
    
    # Subscribe to topic
    consumer.subscribe([topic2], on_assign=reset_offset)
  
    # Poll for new messages from Kafka and print them.
    try:
        while True:
            consumer.subscribe([topic2], on_assign=reset_offset)
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                print("NUM_MSGS:consuming")
                num_msgs = int(msg.value().decode())
                print(type(num_msgs))
                consumer.subscribe([topic], on_assign=reset_offset)
                try:
                    while actual_msgs < num_msgs:
                        msg = consumer.poll(1.0)
                        if msg is None:
                            print("INNER: Waiting...")
                        elif msg.error():
                            print("INNER: ERROR: %s".format(msg.error()))
                        else:
                            print("consumering record {} out of {}".format(actual_msgs, num_msgs))
                            msg_string = msg.value().decode('utf-8')
                            msg_list = list(eval(msg_string))
                            total_list.append(msg_list)
                            actual_msgs += 1 
                except KeyboardInterrupt:
                    pass
                finally:
                    actual_msgs = 0
                    print("EXITING INNER")
                    return 
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()

#FINISHED
def dbconnect():
    connection = psycopg2.connect(
            host="localhost",
            database=DBname,
            user=DBuser,
            password=DBpwd,
    )
    connection.autocommit = True
    return connection

# NOT DONE --> service_id is a character, not a INTEGER
def createTable(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {TableTripName} 
                (trip_id INTEGER, 
                vehicle_number INTEGER, 
                route_number INTEGER, 
                service_key VARCHAR(2), 
                direction INTEGER);""")
    print(f"Create {TableTripName} Table")

def checkServiceKey(value):
    if(value == 'M' or value == 'T' or value == 'W' or value == 'H' or value == 'F' or value == 'S' or value == 'U'):
        return True
    return False

def validation(df):
    # converting the df to numeric
    direction = pd.to_numeric(df['direction'])
    trip_id = pd.to_numeric(df['trip_id'])
    vehicle_number = pd.to_numeric(df['vehicle_number'])
    route_number = pd.to_numeric(df['route_number'])
    service_key = df['service_key']
    for index, row in df.iterrows(): 
        #direction will always be 0 or 1 
        if direction[index] != 0 and direction[index] != 1:
            print("ASSERTION FAILED: direction is a value other than 1 or 0")
            print("its trip_id is {}".format(trip_id[index]))
            # setting direction at -1. CHANGED THIS LATER
            df.at[index, 'direction'] = -1
        #There is always a route_id
        if pd.isna(route_number[index]):
            print("ASSERTION FAILED: route_id doesn't exist")
        #There should be a vehicle number
        if pd.isna(vehicle_number[index]):
            print("ASSERTION FAILED: vehicle_number doesn't exist")
        # service_key should be either M, T, W, H, F, S, U
        if not checkServiceKey(service_key[index]):
            print("ASSERTION FAILED: service_key")
            print("The incorrect service key is {}".format(service_key[index]))
        
def load_db(conn, df):
    with conn.cursor() as cursor:
        print(f"Loading {len(df)} rows")
        for index, row in df.iterrows():
            cursor.execute(f"""
            INSERT INTO {TableTripName}(
            trip_id, 
            vehicle_number,
            route_number, 
            service_key,
            direction) 
            VALUES ({df['trip_id'][index]}, {df['vehicle_number'][index]}, {df['route_number'][index]}, '{df['service_key'][index]}', {df['direction'][index]});""") 

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()
    
    # Parse the configuration.
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])
    
    consumer = create_Kafka_consumer_with(config) 
    consume_messages_with(consumer, topic, topic2)

    #Creates df and drops unnecessary columns
    df_stop_event = upload_to_df()
    df_stop_event = drop_stop_event_columns(df_stop_event)

    #validate the data
    validation(df_stop_event)

    #connect to the db
    conn = dbconnect()

    # Create trip table
    createTable(conn)

    # load to db
    load_db(conn, df_stop_event)

    print(df_stop_event.head(50))
