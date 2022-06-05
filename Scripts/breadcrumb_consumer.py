# Title: breadcrumb_consumer.py
# Authors: Anthony Pham, Ilias Bonie, Long Huynh, Rajat Kulkarni
# LINE TO SUBSTITUTE DOUBLE QUOTES FOR SINGLE QUOTES FROM:
# https://stackoverflow.com/questions/39491420/python-jsonexpecting-property-name-enclosed-in-double-quotes

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

topic = "purchases5"
topic2 = "num_of_msgs5"

num_msgs = 0
actual_msgs = 0

#all_records = []
all_records = ""

DBname = "postgres"
DBuser = "postgres"
DBpwd = "PLEASE INPUT PASSWORD"
TableBreadCrumbName = 'BreadCrumb'
TableTripName = "Trip"
Datafile = "filedoesnotexist"
CreateDB = False

# todays date
pst_tz = timezone('US/Pacific')
today = datetime.datetime.now(pst_tz)
today = today.strftime('%Y-%m-%d')


def create_Kafka_consumer_with(config):
    consumer = Consumer(config)
    return consumer


def consume_messages_with(consumer, topic, topic2):
    global actual_msgs
    global num_msgs 
    global all_records

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
            # Message is found in the topic (num_msgs).
            else:
                print("NUM_MSGS:consuming")
                num_msgs = int(msg.value().decode())
                # Now that the number of messages to read is known,
                # subscribe to the second topic to actually read the
                # messages.
                consumer.subscribe([topic], on_assign=reset_offset)
                try:
                    while actual_msgs < num_msgs:
                        msg = consumer.poll(1.0)
                        if msg is None:
                            print("INNER: Waiting...")
                        elif msg.error():
                            print("INNER: ERROR: %s".format(msg.error()))
                        else:
                            # Consume num_msgs number of messages
                            #print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))        
                            msg_bytes = msg.value()
                            msg_string = msg.value().decode('utf-8')
                            json_data = re.sub( "(?<={)\'|\'(?=})|(?<=\[)\'|\'(?=\])|\'(?=:)|(?<=: )\'|\'(?=,)|(?<=, )\'", "\"", msg_string)
                            all_records = all_records + json_data
                            actual_msgs += 1 
                except KeyboardInterrupt:
                    pass
                finally:
                    actual_msgs = 0
                    print(all_records)
                    all_records = '[' + all_records + ']'
                    all_records = all_records.replace("}", "},")
                    all_records = "".join(all_records.rsplit(",", 1))
                    json_obj = json.loads(all_records)
                    json_formatted = json.dumps(json_obj, indent = 2)
                    fil = open('{}.txt'.format(today), "a")
                    fil.write(json_formatted)
                    #fil.write('/n')
                    fil.close()
                    all_records = ""
                    '''json_formatted = json.dumps(all_records, indent=2)
                    print("INNER: WRITING TO FILE: {}", json_formatted)
                    fil = open('{}.txt'.format(today), "a")
                    fil.write(json_formatted)
                    fil.close()'''
                    print("EXITING INNER")
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()


'''def consume_messages_with(consumer, topic, topic2):
    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)
    
    # Subscribe to topic
    consumer.subscribe([topic], on_assign=reset_offset)
   
    trimet_data = ""
    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                print("c")
                # print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))        
                # WRITE DATA TO A FILE
                msg_bytes = msg.value()
                msg_string = msg.value().decode('utf-8')
                # LINE TO SUBSTITUTE DOUBLE QUOTES FOR SINGLE QUOTES FROM:
                # https://stackoverflow.com/questions/39491420/python-jsonexpecting-property-name-enclosed-in-double-quotes
                valid_json = re.sub( "(?<={)\'|\'(?=})|(?<=\[)\'|\'(?=\])|\'(?=:)|(?<=: )\'|\'(?=,)|(?<=, )\'", "\"", msg_string)
                json_data = valid_json

                #Appends the entry to the string
                trimet_data = trimet_data + json_data
 
    except KeyboardInterrupt:
        pass
    finally:
        print("********FINALLY************")
        print("********FINALLY************")
        if trimet_data != "": 
            # Formatting the trimet data
            # Add square brackets to the front and end
            # replace all } with },
            trimet_data = '[' + trimet_data + ']'
            trimet_data = trimet_data.replace("}", "},")
            # replace the last trailing , empty space
            trimet_data = "".join(trimet_data.rsplit(",", 1))
            # convert the string to a json obj and insert to text file
            json_obj = json.loads(trimet_data)
            json_formatted = json.dumps(json_obj, indent=2)
            fil = open('{}.txt'.format(today), "a")
            fil.write(json_formatted)
            fil.write('\n')
            fil.close()

        # Leave group and commit final offsets
        consumer.close()
'''


# load data into a dataframe
def load_data():
    Datafile = '{}.txt'.format(today)
    return pd.read_json(Datafile)
# drop all the unnecessary columns 
def drop_columns(df):
    return df.drop(columns=['RADIO_QUALITY','GPS_SATELLITES', 'GPS_HDOP', 'SCHEDULE_DEVIATION', 'METERS']) 

def before_transformation_validation(df):
    # converting the df to numeric or string
    direction = pd.to_numeric(df['DIRECTION'])
    event_no_trip = pd.to_numeric(df['EVENT_NO_TRIP'])
    event_no_stop = pd.to_numeric(df['EVENT_NO_STOP'])
    velocity = pd.to_numeric(df['VELOCITY'])
    act_time = pd.to_numeric(df['ACT_TIME'])
    gps_latitude = pd.to_numeric(df['GPS_LATITUDE'])
    gps_longitude = pd.to_numeric(df['GPS_LONGITUDE'])
    vehicle_id = pd.to_numeric(df['VEHICLE_ID'])
    opd_date = df['OPD_DATE']
    # flag 
    # I have this variable because we might want to avoid adding data to database if any assertions is false. Thus returning flag as false
    # currently it doesn't to anything
    flag = True
    for index, row in df.iterrows(): 
        # Direction will always be between 0 to 359
        if(pd.isna(direction[index])):
            df.at[index, 'DIRECTION'] = -1
        if(pd.isna(velocity[index])):
            df.at[index, 'VELOCITY'] = -1
        if(direction[index] < 0 or direction[index] > 359):
            print("DIRECTION IS FALSE")
            print(direction[index])
        if(velocity[index] < 0 or velocity[index] > 37):
            print("ASSERTION FAILED: VELOCITY")
            print(velocity[index])
        if(gps_latitude[index] <45 or gps_latitude[index] >= 46):
            print("ASSERTION FAILED: GPS_LATITUDE")
            print(gps_latitude[index])
        if(pd.isna(event_no_trip[index])):
            print("ASSERTION FAILED: EVENT_NO_TRIP")
            print(event_no_trip[index])
        if(pd.isna(vehicle_id[index])):
            print("ASSERTION FAILED: VEHICLE_ID")
            print(vehicle_id[index])
        if(pd.isna(gps_latitude[index]) or pd.isna(gps_longitude[index])):
            if(pd.isna(gps_latitude[index]) and not pd.isna(gps_longitude[index])):
                print("ASSERTION FAILED: gps_latitude is empty")
                print(index)
            elif(not pd.isna(gps_latitude[index]) and pd.isna(gps_longitude[index])):
                print("ASSERTION FAILED: gps_longitude is empty")
                print(index)
            else:
                df.at[index, "GPS_LONGITUDE"] = -1
                df.at[index, "GPS_LATITUDE"] = -1
        if(pd.isna(event_no_trip[index]) or pd.isna(event_no_stop[index])):
            if(pd.isna(event_no_trip[index]) and not pd.isna(event_no_stop[index])):
                print("ASSERTION FAILED: event_no_trip is empty")
                print(index)
            elif(not pd.isna(event_no_trip[index]) and pd.isna(event_no_stop[index])):
                print("ASSERTION FAILED: event_no_stop is empty")
                print(index)
        if(opd_date[index] == ""):
            print("ASSERTION FAILED: opd_date")
            print(index)
    return flag

# Adds 2 new columns (Speed and Date)
def transformations(df):
    speed = []
    spee = pd.to_numeric(df['VELOCITY'])
    for index, row in df.iterrows(): 
        speed.append(spee[index] * 2.237)
    df['SPEED'] = speed
    df = fix_date(df)
    return df

# Create a new column (Date) and appends to it if ACT_TIME overflows (> 86399)
def fix_date(df):
    old_date = df['OPD_DATE']
    old_time = df['ACT_TIME']
    combined_date = [] 
    for index, row in df.iterrows():
        datetime_object = datetime.datetime.strptime(old_date[index], '%d-%b-%y').date()
        # date.app
        new_delta = datetime.timedelta(seconds = int(old_time[index]))
        new_time = datetime.time(hour= new_delta.seconds//3600, minute= (new_delta.seconds//60)%60, second = new_delta.seconds%60)
        carry = new_delta.days
        date_time = datetime.datetime.combine(datetime_object , new_time)
        if (carry == 1):
            date_time += timedelta(days=1)
        combined_date.append(date_time)
    df['TIME_STAMP'] = combined_date
    return df

def dbconnect():
    connection = psycopg2.connect(
            host="localhost",
            database=DBname,
            user=DBuser,
            password=DBpwd,
    )
    connection.autocommit = True
    return connection

def createTable(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"""
                CREATE TABLE {TableBreadCrumbName} 
                (tstamp TIMESTAMP, 
                latitude DECIMAL, 
                longitude DECIMAL, 
                direction INTEGER, 
                speed DECIMAL, 
                trip_id INTEGER);""")
    print(f"Create {TableBreadCrumbName} Table")

'''def createTripTable(conn):
    with conn.cursor() as cursor:
        cursor.execute(f""" 
                DROP TABLE IF EXISTS {TableTripName};
                CREATE TABLE {TableTripName}
                (trip_id INTEGER,
                route_id INTEGER,
                vehicle_id INTEGER,
                service_id INTEGER,
                direction INTEGER);""")
    print(f"Create {TableTripName} Table")'''



def load_db(conn, df):
    with conn.cursor() as cursor:
        print(f"Loading {len(df)} rows")
        for index, row in df.iterrows():
            cursor.execute(f"""
            INSERT INTO {TableBreadCrumbName}(
            tstamp, 
            latitude, 
            longitude, 
            direction, 
            speed, 
            trip_id) 
            VALUES ('{df['TIME_STAMP'][index]}', {df['GPS_LATITUDE'][index]}, {df['GPS_LONGITUDE'][index]}, {df['DIRECTION'][index]}, {df['SPEED'][index]}, {df['EVENT_NO_TRIP'][index]});""") 



def get_df_add_to_db():
    df = load_data()
    df = drop_columns(df)
    before_transformation_validation(df)
    df = transformations(df)

    # Connect to the database
    conn = dbconnect()

    # Create breadcrumb and trip table
    createTable(conn)

    # Load the data from dataframe to the database 
    load_db(conn, df)

    # prints the df
    #print(df)




if __name__ == '__main__':
    print("Entered main")
    # print(num_msgs)
    # actual_msgs += 100
    # print(actual_msgs)
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

    # Receives the data and store it as a dataframe
    # Transform & validate it and store it into the database
    # Project part 2 
    get_df_add_to_db()
