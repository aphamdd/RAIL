import json
import re
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from urllib.request import urlopen
from bs4 import BeautifulSoup
import re
import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
import base64
import logging
import ast


topic = "second_topic"


def create_Kafka_producer_with(config):
    producer = Producer(config)
    
    return producer


def publish_messages_with(producer, data, topic = "second"):
    print("***************************************")
    print("PRODUCING MESSAGES TO TOPIC: ", topic)
    print("***************************************")
    
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    for i in range(0, len(parsed_json_data)):
        user_id = str(i)
        product = str(parsed_json_data[i])
        try:
            producer.produce(topic, product, user_id, callback=delivery_callback)
            producer.poll(0)
        except BufferError:
            logging.exception('Buffer error: the queue is full. Flushing...')
            producer.flush()
            logging.info('...Queue flushed. Producing message again.')
            producer.produce(topic, product, user_id, callback=delivery_callback)
    
    # Block until messages are sent.
    producer.poll(10000)
    producer.flush()


def parse_configuration_with(args):
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    return (config_parser, config)


def calculate_final_values(table_data, h3):
    temp = []
    
    temp.append(h3)

    for i in range(0, 23):
        try:
            temp.append(table_data[i].contents[0])
        except IndexError:
            temp.append(None)

    return temp
        

def parse_table(table, h3):
    final_values = []
    table_data = []

    table_rows = table.find_all('tr')
    for i in range(1, len(table_rows)):
        table_data.append(table_rows[i].find_all('td'))

    #print(calculate_final_values(table_data)) 

    for i in range(0, len(table_data)):
        final_values.append(calculate_final_values(table_data[i], h3))
        
    return final_values


def extract_headers(h3):
    for i in range(0, len(h3)):
        h3[i] = str(h3[i].contents[0])
        h3[i] = re.sub('[^-0-9]+|[0-9]+(?=-)|^-$|-{2,}', '', h3[i])    
    
    return h3
    

def beautiful_soup():
    url = "http://www.psudataeng.com:8000/getStopEvents/"
    html = urlopen(url)
    soup = BeautifulSoup(html, 'lxml')
    tables = soup.find_all('table')
    h3 = extract_headers(soup.find_all('h3'))
    data_to_produce = []
        
    # final_values = parse_table(tables[0], h3[0])
    # print(final_values)
    print("Running...")
    for i in range(0, len(tables)):
        data_to_produce.append(parse_table(tables[i], h3[i]))
        # produce right here
    print("Done with loop!")
    return data_to_produce
    

def main():
    print("Entered main.")
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    (config_parser, config) = parse_configuration_with(args)

    producer = create_Kafka_producer_with(config)

    data_to_produce = beautiful_soup()
    print(len(data_to_produce))
    print("Finished.")


if __name__=="__main__":
    main()
