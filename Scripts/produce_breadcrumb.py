#!/usr/bin/env python3

import urllib3
import json
import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
from confluent_kafka import Consumer
import base64
import logging
import ast
import re


topic = "purchases5"
topic2 = "num_of_msgs5"


def parse_JSON_data(json_data):
    parsed_json = json.loads(json_data)
    return parsed_json


def get_parsed_json_data_from(URL):
    http = urllib3.PoolManager()
    raw_json = http.request('GET', URL)

    json_data = raw_json.data.decode("utf-8")
    parsed_json_data = parse_JSON_data(json_data)

    return parsed_json_data


def create_Kafka_producer_with(config):
    producer = Producer(config)
    return producer
    

def num_publish_messages_with(producer, num_msgs, topic2):
    print('************************************************************')
    print('PRODUCING  MESSAGES TO TOPIC: ', topic2)
    print('************************************************************')

    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
            #print("P")

    user_id = str(0)
    product = str(num_msgs)
    #product = product.encode()
    print(type(product))

    try:
        producer.produce(topic2, product, user_id, callback=delivery_callback)
        producer.poll(0)
    except BufferError:
        logging.exception('Buffer error: the queue is full. Flushing...')
        producer.flush()
        logging.info('...Queue flushed. Producing message again.')
        producer.produce(topic2, product, user_id, callback=delivery_callback)

    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()


def publish_messages_with(producer, parsed_json_data, topic):
    print('************************************************************')
    print('PRODUCING  MESSAGES TO TOPIC: ', topic)
    print('************************************************************')

    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            #print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
            #    topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
            print("P")

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

    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()


def parse_configuration_with(args):
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    return (config_parser, config)


if __name__ == "__main__":
    print("Entered main")
    
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    (config_parser, config) = parse_configuration_with(args)

    
    parsed_json_data = get_parsed_json_data_from('http://www.psudataeng.com:8000/getBreadCrumbData')
    # Count how many messsages there are for that day
    num_msgs = len(parsed_json_data) 
    # num_msgs = str(num_msgs)
    # num_msgs = num_msgs.encode()
    producer = create_Kafka_producer_with(config)
    num_publish_messages_with(producer, num_msgs, topic2)
    publish_messages_with(producer, parsed_json_data, topic)

    # f = open('sample.json')
    # data = json.loads(f.read())
    # producer = create_Kafka_producer_with(config)
    # publish_messages_with(producer, data)
