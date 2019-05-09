# !/usr/bin/env python

"""
author   : Marcelo Sanches
doc name : Kafka consumer - Elasticsearch flinger 
purpose  : to consume messages from the Kafka producer and fling them into Elasticsearch
date     : 05.07.2019
version  : 3.7.2
"""

# Import modules 
import json
import os 
import time
import pandas as pd
import requests
from concurrent.futures import wait
import ujson
from confluent_kafka import Consumer, KafkaError
from elasticsearch import Elasticsearch

# Elasticsearch setup functions
def create_index(index, index_config):
    """Creates an index in Elasticsearch
    """
    r = requests.put("http://elasticsearch:9200/{}".format(index), json=index_config)
	
    if r.status_code != 200:
        print("Error creating index")
    else:
        print("Index created")

def delete_index(index):
    """Deletes an index in Elasticsearch
    """
    r = requests.delete("http://elasticsearch:9200/{}".format(index))
    if r.status_code != 200:
        print("Error deleting index")
    else:
        print("Index deleted")

def check_index(index_name):
    """Checks whether an index exists in Elasticsearch; if not, creates it with the index 
        configurations specified below.
    """
    es_conn = Elasticsearch("http://elasticsearch:9200")
    res = es_conn.indices.exists(index=index_name)
    if res == True:
        print('index exists')
    else:
        index_config = {"mappings":
                            {"basket":
                                {"properties":
                                    {"timestamp": {"type": "date"},
                                     "StockCodes": {"type": "string"},
                                     "Descriptions": {"type": "string", 
                                                      "index": "not_analyzed"}
                                    }
                                }
                            }
                        }

        create_index(index_name, index_config)

def print_es_indices():
    """Prints to console current Elasticsearch indices.
    """
    r = requests.get("http://elasticsearch:9200/_cat/indices?v")
    if r.status_code != 200:
        print("Error listing indices")
    else:
        print(r.text)

def get_timestamp(date_element):
    """Transform InvoiceDate into an Elasticsearch-friendly timestamp in milliseconds.
    """
    date = date_element.split()[0]
    time = date_element.split()[1]

    year = int(date.split('-')[0])
    month = int(date.split('-')[1])
    day =  int(date.split('-')[2])

    hours = int(time.split(':')[0])
    mins = int(time.split(':')[1])
    secs = int(time.split(':')[2])
    	
    return int(pd.Timestamp(year,month,day,hours,mins,secs).timestamp()*1000)
	
def ETL_msg(msg):
    """ Extract-Transform-Load messages into Elasticsearch
    """
    # reshape into ES-friendly format
    msg["timestamp"] = get_timestamp(msg["InvoiceDate"])
    del msg["InvoiceDate"]

    # fling into ES
    r = requests.post("http://elasticsearch:9200/recommender_system/basket", json=msg)
	
	# if there is an error, display the code 
    if r.status_code != 201:
        print(" "*100)
        print("*"*80)
        print("Error sending message: status code " +str(r.status_code))
	
	# else print the basket being consumed (uncomment pass and comment print for quicker consumer)
    else:
		#pass
        print("consumed basket " +msg['InvoiceNo'])
		

# Run 
if __name__ == "__main__":
    # Kafka consumer setup 
    recommender_system_topic = 'recommender.system.1'

    c = Consumer({'bootstrap.servers': 'kafka-1:9092',
        	      'group.id': 'recommender.system.consumer.2',
		      'api.version.request': True,
		      'log.connection.close': False,
		      'socket.keepalive.enable': True,
		      'session.timeout.ms': 6000,
		      'default.topic.config': {'auto.offset.reset': 'smallest'}})
        
    # subscribe to Kafka producer topic
    c.subscribe(['recommender.system.1'])
        
    # Elasticsearch setup
    r = requests.get("http://elasticsearch:9200")
    if r.status_code != 200:
        print("Error talking to Elasticsearch")
            
    # delete index if exists (no duplicate data for our case)
    index_name = "recommender_system"
    #delete_index(index_name)
        
    # check whether index exists, if not, create it; list indices
    check_index(index_name)
    print_es_indices()


    # consume stream 
    while True:
            
        # consume one message at a time 
        msg = c.poll(timeout=10.0)
            
        # error check for no messages
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())
            
        # if no error (message received)
        else:
            # decode bytes into strings, deserialize strings into dictionary structure
            data = ujson.loads(msg.value().decode('utf-8'))
                
            # fling each basket into Elasticsearch
            ETL_msg(data)

		
		
