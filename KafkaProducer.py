# !/usr/bin/env python

"""
author   : Marcelo Sanches
doc name : Kafka Producer 
purpose  : to produce messages to a Kafka topic for a recommender system 
date     : 05.06.2019
version  : 3.7.2
"""

# Import modules 
import os
import time
import ujson
import pandas as pd
import requests
import urllib2
from retrying import retry
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer
from concurrent.futures import wait


# Setup functions
def delete_recommender_system_topic():
    """Deletes the recommender system topic if it exists
    """
    topics = admin.list_topics(timeout=10).topics.keys()

    # delete topic if exists 
    while recommender_system_topic in topics:
        print("Trying to delete " + recommender_system_topic)
        status = admin.delete_topics([recommender_system_topic])
        fut = status[recommender_system_topic]
        try:
            fut.result()
        except Exception as e:
            print(e)
        topics = admin.list_topics(timeout=10).topics.keys()


def create_recommender_system_topic():
    """Creates the recommender system topic if it doesn't already exist
    """
    topics = admin.list_topics(timeout=10).topics.keys()

    # create topic if doesn't already exist
    while recommender_system_topic not in topics:
        print("Trying to create " + recommender_system_topic)
        status = admin.create_topics([NewTopic(recommender_system_topic, 
						num_partitions=3, 
        					replication_factor=1)])
        fut = status[recommender_system_topic]
        try:
            fut.result()
        except Exception as e:
            print(e)
        topics = admin.list_topics(timeout=10).topics.keys()

    print(topics)

	

# Run
if __name__ == '__main__':
    
    # Basic setup 
    recommender_system_topic = 'recommender.system.1'
    brokers = 'kafka-1:9092'
    admin = AdminClient({'bootstrap.servers': brokers})

    # delete and create topic if exists 
    delete_recommender_system_topic()
    create_recommender_system_topic()

    # setup producer 
    p = Producer({'bootstrap.servers': 'kafka-1:9092'})
    
    while True:
	
        # READ DATA -- ideally, hit an endpoint and stream 
        
	#file_path='https://archive.ics.uci.edu/ml/machine-learning-databases/\
        #                                            00352/Online%20Retail.xlsx'
        #response=urllib2.urlopen(file_path)
        #html=response.read()

        
        # reading from local file instead
        # convert Excel file to CSV if first time 
        if os.path.isfile('./Online Retail.xlsx') == True:
            pass
        else:
            df = pd.read_excel("Online Retail.xlsx")
            # filter out missing data (~25% CustomerIDs are NA)
            df = df.dropna()
            # change CustomerID to integer
            df['CustomerID'] = df['CustomerID'].astype(int)
            # save as CSV 
            df.to_csv("Online_Retail.csv", index=False)

        # read in CSV
        df = pd.read_csv('Online_Retail.csv')

        # group by invoice num
        invoice_groups = df.groupby('InvoiceNo')

        # iterate over each group (each invoice)
        for invoice_name, invoice in invoice_groups:
		
            time.sleep(0.5) # remove or reduce value, added to sync with slower consumer
            basket = {}
            stockcodes = []
            descriptions = []
            quantities = []
            unitprices = []
			
            # iterate over rows in this invoice dataframe
            for row_index, row in invoice.iterrows():
			
                # these fields are the same for each row, so doesn't matter if we keep overwriting
                basket['InvoiceNo'] = row['InvoiceNo']
                basket['CustomerID'] = row['CustomerID']
                basket['InvoiceDate'] = row['InvoiceDate']
                basket['Country'] = row['Country']
				
                # these fields are different for each row, so we append to lists
                stockcodes.append(row['StockCode'])
                descriptions.append(row['Description'])
                quantities.append(row['Quantity'])
                unitprices.append(row['UnitPrice'])
                basket['StockCodes'] = stockcodes
                basket['Descriptions'] = descriptions
                basket['Quantities'] = quantities
                basket['UnitPrices'] = unitprices
                
		# produce each message (basket)
                msgbytes = ujson.dumps(basket).encode('utf-8')
                p.produce(recommender_system_topic, msgbytes)
                print("produced basket " +str(invoice_name))

        p.flush()
        print("Done flushing")
        time.sleep(15) # time to kill the producer so as not to duplicate data


