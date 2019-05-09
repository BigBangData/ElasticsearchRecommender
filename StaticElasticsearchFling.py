# !/usr/bin/env python

"""
author   : Marcelo Sanches
doc name : Flinging into Elasticsearch
purpose  : to mimic a Kafka producer-consumer and fling messages into Elasticsearch
date     : 05.06.2019
version  : 3.7.2
"""

# Import modules 
import pandas as pd
import requests 
import json
import os 
from elasticsearch import Elasticsearch

# Setup functions
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
	
    if r.status_code != 201:
        print(" "*100)
        print("*"*80)
        print("Error sending message: status code " +str(r.status_code))
    else:
        #print(invoice_name) # print basket number if desired (slows down process)
        pass
		

# Run 
if __name__ == "__main__":
	
	# Elasticsearch setup
	r = requests.get("http://elasticsearch:9200")
	if r.status_code != 200:
		print("Error talking to Elasticsearch")

	# Index name 
	index_name = "recommender_system"	

	# Delete index -- since this is a static filling up of Elasticsearch, re-filling would just 
	# produce duplicates, in a real production system one would NOT delete the index 
	delete_index(index_name)	

	# Check whether index exists, if not, create it, then list all indices
	check_index(index_name)
	print_es_indices()

	# READ DATA 
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

		# fling each basket into Elasticsearch
		ETL_msg(basket)


		
		
