# !/usr/bin/env python

"""
author   : Marcelo Sanches
doc name : Querying Elasticsearch
purpose  : to get user input and query Elasticsearch, returning product recommendations
date     : 05.06.2019
version  : 3.7.2
"""

# Import modules
import requests
from prettytable import PrettyTable

# Setup functions
def execute_es_query(index, query, userinput):
    """Executes an Elasticsearch query given an index, a query type (in Lucene), 
    and a string provided by the user to query the index.
    """
    r = requests.get("http://elasticsearch:9200/{}/_search".format(index), json=query)
    if r.status_code != 200:
        print("Error executing query")
        return None
    else:
        return r.json()

def sanitize(input_string):
    """Basic sanitization of input against script tags (< >).
    """
    output_string = ''
    for i in input_string:
        if i == '>':
            outchar = '&gt;'
        elif i == '<':
            outchar = '&lt;'
        else:
            outchar = i
        output_string += outchar

    return output_string

	
# Setup classes 
class queryType():
    """This class handles different types of Elasticsearch queries the system makes.
    """
	
    def __init__(self, userinput):
        self.userinput = userinput

    def query_descriptions(self):
        """Queries descriptions that match a user input and aggregate, count, and 
           returns the significant descriptions with counts above 10.
        """
        query_descriptions = {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"Descriptions": self.userinput}},
                    ]
                }
            },
            "aggs": {
                "correlated_words": {
                    "significant_terms": {
                        "field": "Descriptions",
                        "exclude": self.userinput,
                        "min_doc_count": 10
                    }
                }
            }
        }

        return query_descriptions

    def query_all(self):
        """Queries all matches in Elasticsearch, to be used further for suggesting 
           product names when a user is not aware of them.
        """
        query_all = {
            "query": {"match_all": {} }
            }

        return query_all

		
class queryUserinput():
    """This class only has one function whose main purpose is to return a list of 
	   recommendations, given some user input. 
    """
	
    def __init__(self):
        self.self = self
    
    def user_query(self):
        """Gets user input (a word, hopefully a product name) and passes that as a 
           query to Elasticsearch, returning (if a product name is passed) a list of 
           recommendations for similar items.
        """
        print(" "*100)
        # gets user input
        userinput = sanitize(input("Please Enter Your Product To Query: "))
        userinput = userinput.upper().strip()

        # queries Elasticsearch
        Q =  queryType(userinput)
        query = Q.query_descriptions()
        res = execute_es_query('recommender_system', query, userinput)
        buckets_list = res['aggregations']['correlated_words']['buckets']

        # populates a list of recommendations with counts 
        also_bought = []
        for bucket in buckets_list:
            also_bought.append([bucket['key'],bucket['doc_count']])

        return (userinput, also_bought)

		
class prettyResponse():
    """This class handles responses by the system in the form of pretty tables.
    """

    def __init__(self, userinput, also_bought):
        self.userinput = userinput
        self.also_bought = also_bought
	
    def similar_items(self):
        """Displays a table of recommendations for similar items.
        """
        print(" "*100); print("_"*100); print(" "*100)
        print("Customers who bought " +str(self.userinput) +" also bought: \n")

        # build pretty table with list of recommendations
        x = PrettyTable()
        x.field_names = ["Product Recommended", "Number of Customers Who Bought This Product"]
        for product, count in self.also_bought:
            x.add_row([product, count])
        
        print(x); print(" "*100)

    def not_found_continue(self):
        """For a wrong product name, displays a table of products that one might 
           want to use as in a query.
        """
        print(" "*100); print("_"*100); print(" "*100)
        print("The item " +userinput +" does not appear to be in our list of products.")
        print(" "*100)
        print("Here are some products we have that you could use in your query: ")
        print(" "*100)
        
        # query Elasticsearch again for products containing keywords in the user input
        Q = queryType(userinput)
        query = Q.query_all()
        res = execute_es_query('recommender_system', query, userinput)

        # build list of current product names to choose from
        products_list = []
        for hit in res['hits']['hits']:
            for description in hit['_source']['Descriptions']:
                if userinput in description:
                    products_list.append(description)

        # remove duplicates and build pretty table
        x = PrettyTable()
        x.field_names = ["Products Containing "+userinput]
        products_list = list(set(products_list))
        for item in products_list:
            x.add_row([item])
        print(x); print(" "*100)
	
    def not_found_end(self):
        """Message received after four unsuccessful attempts.
        """
        print(" "*100); print("_"*100); print(" "*100)
        print("The item " +userinput +" does not appear to be in our list of products.")
        print(" "*100)
        print("You have no more attempts. Good bye!")
        print(" "*100)		


# Run
if __name__ == "__main__":

    # get user input, return list of recommendations
    UI = queryUserinput()
    (userinput, also_bought) = UI.user_query()

    # user has four attempts to get a product name correctly
    for i in range(0, 4):

        # first 3 attemps
        if i < 3:

            # if there are no products with that name, offer a list of products
            if len(also_bought) == 0:
                table = prettyResponse(userinput, also_bought)
                table.not_found_continue()
                if i == 2:
                    print("You have " +str(3-i) +" attempt left.")
                else:
                    print("You have " +str(3-i) +" attempts left.")
        
                # get user input again
                UI = queryUserinput()
                (userinput, also_bought) = UI.user_query()

            else:
                # display recommendations if product name was found
                table = prettyResponse(userinput, also_bought)
                table.similar_items()
                break

        # last attempt
        else:
            # display a logout message    
            if len(also_bought) == 0:
                logout = prettyResponse(userinput, also_bought)
                logout.not_found_end()
                
            else:
                # display recommendations if product name was found
                table = prettyResponse(userinput, also_bought)
                table.similar_items()


