'''
Created on 2016-09-14
Elasticsearch  Test Tool
@author: Administrator
'''
import argparse
from datetime import datetime
import random
import time, threading
import sys
from threading import Event

from elasticsearch import Elasticsearch, helpers
from elasticsearch import client

parser = argparse.ArgumentParser()
parser.add_argument("--number-of-shards", type = int, default = 3, help="Number of shards per index (default 3)")
parser.add_argument("--number-of-replicas", type = int, default = 1, help="Number of replicas per index (default 1)")
parser.add_argument("--number-of-documents", type = int, help = "Number of documents to write", required = True)
parser.add_argument("--number-of-threads", type = int, help = "Number of threads to write", required = True)
parser.add_argument("--refresh-interval", type = int, default = 10, help = "The interval between two refresh (default 10)")
 
args = parser.parse_args()
 
number_of_shards = args.number_of_shards
number_of_replicas = args.number_of_replicas
number_of_documents = args.number_of_documents
number_of_threads = args.number_of_threads
refresh_interval = args.refresh_interval

es = Elasticsearch(["10.4.200.23", "10.4.200.24", "10.4.200.25"], maxsize=25)
nodes = client.NodesClient(es)
cluster = client.ClusterClient(es)
index_name_list = ["mstp-as-2016.08.23", "mstp-as-2016.08.24", "mstp-as-2016.08.25", "mstp-as-2016.08.26"]

#Event to notify the show thread.
shutdown_event = Event()

# A temp list to store documents which will be send.
docs_to_send = []

# Create specified indices.
def create_indices():
    for index_name in index_name_list:
        es.indices.create(index = index_name, 
                          body ={"settings": {"number_of_shards": number_of_shards,"number_of_replicas": number_of_replicas}})

# Generate documents to a temp list docs_to_send.
def generate_docs(doc_num):
    
    index_name =  random.choice(index_name_list)

    for j in range(0, doc_num):
        doc = {
            "_index": index_name,
            "_type": "mstp",
            "_id": j,
            "_source": {
                        'host': 'JN-STB-TFD-AS01',
                        'servicename': 'mstp-as',
                        'vnum': '1',
                        'service': 'AS',
                        'file': 'ErrorCodeParser.java',
                        'line': 'ErrorCodeParser.java',
                        'level': 'TEST',
                        'bid': '840f8d13-0925-4b17-9a21-4f6add68bcc1',
                        'msg': 'Receive request /server/address/queryappId:4311744518&params:oqFAj3qxi\
                        -WpB-XReiTPHSX9VBGRbLbYQGjE2dznPmLHcQPJOJx3YDavAtUWULNTRd3MPZQiLq0pd9pSnM-zeWSN-LOff2s_up_NgLiAXQqB0DomMYqDpg2COoP1TTyp41UJejN3jIFSmM9cavwyKdgsVkpvq4tyaTRHInT289l_8hZFl9zeCpPKw0h9kLgxGDKkxNkprw6MSYY12OWsGDoX5dKezYSVoSLYmzz-Kuhv445GxZo33rtGc1bT9kNP1IP1XwCKZAwCc1WcGYkj5jU_lxD-yajK_y8lbw9wmeQN1HUY377hiq5VYAFvDrkp-qsipvYmzYZ-ZIPgHCFo9Z_jmS1oUrx3Hf60PeA0bddrxKvkkj2sdrSA-GCPd25HOgLIWhhds0NkU26jV6f8yv96FObLffBnsFOJD5dxGGuZWzqd8B6sdPcCQbhrsvZORKdPrBfRmzGdsLWbLbDMgLBsVYv_VIZp6RDxYSyDF09eNYdmz7fdRuUvzXY9OtLaGpm6AEuc6nXfktMREbJtSA==&secretKey:A1QPVyEUbGbKhaHW-_B_AE9q8OQIx-TI0VrM6P3O7Jj4nPCJ_qtIFJqUasVa2O4_fhpijQCEGBLSQuYhoURi8w3FrwSTUnQSZw6cLaFdy3SmAp1QVRRAlrFvxlHeoIO_aLV377n9zdAxWy7Bwmh26EI8DLcDncwh_zEWoHCLoRjAQSTN0A==',
                        "timestamp": datetime.now()
                }
            }
        docs_to_send.append(doc)
        #print getsizeof(doc)
        
# Send the documents by bulk api.        
def send_docs(doc_num):
    if len(docs_to_send) > 0:
        try:
            helpers.bulk(es, docs_to_send)
        except:
            print 'Send documents error!'

# Delete the specified indices.   
def delete_indices():
    for index_name in index_name_list:
        try:
            es.indices.delete(index= index_name, ignore=[400, 404])
        except:
            print 'Delete indices error!'


# Split the number of documents equally to the threads.      
def split_documents_num(number_of_documents, number_of_threads):
    quotient = number_of_documents / number_of_threads
    remainder = number_of_documents % number_of_threads
    if remainder > 0:
        print [quotient] * number_of_threads
        return [quotient] * (number_of_threads - remainder) + [quotient + 1] * remainder
    else:
        return  [quotient] * number_of_threads
     
# Start the test.  
def start_test():
    
    # Initial steps.
    delete_indices()
    start_time = int(time.time())
    create_indices()
    generate_docs(number_of_documents)
    
    # Start threads to send documents.
    threads_list = []
    for i in range(number_of_threads):
        doc_num_per_thread = split_documents_num(number_of_documents, number_of_threads)
        t = threading.Thread(target = send_docs, name = 'thread_send_docs' + str(i), args = (doc_num_per_thread,))
        t.daemon = True
        t.start() 
        threads_list.append(t)
    
    # Start a thread to show test status.    
    t_show = threading.Thread(target = show_stats, name = 'thread_show_stats', args = (start_time, shutdown_event, refresh_interval))
    t_show.daemon = True
    t_show.start()
    
    # Wait for the finish of send thread.
    for t in  threads_list:
        try:
            while True:
                t.join(2)
                if not t.is_alive():
                    break
        except KeyboardInterrupt:
            print ''
            print "Ctrl-c pressed. Exit!"
            sys.exit(1)
     
    # Set the event to notify the show_stats thread to stop print status.        
    shutdown_event.set()
            

if __name__ == '__main__':
    
    begin_time = time.time()
    start_test()
    end_time = time.time()
    cost_time = end_time - begin_time
    
    print 'cost :',cost_time,'s'
    print 'End test....................'
    