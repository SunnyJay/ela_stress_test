#-*- coding:utf-8 -*-
'''
Created on 2016-09-14
Elasticsearch Stress Test Tool
@author: Administrator
'''
import os
import time

from elasticsearch import Elasticsearch
from elasticsearch import client
from threading import Condition


es = Elasticsearch(["10.4.200.23", "10.4.200.24", "10.4.200.25"], maxsize=25)
nodes = client.NodesClient(es)
cluster = client.ClusterClient(es)
thread_num = 0


def fetch_nodes_num():
    ret = cluster.health()
    nodes_num = ret['number_of_nodes']
    return nodes_num
    
def fetch_nodes_name_list():
    ret = cluster.state()
    name_list = []
    for value in ret['nodes'].values():
        name_list.append(value['name'])
    return sorted(name_list)
    
def fetch_stats(node_name):
    ret =  nodes.stats(node_name, human = True)
    
    node_id = ret['nodes'].keys()[0]
    host_name = ret['nodes'][node_id]['name']
    ip = ret['nodes'][node_id]['ip'][0].split(':')[0].split('.')[3]
    doc_num = ret['nodes'][node_id]['indices']['docs']['count']
    cpu = ret['nodes'][node_id]['os']['cpu_percent']
    mem = ret['nodes'][node_id]['jvm']['mem']['heap_used_percent']
    load = ret['nodes'][node_id]['os']['load_average']
    y_gc = ret['nodes'][node_id]['jvm']['gc']['collectors']['young']['collection_count']
    o_gc = ret['nodes'][node_id]['jvm']['gc']['collectors']['old']['collection_count']
    rejected = ret['nodes'][node_id]['thread_pool']['bulk']['rejected']
    
    ret = cluster.stats()    
    index_num = ret['indices']['count']
    total_doc = ret['indices']['docs']['count']
    cluster_status = ret['status']
    
    return host_name,ip,cpu,mem,load,y_gc,o_gc,doc_num,index_num,rejected,cluster_status,total_doc

def print_stats(node_name_list, start_time, shutdown_event, refresh_interval):
    # Create a conditional lock to be used instead of sleep (prevent dead locks)
    lock = Condition()

    # Acquire it
    lock.acquire()
    
    nodes_num= len(node_name_list)
    old_num = [0]*nodes_num
    inc = [0]*nodes_num
    max = 0
    j = 0
    
    avg_array = []
    sum = 0

    os.system("cls")
    
    while not shutdown_event.is_set():
        
        print ''
        print 'refresh time:',refresh_interval,'s'
        print 'number_of_documents:',refresh_interval,'s'
        print 'number_of_threads:',refresh_interval,'s'
        print 'number_of_shards :',refresh_interval,'s'
        print 'number_of_replicas:',refresh_interval,'s'
        
        print ''
        total_inc = 0
        i = 0
        print 'host\tip\tcpu\tmem\tload\ty_gc\to_gc\tdoc\trejected\tdoc_inc(/s)'
        for node_name in node_name_list:
            host_name,ip,cpu,mem,load,y_gc,o_gc,doc_num,index_num,rejected,cluster_status,total_doc = fetch_stats(node_name)
            inc[i] = doc_num - old_num[i]  
            old_num[i] = doc_num
            if inc[i] >= 0:
                inc[i] = '+' + str(inc[i])
            print host_name,'\t',ip,'\t',cpu,'\t',mem,'\t',load,'\t',y_gc,'\t',o_gc,'\t',doc_num,'\t',rejected,'\t\t',inc[i]
            if j == 0:
                total_inc = 0
            else:
                total_inc = total_inc + int(inc[i])
            i = i + 1
            
        if total_inc > max:
                max = total_inc
        
        print ''        
        print 'cluster status:', cluster_status
        print 'indices num:', index_num
        print 'docs num:', total_doc
        past_time = int(time.time()- start_time)
        
        print ''
        print 'past time:', past_time
        
        sum = sum + total_inc/2
        avg = sum / past_time
        avg_array.append(avg) 
        
        print 'indexing rate(/s): average(%d) current(%d) max(%d)' % (total_inc/2/refresh_interval, avg, max/2/refresh_interval)
        
        print ''
        print 'average rate array:'
        for avg in avg_array:
            print avg,
        
        lock.wait(refresh_interval)
        
        os.system("cls") #In linux is 'clear'.
        j = j + 1
    
def show_stats(start_time,shutdown_event, refresh_interval):
    print_stats(fetch_nodes_name_list(), start_time,shutdown_event,  refresh_interval)