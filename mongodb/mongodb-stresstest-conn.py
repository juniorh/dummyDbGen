#!/usr/bin/env python
#  e.g
#    python mongodb-stresstest-conn.py -h mongo1,mongo2,mongo3 -R cluster1 -d dbtest -c coltest1 -n 10 -r 2 

import sys                              
import time                        
import random           
import thread
import datetime
import argparse
from pymongo import MongoClient                                                             
def get_args_parser():                         
  parser = argparse.ArgumentParser(add_help=False)
  parser.add_argument(                          
    "-h", "--host", 
    default="localhost",                                            
    nargs='?',    
    type=str,
    help="Connect to host.")
  parser.add_argument(      
    "-p", "--port",         
    default=27017,                                          
    nargs='?',         
    type=int,    
    help="Port number to use for connection.")
  parser.add_argument(
    "-d", "--database",
    default=None, 
    nargs='?',           
    type=str,                
    help="Select database.")
  parser.add_argument(                                               
    "-R", "--replicaset",                           
    default=None,
    nargs='?', 
    type=str,
    help="Replica Set name.")
  parser.add_argument(
    "-c", "--collection",
    default=None,
    nargs='?',                                    
    type=str,
    help="Select collection")
  parser.add_argument(
    "-n", "--number",                 
    default=1,
    nargs='?',
    type=int,
    help="How much connection created")
  parser.add_argument(
    "-r", "--rate",
    default=10,
    nargs='?',                 
    type=int,         
    help="Creating connection rate (r connection/second)")
  parser.add_argument(
    "-v", "--verbose",  
    default=False,       
    action='store_true',                          
    help="Show queries output")
  parser.add_argument(
    "--help",                       
    default=False,                                 
    action='store_true',                                                        
    help="Show this help"                                                       
  )                                  
  return parser                                                        
                                                                                                    
def startProcess(threadName, delay):
  print "Try to create thread - " + str(threadName)
  try:                             
    if args.replicaset: 
      print "Connect in ReplSet mode"
      listHosts = args.host.split(',')
      conn = MongoClient(listHosts, replicaset=args.replicaset, readPreference='secondaryPreferred')
    else:                                                                                   
      print "Connect in standalone mode"       
      conn = MongoClient(args.host)               
  except Exception, err:                        
    print err       
    sys.exit()                                                      
  while True:     
    #print "exec queries thread - " + str(threadName) + " - " + str(datetime.datetime.now())
    coll = conn[args.database][args.collection]
    zipcode = int(random.random()*100001)
    result = coll.find_one({"zipcode": zipcode})
    if args.verbose:                                        
      print "exec thread - " + str(threadName) + " - " + str(result)
    time.sleep(10)
                                              
if __name__ == '__main__':
  parser = get_args_parser()
  args = parser.parse_args()
  if args.help or not args.database or not args.collection :
    parser.print_help()      
    parser.exit()           
    sys.exit()                                                       
  i = 0                                             
  j = 0          
  t1 = time.time()
  while i < args.number :
    if (time.time()-t1 < 1) :
      if j < args.rate :
        #print "conn: " + str(i) + "\t - " + str(t1) + " - " + str(j)
        thread.start_new_thread(startProcess, (i,i))
        i = i+1                                   
        j = j+1
      else:                  
        time.sleep(0.1)
    else:                             
      t1 = time.time()
      j = 0   
      print "created connection thread: " + str(i)
  while 1:                             
    pass 
