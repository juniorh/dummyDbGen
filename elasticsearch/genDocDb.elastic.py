#!/usr/bin/env python
# How to use
# >python genDocDb.elastic.py -h localhost -p 9200 -i test -t data1 -n 1000 -r 1

from elasticsearch import Elasticsearch 
import argparse
import random
import datetime
import time
import sys

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
    default=9200,
    nargs='?',
    type=int,
    help="Port number to use for connection.")
  parser.add_argument(
    "-u", "--user",
    default=None,
    nargs='?',
    type=str,
    help="User for authentication if needed.")
  parser.add_argument(
    "-P", "--password",
    default=None,
    nargs='?',
    type=str,
    help="Password for authentication if needed.")
  parser.add_argument(
    "-i", "--index",
    default=None,
    nargs='?',
    type=str,
    help="Select index (similar to database name).")
  parser.add_argument(
    "-t", "--type",
    default=None,
    nargs='?',
    type=str,
    help="Select type (similar to table name)")
  parser.add_argument(
    "-r", "--report",
    default=0,
    nargs='?',
    type=int,
    help="Print report every r second")
  parser.add_argument(
    "-n", "--number",
    default=1,
    nargs='?',
    type=int,
    help="How much point dummies will be generated")
  parser.add_argument(
    "--help",
    default=False,
    action='store_true',
    help="Show this help"
  )
  return parser

def genData():
  people = ["Liam", "Hunter", "Connor", "Jack", "Cohen", "Jaxon", "John", "Landon", "Owen", "William", "Benjamin", "Caleb", "Henry", "Lucas", "Mason", "Noah", "Alex", "Alexander", "Carter", "Charlie", "David", "Jackson", "James", "Jase", "Joseph", "Wyatt", "Austin", "Camden", "Cameron", "Emmett", "Griffin", "Harrison", "Hudson", "Jace", "Jonah", "Kingston", "Lincoln", "Marcus", "Nash", "Nathan", "Oliver", "Parker", "Ryan", "Ryder", "Seth", "Xavier", "Charles", "Clark", "Cooper", "Daniel", "Drake", "Dylan", "Edward", "Eli", "Elijah", "Emerson", "Evan", "Felix", "Gabriel", "Gavin", "Gus", "Isaac", "Isaiah", "Jacob", "Jax", "Kai", "Kaiden", "Malcolm", "Michael", "Nathaniel", "Riley", "Sawyer", "Thomas", "Tristan", "Antonio", "Beau", "Beckett", "Brayden", "Bryce", "Caden", "Casey", "Cash", "Chase", "Clarke", "Dawson", "Declan", "Dominic", "Drew", "Elliot", "Elliott", "Ethan", "Ezra", "Gage", "Grayson", "Hayden", "Jaxson", "Jayden","Kole", "Levi", "Logan", "Luke", "Matthew", "Morgan", "Nate", "Nolan", "Peter", "Ryker", "Sebastian", "Simon", "Tanner", "Taylor", "Theo", "Turner", "Ty", "Tye"]
  ltags = ["tg1","tg2","tg3","tg4","tg5","tg6","tg7","tg8","tg9","tg10","tg11","tg12","tg13","tg14","tg15","tg16","tg17","tg18","tg19","tg20"]
  nameLen = int(random.random()*(5-1))+1
  #name = people[int(random.random()*len(people))]
  nameFirst = people[int(random.random()*len(people))]
  nameLast = ""
  for j in range(0,nameLen):
    nameLast = nameLast+' '+people[int(random.random()*len(people))]
  name = nameFirst+" "+nameLast
  bool = int(random.random()*2)
  zipcode = int(random.random()*100001)
  phone = int(random.random()*100000001)
  value = random.randrange(0,1000)
  tags = []
  t = time.time()
  for j in range(random.randrange(1,10)):
    tags.append(ltags[int(random.random()*len(ltags))])
    if time.time() - t > 1:
      t=time.time()
      print i
  DataOut = {
    "name": name,
    "names":{
      "first": nameFirst,
      "last": nameLast
    },
    "bool": bool, 
    "zipcode": zipcode,
    "phone": phone,
    "value": value,
    "tags": tags,
    "input_time": datetime.datetime.now(),
    "random_time": datetime.datetime.fromtimestamp(random.randrange(limit_t_first,limit_t_last))
  }
  return DataOut

if __name__ == '__main__':
  limit_t_first = time.mktime(datetime.datetime(2015,1,1).timetuple())
  limit_t_last = time.mktime(datetime.datetime(2015,12,31).timetuple())
  parser = get_args_parser()
  args = parser.parse_args()
  conn = None
  es = None
  if args.help or not args.index or not args.type or not args.host:
    parser.print_help()
    parser.exit()
  #check connection to elasticsaerch searver
  auth = None
  if args.user and args.password:
    auth = (args.user, args.password)
  es = Elasticsearch([
    {
      "host": args.host,
      "port": args.port
    }],
    http_auth=auth
  )
  if not es.ping():
    print es.ping()
    print "Can't Connect to Cluster"
    sys.exit()
  else:
    t = time.time()
    for i in range(0,args.number):
      data = genData()
      res = None
      try:
        res = es.index(index=args.index, doc_type=args.type, body=data)
        #print data
        #print res
      except Exception, err:
        print err
        print "last number input i-"+str(i)+" data: "+str(data)
      if args.report:
        if time.time() - t > args.report:
          t = time.time()
          print "rtDummy "+str(i)+" : "+str(res)
  print "Finish generate rethinkdb dummy : "+str(args.number)
  sys.exit()

