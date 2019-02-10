#!/usr/bin/env python
# pip install redis-py-cluster
# How to use
# >python genDocDb.redis.py -h localhost -p 6379

import argparse
import datetime
import string
import random
import redis
import uuid
import time
import sys
from rediscluster import StrictRedisCluster

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
    default=6379,
    nargs='?',
    type=int,
    help="Port number to use for connection.")
  parser.add_argument(
    "-r", "--report",
    default=0,
    nargs='?',
    type=int,
    help="Print report every r second") 
  parser.add_argument(
    "-d", "--db",
    default=0,
    nargs='?',
    type=int,
    help="Redis db number")
  parser.add_argument(
    "-k", "--key",
    default="dummy",
    nargs='?',
    type=str,
    help="key name")
  parser.add_argument(
    "-n", "--number",
    default=1,
    nargs='?',
    type=int,
    help="How much point dummies will be generated")
  parser.add_argument(
    "-o", "--output",
    default=None,
    nargs='?',
    type=str,
    help="Store key to file")
  parser.add_argument(
    "-s", "--size",
    default=None,
    nargs='?',
    type=int,
    help="Payload size in randtext column, max 10000")
  parser.add_argument(
    "-c", "--cluster",
    default=False,
    nargs='?',
    type=bool,
    help="Cluster mode connection")
  parser.add_argument(
    "--help",
    default=False,
    action='store_true',
    help="Show this help"
  )
  return parser

def genRandString(y):
  return ''.join(random.choice(string.ascii_letters) for x in range(y))

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
  randomtext = None
  if args.size:
    if args.size > 0 and args.size <= 10000: # max 10KB
      randomtext = genRandString(int(args.size))
    else:
      print "size arg should be 0<size<=10000, exit script"
      sys.exit()
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
  if randomtext:
    DataOut['randtext'] = randomtext
  return DataOut

if __name__ == '__main__':
  r = None
  f = None
  w_ok = 0
  w_fail = 0
  t_start = None
  limit_t_first = time.mktime(datetime.datetime(2015,1,1).timetuple())
  limit_t_last = time.mktime(datetime.datetime(2015,12,31).timetuple())
  parser = get_args_parser()
  args = parser.parse_args()
  print args
  if args.help:
    parser.print_help()
    parser.exit()
  try:
    if args.cluster:
      nodes = [{"host": args.host, "port": str(args.port)}]
      r = StrictRedisCluster(startup_nodes=nodes, decode_responses=True)
    else:
      r = redis.StrictRedis(host=str(args.host), port=str(args.port), db=args.db)
  except Exception, err:
    print err
    sys.exit()
  # Open File for storing keys
  if args.output:
    try:
      f = open(args.output,"a")
    except Exception, err:
      print err
      sys.exit()
  # Generate dummy data
  t = time.time()
  t_start = t
  key = args.key
  for i in range(0,args.number):
    #key = "data-"+str(uuid.uuid1())
    value = genData()
    try:
      res = r.lpush(key,value)
      if res:
        w_ok = w_ok+1
      else:
        w_fail = w_fail+1
      #print res
      #print data
      #print val
    except Exception, err:
      print err
      print "Failed connected to Redis, last number input i-"+str(i+1)+" data: "+str(value)
      w_fail = w_fail+1
    if f:
      f.write(key+"\n")
      f.flush()
    if args.report:
      if time.time() - t > args.report:
        t = time.time()
        print "rtDummy "+str(i+1)+" : "+str(res)+" w_ok:"+str(w_ok)+" w_fail:"+str(w_fail)
  print "Finish generate memcached dummy : "+str(args.number)+" w_ok:"+str(w_ok)+" w_fail:"+str(w_fail)+" time:"+str(time.time()-t_start)
  sys.exit()
