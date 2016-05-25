#!/usr/bin/env python
# How to use
# > python genDocDb.consul.py -h localhost -p 8500  -d /a/b/c -n 100 -t 4
#   file structure will be like:
#     /data/aa/bb/cc/dd

import multiprocessing as mp
import datetime
import argparse
import httplib
import random
import string
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
    default=8500,
    nargs='?',
    type=int,
    help="Port number to use for connection.")
  parser.add_argument(
    "-n", "--number",
    default=False,
    nargs='?',
    type=int,
    help="Number files is generated (max 2^32)")
  parser.add_argument(
    "-dir", "--dir",
    default='/dummy/',
    nargs='?',
    type=str,
    help="Base key / directory")
  parser.add_argument(
    "-t", "--thread",
    default=1,
    nargs='?',
    type=int,
    help="Total thread spawned")
  parser.add_argument(
    "--help",
    default=False,
    action='store_true',
    help="Show this help")
  return parser

def genRandString(y):
  return ''.join(random.choice(string.ascii_letters) for x in range(y))

def genData():
  people = ["Liam", "Hunter", "Connor", "Jack", "Cohen", "Jaxon", "John", "Landon", "Owen", "William", "Benjamin", "Caleb", "Henry", "Lucas", "Mason", "Noah", "Alex", "Alexander", "Carter", "Charlie", "David", "Jackson", "James", "Jase", "Joseph", "Wyatt", "Austin", "Camden", "Cameron", "Emmett", "Griffin", "Harrison", "Hudson", "Jace", "Jonah", "Kingston", "Lincoln", "Marcus", "Nash", "Nathan", "Oliver", "Parker", "Ryan", "Ryder", "Seth", "Xavier", "Charles", "Clark", "Cooper", "Daniel", "Drake", "Dylan", "Edward", "Eli", "Elijah", "Emerson", "Evan", "Felix", "Gabriel", "Gavin", "Gus", "Isaac", "Isaiah", "Jacob", "Jax", "Kai", "Kaiden", "Malcolm", "Michael", "Nathaniel", "Riley", "Sawyer", "Thomas", "Tristan", "Antonio", "Beau", "Beckett", "Brayden", "Bryce", "Caden", "Casey", "Cash", "Chase", "Clarke", "Dawson", "Declan", "Dominic", "Drew", "Elliot", "Elliott", "Ethan", "Ezra", "Gage", "Grayson", "Hayden", "Jaxson", "Jayden","Kole", "Levi", "Logan", "Luke", "Matthew", "Morgan", "Nate", "Nolan", "Peter", "Ryker", "Sebastian", "Simon", "Tanner", "Taylor", "Theo", "Turner", "Ty", "Tye"]
  ltags = ["tg1","tg2","tg3","tg4","tg5","tg6","tg7","tg8","tg9","tg10","tg11","tg12","tg13","tg14","tg15","tg16","tg17","tg18","tg19","tg20"]
  nameLen = int(random.random()*(5-1))+1
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
    "input_time": str(datetime.datetime.now()),
    "random" : {
      "time": str(datetime.datetime.fromtimestamp(random.randrange(limit_t_first,limit_t_last))),
      "string": genRandString(random.randrange(1000,5000))
    }
  }
  return str(DataOut)

def sendData(n):
  data = genData()
  pathDirSplit = args.dir.split('/')
  path = "/v1/kv"
  for x in pathDirSplit:
    if x:
      path = path+'/'+x
  hexstr = format(n,'x')
  nSplit = 2
  pathKeySplit = [hexstr[i:i+nSplit] for i in range(0, len(hexstr), nSplit)]
  for x in pathKeySplit:
    if x:
      path = path+'/'+x
  h = {
    'User-Agent': 'curl/7.26.0 genData/0.1',
    'Accept': '*/*',
  }
  HTTPVerb = "PUT"
  conn = httplib.HTTPConnection(args.host,int(args.port))
  conn.putrequest(HTTPVerb, path)
  conn.putheader("User-Agent", h['User-Agent'])
  conn.putheader("Accept", h['Accept'])
  conn.putheader("Content-Length", str(len(data)))
  conn.endheaders(data)
  res = conn.getresponse()
  rescode = res.status
  if rescode != 200:
    # print "Error path "+path
    pass
  res.close()
  conn.close()
  return rescode

if __name__ == '__main__':
  w_ok = 0
  w_fail = 0
  t = time.time()
  parser = get_args_parser()
  args = parser.parse_args()
  if args.help or not args.number or not args.number:
    parser.print_help()
    parser.exit()
    sys.exit()
  limit_t_first = time.mktime(datetime.datetime(2016,1,1).timetuple())
  limit_t_last = time.mktime(datetime.datetime(2016,12,31).timetuple())
  print "file number will be generated: "+str(args.number)
  pool = mp.Pool(processes=args.thread) 
  pool.map(sendData,range(0,args.number))
  pool.close()
  print "file number will generated: "+str(args.number)
  print 'time: '+str(time.time()-t)+'s'
  sys.exit()
