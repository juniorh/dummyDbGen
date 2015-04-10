#!/usr/bin/env python
# How to use
# >python genDocDb.rethink.py -h localhost -p 28015 -a authkey -d database -t table -n 1000 -r 1

import rethinkdb as r
import argparse
import random
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
    default=28015,
    nargs='?',
    type=int,
    help="Port number to use for connection.")
  parser.add_argument(
    "-a", "--auth",
    default="",
    nargs='?',
    type=str,
    help="Auth key to use when connecting to server.")
  parser.add_argument(
    "-d", "--database",
    default='test',
    nargs='?',
    type=str,
    help="Select database.")
  parser.add_argument(
    "-t", "--table",
    default='testing',
    nargs='?',
    type=str,
    help="Select table")
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
  ppeople = ["Liam", "Hunter", "Connor", "Jack", "Cohen", "Jaxon", "John", "Landon", "Owen", "William", "Benjamin", "Caleb", "Henry", "Lucas", "Mason", "Noah", "Alex", "Alexander", "Carter", "Charlie", "David", "Jackson", "James", "Jase", "Joseph", "Wyatt", "Austin", "Camden", "Cameron", "Emmett", "Griffin", "Harrison", "Hudson", "Jace", "Jonah", "Kingston", "Lincoln", "Marcus", "Nash", "Nathan", "Oliver", "Parker", "Ryan", "Ryder", "Seth", "Xavier", "Charles", "Clark", "Cooper", "Daniel", "Drake", "Dylan", "Edward", "Eli", "Elijah", "Emerson", "Evan", "Felix", "Gabriel", "Gavin", "Gus", "Isaac", "Isaiah", "Jacob", "Jax", "Kai", "Kaiden", "Malcolm", "Michael", "Nathaniel", "Riley", "Sawyer", "Thomas", "Tristan", "Antonio", "Beau", "Beckett", "Brayden", "Bryce", "Caden", "Casey", "Cash", "Chase", "Clarke", "Dawson", "Declan", "Dominic", "Drew", "Elliot", "Elliott", "Ethan", "Ezra", "Gage", "Grayson", "Hayden", "Jaxson", "Jayden","Kole", "Levi", "Logan", "Luke", "Matthew", "Morgan", "Nate", "Nolan", "Peter", "Ryker", "Sebastian", "Simon", "Tanner", "Taylor", "Theo", "Turner", "Ty", "Tye"];
  ltags = ["tg1","tg2","tg3","tg4","tg5","tg6","tg7","tg8","tg9","tg10","tg11","tg12","tg13","tg14","tg15","tg16","tg17","tg18","tg19","tg20"]
  nameLen = int(random.random()*(5-1))+1
  name = people[int(random.random()*len(people))]
  for j in range(0,nameLen):
    name = name+' '+people[int(random.random()*len(people))]
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
    "bool": bool, 
    "zipcode": zipcode,
    "phone": phone,
    "value": value,
    "tags": tags
  }
  return DataOut

if __name__ == '__main__':
  parser = get_args_parser()
  args = parser.parse_args()
  conn = None
  if args.help:
    parser.print_help()
    parser.exit()
  try:
    #check connection to rethinkdb server
    conn = r.connect(host=args.host,port=args.port,auth_key=args.auth)
    conn.use(args.database)
  except Exception, err:
    print err
    sys.exit()
  try:
    #check db exist
    r.db(args.database).config().run(conn) 
  except Exception, err:
    print err
    r.db_create(args.database).run(conn)
    print "Create database: "+args.database
  try:
    #check table exist
    r.table(args.table).config().run(conn) 
  except Exception, err:
    print err
    r.table_create(args.table).run(conn)
    print "Create table: "+args.table
  print "Generate rethinkdb dummy : "+str(args.number)
  t = time.time()
  for i in range(0,args.number):
    data = genData()
    try:
      r.table(args.table).insert(data).run(conn)
    except Exception, err:
      print err
      print "last number input i-"+str(i)
    if args.report:
      if time.time() - t > args.report:
        t = time.time()
        print "rtDummy "+str(i)
  print "Finish generate rethinkdb dummy : "+str(args.number)
  sys.exit()

