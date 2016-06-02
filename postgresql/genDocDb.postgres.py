#!/usr/bin/env python
# How to use
# >python genDocDb.postgres.py -h localhost -P 5432 -u username -p password -d database -t table -n 1000 -r 1
# library: 
#   pip install psycopg2

import argparse
import psycopg2
import string
import random
import math
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
    "-P", "--port",
    default=5432,
    nargs='?',
    type=int,
    help="Port number to use for connection.")
  parser.add_argument(
    "-u", "--username",
    default=None,
    nargs='?',
    type=str,
    help="Username for login to server.")
  parser.add_argument(
    "-p", "--password",
    default=None,
    nargs='?',
    type=str,
    help="Password for login to server.")
  parser.add_argument(
    "-s", "--size",
    default=None,
    nargs='?',
    type=int,
    help="Payload size in randtext column.")
  parser.add_argument(
    "-d", "--database",
    default=None,
    nargs='?',
    type=str,
    help="Select database.")
  parser.add_argument(
    "-t", "--table",
    default=None,
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
    "-o", "--output",
    default=None,
    nargs='?',
    type=str,
    help="Store key to file")
  parser.add_argument(
    "-U","--update",
    default=False,
    action='store_true',
    help="Update existing tables start from id 1 to n"
  )
  parser.add_argument(
    "--help",
    default=False,
    action='store_true',
    help="Show this help"
  )
  return parser

people = ["Liam", "Hunter", "Connor", "Jack", "Cohen", "Jaxon", "John", "Landon", "Owen", "William", "Benjamin", "Caleb", "Henry", "Lucas", "Mason", "Noah", "Alex", "Alexander", "Carter", "Charlie", "David", "Jackson", "James", "Jase", "Joseph", "Wyatt", "Austin", "Camden", "Cameron", "Emmett", "Griffin", "Harrison", "Hudson", "Jace", "Jonah", "Kingston", "Lincoln", "Marcus", "Nash", "Nathan", "Oliver", "Parker", "Ryan", "Ryder", "Seth", "Xavier", "Charles", "Clark", "Cooper", "Daniel", "Drake", "Dylan", "Edward", "Eli", "Elijah", "Emerson", "Evan", "Felix", "Gabriel", "Gavin", "Gus", "Isaac", "Isaiah", "Jacob", "Jax", "Kai", "Kaiden", "Malcolm", "Michael", "Nathaniel", "Riley", "Sawyer", "Thomas", "Tristan", "Antonio", "Beau", "Beckett", "Brayden", "Bryce", "Caden", "Casey", "Cash", "Chase", "Clarke", "Dawson", "Declan", "Dominic", "Drew", "Elliot", "Elliott", "Ethan", "Ezra", "Gage", "Grayson", "Hayden", "Jaxson", "Jayden","Kole", "Levi", "Logan", "Luke", "Matthew", "Morgan", "Nate", "Nolan", "Peter", "Ryker", "Sebastian", "Simon", "Tanner", "Taylor", "Theo", "Turner", "Ty", "Tye"];
scheme = "public"
defaultdb = "postgres"

def genRandString(y):
  return ''.join(random.choice(string.ascii_letters) for x in range(y))

def genData(n):
  insquery = None
  nameLen = int(random.random()*(5-1))+1
  name = people[int(random.random()*len(people))]
  for j in range(0,nameLen):
    name = name+' '+people[int(random.random()*len(people))]
  bool = int(random.random()*2)
  added_at = "CURRENT_TIMESTAMP"
  zipcode = int(random.random()*100001)
  phone = int(random.random()*100000001)
  if args.size:
    if args.size > 0 and args.size <= 10000: # max 10KB
      randomtext = genRandString(int(args.size))
      if args.update:
        insquery = "UPDATE "+args.database+"."+scheme+"."+args.table+" SET (name,boolean,added_at,zipcode,phone,randtext) = ('"+name+"','"+str(bool)+"','now','"+str(zipcode)+"','"+str(phone)+"','"+randomtext+"') WHERE user_id = '"+str(i+1)+"'";
      else:
        insquery = "INSERT INTO "+args.database+"."+scheme+"."+args.table+" (name,boolean,added_at,zipcode,phone,randtext) VALUES ('"+name+"','"+str(bool)+"','now','"+str(zipcode)+"','"+str(phone)+"','"+randomtext+"')";
    else:
      print "size arg should be 0<size<=10000, exit script"
      sys.exit()
  else:
    if args.update:
        insquery = "UPDATE "+args.database+"."+scheme+"."+args.table+" SET (name,boolean,added_at,zipcode,phone) = ('"+name+"','"+str(bool)+"','now','"+str(zipcode)+"','"+str(phone)+"') WHERE user_id = '"+str(i+1)+"'";     
    else:
      insquery = "INSERT INTO "+args.database+"."+scheme+"."+args.table+" (name,boolean,added_at,zipcode,phone) VALUES ('"+name+"','"+str(bool)+"','now','"+str(zipcode)+"','"+str(phone)+"')";
  #return insquery
  return {
    "query": insquery,
    "keys": [name,bool,added_at,zipcode,phone]
  }

if __name__ == '__main__':
  parser = get_args_parser()
  args = parser.parse_args()
  conn = None
  db = None
  f = None
  if args.help or not args.database or not args.table or not args.username or not args.password :
    parser.print_help()
    parser.exit()
    sys.exit()
  try:
    conn = psycopg2.connect(host=args.host,port=int(args.port),user=args.username,password=args.password,database=defaultdb)
    #print "Connection: "+str(conn.status)
  except Exception, err:
    print err
    sys.exit()
  createdb = "CREATE DATABASE "+args.database
  createtbl = "CREATE TABLE "+args.database+"."+scheme+"."+args.table+" (user_id SERIAL, name text, boolean boolean, added_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(), zipcode numeric(12), phone numeric(12), randtext text);"
  #print createdb
  #print createtbl
  try:
    conn.set_isolation_level(0)
    db = conn.cursor()
    db.execute(createdb)
    conn.close()
  except Exception, err:
    print err
    pass
  else:
    print "Success create database: "+args.database 
  try:
    conn = psycopg2.connect(host=args.host,port=int(args.port),user=args.username,password=args.password,database=args.database)
    db = conn.cursor()
    #print "Connection: "+str(conn.status)
  except Exception, err:
    print err
    sys.exit()
  try:
    db.execute(createtbl)  
    conn.commit()
  except Exception, err:
    conn.commit()
    print err
  if args.output:
    try:
      f = open(args.output,"a")
    except Exception, err:
      print err
      sys.exit()
  # Generate dummy data
  print "Total data will be generated = "+str(args.number)
  t = time.time()
  for i in range(0,int(args.number)):
    query = genData(i)
    #print query
    db.execute(query["query"])
    conn.commit()
    if f:
      f.write(';'.join(map(str,query["keys"]))+"\n")
      f.flush()
    if args.report:
      if time.time() - t > args.report:
        t = time.time()
        print "n data: "+str(i)
  conn.close()
  sys.exit()
