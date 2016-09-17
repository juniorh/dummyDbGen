#!/usr/bin/env python
# How to use
# >python genDocDb.mysql.py -h localhost -P 3306 -u username -p password -d database -t table -n 1000 -r 1

import argparse
import random
import _mysql
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
    default=3306,
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
    "-e", "--engine",
    default="InnoDB",
    nargs='?',
    type=str,
    help="Table engine, default is InnoDB. Ignore if table is existed")
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

people = ["Liam", "Hunter", "Connor", "Jack", "Cohen", "Jaxon", "John", "Landon", "Owen", "William", "Benjamin", "Caleb", "Henry", "Lucas", "Mason", "Noah", "Alex", "Alexander", "Carter", "Charlie", "David", "Jackson", "James", "Jase", "Joseph", "Wyatt", "Austin", "Camden", "Cameron", "Emmett", "Griffin", "Harrison", "Hudson", "Jace", "Jonah", "Kingston", "Lincoln", "Marcus", "Nash", "Nathan", "Oliver", "Parker", "Ryan", "Ryder", "Seth", "Xavier", "Charles", "Clark", "Cooper", "Daniel", "Drake", "Dylan", "Edward", "Eli", "Elijah", "Emerson", "Evan", "Felix", "Gabriel", "Gavin", "Gus", "Isaac", "Isaiah", "Jacob", "Jax", "Kai", "Kaiden", "Malcolm", "Michael", "Nathaniel", "Riley", "Sawyer", "Thomas", "Tristan", "Antonio", "Beau", "Beckett", "Brayden", "Bryce", "Caden", "Casey", "Cash", "Chase", "Clarke", "Dawson", "Declan", "Dominic", "Drew", "Elliot", "Elliott", "Ethan", "Ezra", "Gage", "Grayson", "Hayden", "Jaxson", "Jayden","Kole", "Levi", "Logan", "Luke", "Matthew", "Morgan", "Nate", "Nolan", "Peter", "Ryker", "Sebastian", "Simon", "Tanner", "Taylor", "Theo", "Turner", "Ty", "Tye"];

def genData():
  nameLen = int(random.random()*(5-1))+1
  name = people[int(random.random()*len(people))]
  for j in range(0,nameLen):
    name = name+' '+people[int(random.random()*len(people))]
  bool = int(random.random()*2)
  added_at = "CURRENT_TIMESTAMP"
  zipcode = int(random.random()*100001)
  phone = int(random.random()*100000001)
  insquery = "INSERT INTO "+args.database+"."+args.table+" (user_id,name,boolean,added_at,zipcode,phone) VALUES (NULL, '"+name+"',"+str(bool)+",CURRENT_TIMESTAMP,"+str(zipcode)+","+str(phone)+")";
  return insquery

if __name__ == '__main__':
  parser = get_args_parser()
  args = parser.parse_args()
  conn = None
  db = None
  if args.help or not args.database or not args.table or not args.username or not args.password :
    parser.print_help()
    parser.exit()
    sys.exit()
  try:
    db = _mysql.connect(host=args.host,port=int(args.port),user=args.username,passwd=args.password)
    print db.stat()
  except Exception, err:
    print err
    sys.exit()
  createdb = "CREATE DATABASE IF NOT EXISTS "+args.database
  createtbl = "CREATE TABLE IF NOT EXISTS "+args.database+"."+args.table+" (`user_id` int(12) NOT NULL AUTO_INCREMENT, `name` varchar(128) NOT NULL, `boolean` tinyint(1) NOT NULL, `added_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, `zipcode` int(8) NOT NULL, `phone` int(12) NOT NULL, UNIQUE KEY `user_id` (`user_id`)) ENGINE="+args.engine+"  DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 "
  db.query(createdb)
  db.query(createtbl)  
  print "Total data will be generated = "+str(args.number)
  t = time.time()
  for i in range(0,int(args.number)):
    query = genData()
    #print query
    db.query(query)
    if args.report:
      if time.time() - t > args.report:
        t = time.time()
        print "n data: "+str(i)

