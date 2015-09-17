#!/usr/bin/env python
# How to use
# >python genDocDb.postgres.py -h localhost -P 5432 -u username -p password -d database -t table -n 1000 -r 1
# library: 
#   pip install psycopg2

import argparse
import psycopg2
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
    "-i", "--input",
    default=None,
    nargs='?',
    type=str,
    help="Store key to file")
  parser.add_argument(
    "-v", "--verbose",
    default=False,
    action='store_true',
    help="Verbose query")
  parser.add_argument(
    "--help",
    default=False,
    action='store_true',
    help="Show this help"
  )
  return parser

scheme = "public"
defaultdb = "postgres"

if __name__ == '__main__':
  parser = get_args_parser()
  args = parser.parse_args()
  conn = None
  db = None
  t_start = None
  r_ok = 0
  r_fail = 0
  r_multi = 0
  f = None
  if args.help or not args.database or not args.table or not args.username or not args.password :
    parser.print_help()
    parser.exit()
    sys.exit()
  try:
    conn = psycopg2.connect(host=args.host,port=int(args.port),user=args.username,password=args.password,database=args.database)
    db = conn.cursor()
    #print "Connection: "+str(conn.status)
  except Exception, err:
    print err
    sys.exit()
  if args.input:
    try:
      f = open(args.input,"r")
    except Exception, err:
      print err
      sys.exit()
  # Generate dummy data
  t = time.time()
  t_start = t
  while True:
    line = f.readline()
    if not line:
      break
    keys = line.split(';')
    query = "select * from "+args.table+" where name = '"+keys[0]+"';"
    # print query
    db.execute(query)
    res = db.fetchall()
    if args.verbose:
      print query
      print res
    if len(res):
      r_ok = r_ok+1
      if len(res) > 1:
        r_multi = r_multi+1
    else:
      r_fail = r_fail+1
    if args.report:
      if time.time() - t > args.report:
        t = time.time()
        print "r_ok:"+str(r_ok)+" r_fail:"+str(r_fail)+" r_multi:"+str(r_multi)+" current_value:"+str(res)
  conn.close()
  print "Last_value:"+str(res)+"\n"
  print "Finish test read from postgres : "+"r_ok:"+str(r_ok)+" r_fail:"+str(r_fail)+" r_multi:"+str(r_multi)+" time:"+str(time.time()-t_start)
