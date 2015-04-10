#!/usr/bin/env python
# How to use
# >python genDocDb.memcache.py -h localhost -p 22222

import argparse
import memcache
import datetime
import random
import uuid
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
    default=11211,
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
    "-i", "--input",
    default=None,
    nargs='?',
    type=str,
    help="File contain keys")
  parser.add_argument(
    "--help",
    default=False,
    action='store_true',
    help="Show this help"
  )
  return parser

if __name__ == '__main__':
  f = None
  t_start = None
  r_ok = 0
  r_fail = 0
  parser = get_args_parser()
  args = parser.parse_args()
  print args
  if args.help:
    parser.print_help()
    parser.exit()
  try:
    mc = memcache.Client([str(args.host)+":"+str(args.port)])
  except Exception, err:
    print err
    sys.exit()
  # Open File for storing keys
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
    key = f.readline()
    if not key:
      break
    value = mc.get(key[:-1])
    if value:
      r_ok = r_ok+1
    else:
      r_fail = r_fail+1
    if args.report:
      if time.time() - t > args.report:
        t = time.time()
        print "r_ok:"+str(r_ok)+" r_fail:"+str(r_fail)+" current_value:"+str(value)
  print "Last_value:"+str(value)+"\n"
  print "Finish test read from memcached : "+"r_ok:"+str(r_ok)+" r_fail:"+str(r_fail)+" time:"+str(time.time()-t_start)
  sys.exit()
