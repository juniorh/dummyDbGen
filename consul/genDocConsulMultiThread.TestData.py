#!/usr/bin/env python
# How to use
# > python genDocConsulMultiThread.TestData.py -h localhost -p 8500  -d /a/b/c -n 100 -t 4
#   file structure is like:
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

def reqData(n):
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
  HTTPVerb = "GET"
  conn = httplib.HTTPConnection(args.host,int(args.port))
  conn.putrequest(HTTPVerb, path+"?raw")
  conn.putheader("User-Agent", h['User-Agent'])
  conn.putheader("Accept", h['Accept'])
  conn.endheaders()
  res = conn.getresponse()
  rescode = res.status
  if rescode != 200:
    print "Error "+str(rescode)+" insert to path "+path
    #pass
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
  print "file number will be test: "+str(args.number)
  pool = mp.Pool(processes=args.thread) 
  pool.map(reqData,range(0,args.number))
  pool.close()
  print "file number tested "+str(args.number)
  print 'time: '+str(time.time()-t)+'s'
  sys.exit()
