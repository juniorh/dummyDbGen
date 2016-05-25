#!/usr/bin/env python
# How to use
# > python genFileDummy.py -r 1 -d /data/ -n 100000
#   file structure will be like:
#     /data/aa/bb/cc/dd

import argparse
import time
import sys
import os

i = 0xffffffff+1
f = []

def get_args_parser():
  parser = argparse.ArgumentParser(add_help=False)
  parser.add_argument(
    "-n", "--number",
    default=False,
    nargs='?',
    type=int,
    help="Number files is generated (max 2^32)")
  parser.add_argument(
    "-d", "--dir",
    default='.',
    nargs='?',
    type=str,
    help="Directory base")
  parser.add_argument(
    "-r", "--report",
    default=0,
    nargs='?',
    type=int,
    help="Print report every r second")
  parser.add_argument(
    "--help",
    default=False,
    action='store_true',
    help="Show this help")
  return parser

def openfile(n):
  tmpfile = None
  hexstr = format(n,'x')
  level1 = hexstr[1:3]
  level2 = hexstr[3:5]
  level3 = hexstr[5:7]
  level4 = hexstr[7:9]
  path = args.dir+'/'+level1+'/'+level2+'/'+level3
  try:
    tmpfile = open(path+'/'+level4, 'r')
  except:
    print "retry openfile "+str(n-i+1)
    tmpfile = open(path+'/'+level4, 'r')
  return tmpfile  

if __name__ == '__main__':
  parser = get_args_parser()
  args = parser.parse_args()
  if args.help or not args.number or args.number > i:
    parser.print_help()
    parser.exit()
    sys.exit()
  if not os.path.exists(args.dir):
    print "Base directory "+args.dir+" doesn't exist"
    sys.exit()
  t = time.time()
  for j in range(i,i+args.number):
    f.append(openfile(j))
    if args.report:
      if time.time() - t > args.report:
        t = time.time()
        print "open file number: "+str(j-i+1)
  print "open file number: "+str(j-i+1)
  while 1:
    print time.time()
    time.sleep(1)
  sys.exit()
