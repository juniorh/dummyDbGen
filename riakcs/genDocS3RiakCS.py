#!/usr/bin/env python
# How to use
# > python dummyFileGenerator.py -r 1 -d /data/ -n 100000
#   file structure will be like:
#     /data/aa/bb/cc/dd

import multiprocessing as mp
import argparse
import time
import sys
import os

i = 0xffffffff+1
t = time.time()
t1 = [time.time()]

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
    help="Directory base including bucket")
  parser.add_argument(
    "-f", "--file",
    default=None,
    nargs='?',
    type=str,
    help="Dummy file is uploaded")
  parser.add_argument(
    "-c", "--config",
    default=None,
    nargs='?',
    type=str,
    help="File s3cfg config")
  parser.add_argument(
    "-r", "--report",
    default=0,
    nargs='?',
    type=int,
    help="Print report every r second")
  parser.add_argument(
    "--parallel",
    default=1,
    nargs='?',
    type=int,
    help="Print report every r second")
  parser.add_argument(
    "--help",
    default=False,
    action='store_true',
    help="Show this help")
  return parser

def uploadfile(n):
  tmpCmd = None
  hexstr = format(n,'x')
  level1 = hexstr[1:3]
  level2 = hexstr[3:5]
  level3 = hexstr[5:7]
  level4 = hexstr[7:9]
  dirSplit = args.dir.split('/')
  path = ''
  for x in dirSplit:
    if x:
      path = path+'/'+x     
  path = path+'/'+level1+'/'+level2+'/'+level3
  try:
    tmpCmd = 's3cmd '
    if args.config:
      tmpCmd = tmpCmd+'-c '+args.config
    tmpCmd = tmpCmd+' put '+args.file+' s3:/'+path+'/'+level4+' 2>/dev/null'
    runCmd = os.popen(tmpCmd)
  except Exception, err:
    print err
    sys.exit()    
  if not runCmd.read():
    print 'Gagal '+str(n - 0xffffffff + 1)
    sys.stdout.flush()

def runThread(num):
  uploadfile(num)
  #if time.time() - t1[-1] > args.report:
  #  t1.pop()
  #  t1.append(time.time())
  #  print "file number: "+str(num-i+1)
  #  sys.stdout.flush()

if __name__ == '__main__':
  parser = get_args_parser()
  args = parser.parse_args()
  if args.help or not args.number or args.number > i:
    parser.print_help()
    parser.exit()
    sys.exit()
  pool = mp.Pool(processes=args.parallel) 
  t1.append(time.time())
  pool.map(runThread,range(i,i+args.number))
  #pool.apply([runThread(x) for x in range(i,i+args.number)])
  pool.close()
  print "file number will generated: "+str(args.number)
  print 'time: '+str(time.time()-t)+'s'
  sys.exit()
