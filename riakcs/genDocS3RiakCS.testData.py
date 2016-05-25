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

import time
import httplib
import sha, hmac, base64, urllib

i = 0xffffffff+1
#t = time.time()
t1 = [time.time()]
w_ok = 0
w_fail = 0

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
    default=8080,
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
    "-d", "--dir",
    default='.',
    nargs='?',
    type=str,
    help="Directory base including bucket")
  parser.add_argument(
    "-k", "--key",
    default="IEOYC6IAAN4RBW4JUN-S",
    nargs='?',
    type=str,
    help="Key id")
  parser.add_argument(
    "-s", "--secret",
    default="UuB4kNVv7oXAosF_ejU6s_1Sdqw56qfCUNOzfQ==",
    nargs='?',
    type=str,
    help="Key secret")
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
    help="Num threads")
  parser.add_argument(
    "--help",
    default=False,
    action='store_true',
    help="Show this help")
  return parser

def cekFileS3(n):
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
  path = path+'/'+level1+'/'+level2+'/'+level3+'/'+level4
  AWSAccessKeyId = args.key
  AWSSecretAccessKey = args.secret
  Expires = str(time.strftime("%b, %d %b %Y %H:%M:%S +0000", time.gmtime()))
  HTTPVerb = "GET"
  ContentMD5 = ""
  ContentType = ""
  CanonicalizedAmzHeaders = ""
  CanonicalizedResource = path
  string_to_sign = HTTPVerb + "\n" +  ContentMD5 + "\n" +  ContentType + "\n" + str(Expires) + "\n" + CanonicalizedAmzHeaders + CanonicalizedResource
  sig = base64.b64encode(hmac.new(AWSSecretAccessKey, string_to_sign, sha).digest())
  auth = 'AWS '+AWSAccessKeyId+':'+sig
  h = {
    'User-Agent': 'curl/7.26.0',
    'Host': 's3.amazonaws.com:8080',
    'Accept': '*/*',
    'Date': str(time.strftime("%b, %d %b %Y %H:%M:%S +0000", time.gmtime())),
    'Authorization': 'AWS '+AWSAccessKeyId+':'+sig,
    'Accept-Encoding': None
  }
  conn = httplib.HTTPConnection(args.host,int(args.port))
  conn.putrequest(HTTPVerb, path, skip_host=True, skip_accept_encoding=True)
  conn.putheader("User-Agent", h['User-Agent'])
  conn.putheader("Host", h['Host'])
  conn.putheader("Accept", h['Accept'])
  conn.putheader("Date", h['Date'])
  conn.putheader("Authorization", h['Authorization'])
  conn.endheaders()
  res = conn.getresponse()
  rescode = res.status
  if rescode != 200:
    ##print "Error path "+path
    pass
  res.close()
  conn.close()
  return rescode

def runThread(num):
  cekFileS3(num)

if __name__ == '__main__':
  parser = get_args_parser()
  args = parser.parse_args()
  if args.help or not args.number or args.number > i:
    parser.print_help()
    parser.exit()
    sys.exit()
  t1.append(time.time())
  t = time.time()
  for n in range(i,i+args.number):
    rc = cekFileS3(n)
    if rc == 200:
      w_ok = w_ok+1
    else:
      w_fail = w_fail+1
    if args.report > 0:
      if (time.time() - t) > args.report:
        print "rtDummy "+str(i+1)+" :  w_ok:"+str(w_ok)+" w_fail:"+str(w_fail)
        t = time.time()
  print "file number will generated: "+str(args.number)
  print 'time: '+str(time.time()-t)+'s'
  print "rtDummy "+str(i+1)+" :  w_ok:"+str(w_ok)+" w_fail:"+str(w_fail)
  sys.exit()
