#!/usr/bin/env python
# How to use
# >python genDocDb.influx.py -h localhost -P 8086 -u root -p password -d database -m measurement -n 1000 -t 1 -r default
import datetime
import logging
import argparse
import random
import time
import sys
from influxdb import InfluxDBClient

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
    default=8086,
    nargs='?',
    type=int,
    help="Port number to use for connection.")
  parser.add_argument(
    "-u", "--user",
    default=None,
    nargs='?',
    type=str,
    help="User for login if not current user.")
  parser.add_argument(
    "-p", "--password",
    default='',
    nargs='?',
    type=str,
    help="Password to use when connecting to server.")
  parser.add_argument(
    "-d", "--database",
    default=None,
    nargs='?',
    type=str,
    help="Select variable")
  parser.add_argument(
    "-xd", "--no-create-database",
    default=False,
    action='store_true',
    help="Don't create database")
  parser.add_argument(
    "-m", "--measurement",
    default=None,
    nargs='?',
    type=str,
    help="Select measurement")
  parser.add_argument(
    "-r", "--retention",
    default=None,
    nargs='?',
    type=str,
    help="Select retention policy")
  parser.add_argument(
    "--help",
    default=False,
    action='store_true',
    help="Show this help")
  parser.add_argument(
    "-v","--verbose",
    default=False,
    action='store_true',
    help="Print progress each 1s")
  parser.add_argument(
    "--debug",
    default=False,
    action='store_true',
    help="Print all data generated")
  parser.add_argument(
    "-n", "--number",
    default='1',
    nargs='?',
    type=int,
    help="How much point dummies will be generated, set backdate")
  parser.add_argument(
    "-t", "--time",
    default='1',
    nargs='?',
    type=int,
    help="Time range (in second) each point is generated")
  parser.add_argument(
    "-b", "--basetime",
    default=None,
    nargs='?',
    type=int,
    help="Base time in precision microsecond")
  return parser

def genData(payload):
    ltag1 = ["nTag1_1","nTag1_2","nTag1_3"]
    ltag2 = ["nTag2_1","nTag2_2","nTag2_3","nTag2_4","nTag2_5","nTag2_6"]
    ltag3 = ["nTag3_1","nTag3_3","nTag3_3","nTag3_4","nTag3_5","nTag3_6","nTag3_7","nTag3_8","nTag3_9"]
    ltag4 = ["nTag4_1","nTag4_4","nTag4_3","nTag4_4","nTag4_5","nTag4_6","nTag4_7","nTag4_8","nTag4_9","nTag4_10","nTag4_11","nTag4_12"]
    ltag5 = ["nTag5_1","nTag5_2","nTag5_3","nTag5_4","nTag5_5","nTag5_6","nTag5_7","nTag5_8","nTag5_9","nTag5_10","nTag5_11","nTag5_12","nTag5_13","nTag5_14","nTag5_15"]
    tag1 = ltag1[int(random.random()*len(ltag1))]
    tag2 = ltag2[int(random.random()*len(ltag2))]
    tag3 = ltag3[int(random.random()*len(ltag3))]
    tag4 = ltag4[int(random.random()*len(ltag4))]
    tag5 = ltag5[int(random.random()*len(ltag5))]
    value1 = random.randint(80,100)
    value2 = random.randint(10,20)
    value3 = random.randint(0,50)
    value4 = random.randint(0,100)
    value5 = random.random()*10  
    hostname = "server"+str(random.randint(0,100))
    payload["tags"]={
      "tag1": tag1, 
      "tag2": tag2, 
      "tag3": tag3, 
      "tag4": tag4, 
      "tag5": tag5 
    }
    payload["fields"]={
      "value1": value1,
      "value2": value2,
      "value3": value3,
      "value4": value4,
      "value5": value5,
      "host": hostname
    }
    return payload

#main
if __name__ == '__main__':
  parser = get_args_parser()
  args = parser.parse_args()
  db = None
  if args.help or not args.user or not args.database or not args.measurement:
    parser.print_help()
    parser.exit()
  try:
  	db = InfluxDBClient(args.host, args.port, args.user, args.password, args.database)
  	db.ping()
  except Exception, err:
    logging.exception(err)
    print err
    sys.exit()
  try:
    if not args.no_create_database:
      db.create_database(args.database)
  except:
    print "Error creating database"
  db.switch_database(args.database)
  #Generate dummy data
  templateData = {
    "measurement": args.measurement,
    "tags":{},
    "fields": {}
  }
  if args.retention:
    templateData["retentionPolicy"] = args.retention
  timeScale = 1000000 # us precision
  timeInterval = args.time * timeScale 
  backMilliseconds = timeInterval * args.number
  if args.basetime:
    startTime = int(args.basetime) - backMilliseconds
  else:
    startTime = int(datetime.datetime.now().strftime('%s%f')) - backMilliseconds
  print "Start at "+str(startTime)+" = "+datetime.datetime.fromtimestamp(startTime/timeScale).strftime('%Y-%m-%d %H:%M:%S ') + time.tzname[0]
  t = time.time()
  for i in range(0,backMilliseconds,timeInterval):
    dummyData = genData(templateData)
    dummyData["time"] = startTime+i
    db.write_points([dummyData], time_precision='u')
    if args.debug:
      print dummyData
    if args.verbose:
      if time.time() - t > 1:
        t = time.time()
        print "rtDummy "+str((i/timeScale)/args.time+1)
  print "FINISH" 
