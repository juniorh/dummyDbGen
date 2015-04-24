#!/usr/bin/env python
# How to use
# >python genDocDb.influx.py -h localhost -P 8086 -u root -p password -d database -s series -n 1000 -t 1
import datetime
import logging
import argparse
import random
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
    default='root',
    nargs='?',
    type=str,
    help="User for login if not current user.")
  parser.add_argument(
    "-p", "--password",
    default='root',
    nargs='?',
    type=str,
    help="Password to use when connecting to server.")
  parser.add_argument(
    "-d", "--database",
    default='test',
    nargs='?',
    type=str,
    help="Select variable")
  parser.add_argument(
    "-s", "--serie",
    default='testing',
    nargs='?',
    type=str,
    help="Select serie")
  parser.add_argument(
    "--help",
    default=False,
    action='store_true',
    help="Show this help"
  )
  parser.add_argument(
    "-n", "--number",
    default='1',
    nargs='?',
    type=str,
    help="How much point dummies will be generated")
  parser.add_argument(
    "-t", "--time",
    default='1',
    nargs='?',
    type=str,
    help="Time range (in second) each point is generated")
  return parser

#main
if __name__ == '__main__':
  parser = get_args_parser()
  args = parser.parse_args()
  if args.help:
    parser.print_help()
    parser.exit()
  try:
  	db = InfluxDBClient(args.host,args.port,args.user,args.password)
  	db.get_database_list()
  except Exception, err:
    logging.exception(err)
    print err
    sys.exit()
  try:
    db.create_database(args.database)
  except:
    print "Error creating database"
  db.switch_db(args.database)
  #Generate dummy data
  backMilliseconds = 86000 * 1
  startTime = int(datetime.datetime.now().strftime('%s')) - backMilliseconds
  timeInterval = 60 * 1
  eventTypes = ["click", "view", "post", "comment"]
  cpuSeries = {
    'name': 'cpu_idle',
    'columns': ['time','value','value1','value2','value3','host'],
    'points': []
  }
  eventSeries = {
    'name': "customer_events",
    'columns': ['time','customerId','type'],
    'points': []
  }
  for i in range(0,backMilliseconds,timeInterval):
    hostname = "server"+str(random.randint(0,100))
    value = random.randint(0,100)
    value1 = random.randint(80,100)
    value2 = random.randint(10,20)
    value3 = random.randint(0,50)
    cpuSeries['points'] = [[startTime+i,value,value1,value2,value3,hostname]]
    eventSeries['points'] = []
    for j in range(0,random.randint(1,10)):
      customerId = random.randint(1,999)
      eventTypeIndex = random.randint(0,3)
      eventSeries['points'].append([startTime+j,customerId,eventTypes[eventTypeIndex]])
    #print i,cpuSeries
    db.write_points([cpuSeries])
    db.write_points([eventSeries])
  print "FINISH"
