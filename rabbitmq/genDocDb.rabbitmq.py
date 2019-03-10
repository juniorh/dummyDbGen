#!/usr/bin/env python
# How to use
# > python genDocDb.rabbitmq.py -u user1 -P user1 --vhost vh1 -q q1 -n 10 -S -s 1

import sys
import pika  
import time
import random
import string
import argparse

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
    default=5672,
    nargs='?',
    type=int, 
    help="Port number to use for connection.")
  parser.add_argument(
    "-u", "--user",
    default=None,
    nargs='?',
    type=str,
    help="Username for login if not current user.")
  parser.add_argument(
    "-P", "--password",
    default=None,
    nargs='?',
    type=str,
    help="Password to use when connecting to server.")
  parser.add_argument( 
    "--vhost",   
    default=None,
    nargs='?',
    type=str,
    help="Rabbitmq virtual hostname.")
  parser.add_argument( 
    "-S", "--send",
    default=False,
    action='store_true',
    help="Run as sender / publisher")
  parser.add_argument(
    "-R", "--receive",
    default=False, 
    action='store_true',
    help="Run as receiver / consumer")
  parser.add_argument(
    "-q", "--queue",
    default=None,  
    nargs='?',
    type=str,
    help="Rabbitmq queue name.")
  parser.add_argument(
    "-e", "--exchange",
    default='',
    nargs='?',
    type=str,
    help="Rabbitmq exchange binding.")
  parser.add_argument(
    "-s", "--size",
    default=0,
    nargs='?',
    type=int,
    help="Payload size.")
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
    help="How many messages will be generated")
  parser.add_argument(
    "--durable",
    default=False,
    action='store_true',
    help="Message durability")
  parser.add_argument(
    "--help",
    default=False,
    action='store_true',
    help="Show this help")
  return parser

def genRandString(y):
  return ''.join(random.choice(string.ascii_letters) for x in range(y))

def genData():
  return genRandString(int(args.size))

def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)

if __name__ == '__main__':
  parser = get_args_parser()
  args = parser.parse_args()
  if args.help or not (args.receive or args.send) or not args.vhost or not args.queue :
    if not (args.send or args.receive):
      print "Option -S or -R is needed"
    if args.send and args.receive:
      print "Don't set both -S and -R"
    if not (args.vhost and args.queue):
      print "vHost and Queue name is needed"
    print ""
    parser.print_help()
    parser.exit()
    sys.exit()   
  try:
    auth = pika.PlainCredentials(args.user, args.password)
    conn = pika.BlockingConnection(pika.ConnectionParameters(host=args.host, port=args.port, credentials=auth, virtual_host=args.vhost))
    ch = conn.channel()
    ch.queue_declare(queue=args.queue, durable=args.durable)
  except Exception, err:
    print err
    sys.exit()
  if args.send:
    print "Start send data"
    t = time.time()
    t_start = t
    for i in range(0,args.number):
      payload = "origin " + str(conn._impl.socket.getsockname()) + " | payload " + str(i) + ": "+ genData()
      ch.basic_publish(exchange='', routing_key=args.queue, body=payload)
      print payload
      if args.report:
        if time.time() - t > args.report:
          t = time.time()
          print "rtDummy "+str(i+1)
    print "Finish send message " + str(i+1)
  if args.receive:
    print "Start listen and receive from vhost: " + args.vhost + "port: " + str(args.port)
    ch.basic_consume(callback, queue=args.queue, no_ack=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    ch.start_consuming()
