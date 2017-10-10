#!/usr/bin/env python
#
# Gather some IO statistics about RBD volumes from the gzipped OSD logs. It works on Ceph OSD v12.x with bluestore backend
#
# The OSD logs must contain the filestore messages at log level 10. Enable 
# that dynamically with something like
#
#    ceph tell osd.0 injectargs '--debug_bluestore 10'
#
# or
#
#    ceph tell osd.* injectargs '--debug_bluestore 10'
#
# or via ceph.conf on the OSDs:
#
# [osd]
#        debug bluestore = 10
#
# Usage: 
#     python rbd-io-stats.py /var/log/ceph/ceph-osd.0.log-20140416.gz
#     python rbd-io-stats.py /var/log/ceph/ceph-osd.0.log
#
## write pattern
##     2017-10-09 18:24:57.998841 7fd604d6c700 10 bluestore(/var/lib/ceph/osd/ceph-1) _write 6.5_head #6:a6356a47:::rbd_data.c65e3238e1f29.000000000000083b:head# 0x240000~7e000 = 0
## read pattern
##     2017-10-10 11:27:32.223924 7fd602d68700 10 bluestore(/var/lib/ceph/osd/ceph-1) read 6.4_head #6:24526142:::rbd_data.c65e3238e1f29.0000000000000318:head# 0x3fc000~4000 = 16384

import json
import sys
import re
from subprocess import Popen, PIPE

def sortDict(d):
  return sorted(d.items(), key=lambda x:x[1], reverse=True) 

def printListLine(list):
  for i in list:
    print i

filename = sys.argv[1]
if filename.split('.')[-1] == 'gz':
  f = Popen(['zcat', filename], stdout=PIPE)
else:
  f = Popen(['cat', filename], stdout=PIPE)

wp = re.compile('10 bluestore\(\/var\/lib\/ceph\/osd\/ceph-(\d+)\) (_write|read) (\S+) \#(\S+)\# (\S+)\~(\S+) \= (\d+)');

wc_osd = {}
wc_pool = {}
wc_pg = {}
wc_rbd = {}
wc_object = {}
wc_length = {}

rc_osd = {}
rc_pool = {}
rc_pg = {}
rc_rbd = {}
rc_object = {}
rc_length = {}

for line in  f.stdout:
  filter1 = re.search('rbd_data',line) 
  s = wp.search(line)
  if s and filter1:
    (osd,op,pg,object,offset_h,length_h,length) = s.groups()
    length_d = int('0x'+length_h,0)
    try:
      (w_pool,w_pg) = re.search(r'(\S+).(\S+)\_',pg).groups()
      (w_rbd,) = re.search(r'rbd_(\S+)\.',object).groups()
    except:
      pass
    if op == '_write':
      try:
        wc_osd[osd] += 1
      except:
        wc_osd[osd] = 1
      try:
        wc_pool[w_pool] += 1 
      except:
        wc_pool[w_pool] = 1
      try:
        wc_pg[w_pg] += 1 
      except:
        wc_pg[w_pg] = 1
      try:
        wc_rbd[w_rbd.split('.')[1]] += 1 
      except:
        wc_rbd[w_rbd.split('.')[1]] = 1
      try:
        wc_object[object] += 1 
      except:
        wc_object[object] = 1
      try:
        wc_length[length_d] += 1 
      except:
        wc_length[length_d] = 1
    if op == 'read':
      try:
        rc_osd[osd] += 1
      except:
        rc_osd[osd] = 1
      try:
        rc_pool[w_pool] += 1
      except:
        rc_pool[w_pool] = 1
      try:
        rc_pg[w_pg] += 1
      except:
        rc_pg[w_pg] = 1
      try:
        rc_rbd[w_rbd.split('.')[1]] += 1
      except:
        rc_rbd[w_rbd.split('.')[1]] = 1
      try:
        rc_object[object] += 1
      except:
        rc_object[object] = 1
      try:
        rc_length[length_d] += 1
      except:
        rc_length[length_d] = 1

print "\nWrites per OSD:"
printListLine(sortDict(wc_osd))
print "\nWrites per POOl:"
printListLine(sortDict(wc_pool))
print "\nWrites per PG:"
printListLine(sortDict(wc_pg))
print "\nWrites per RBD image:"
printListLine(sortDict(wc_rbd))
print "\nWrites per OBJECT:"
printListLine(sortDict(wc_object))
print "\nWrites per LENGTH:"
printListLine(sortDict(wc_length))

print "\nRead per OSD:"
printListLine(sortDict(rc_osd))
print "\nRead per POOl:"
printListLine(sortDict(rc_pool))
print "\nRead per PG:"
printListLine(sortDict(rc_pg))
print "\nRead per RBD image:"
printListLine(sortDict(rc_rbd))
print "\nRead per OBJECT:\n"
printListLine(sortDict(rc_object))
print "\nRead per LENGTH:\n"
printListLine(sortDict(rc_length))

#print "\nWrites per OSD:\n"+json.dumps(wc_osd, indent=4)
#print "\nWrites per POOl:\n"+json.dumps(wc_pool, indent=4)
#print "\nWrites per PG:\n"+json.dumps(wc_pg, indent=4)
#print "\nWrites per RBD image:\n"+json.dumps(wc_rbd, indent=4)
#print "\nWrites per OBJECT:\n"+json.dumps(wc_object, indent=4)
#print "\nWrites per LENGTH:\n"+json.dumps(wc_length, indent=4)

#print "\nRead per OSD:\n"+json.dumps(rc_osd, indent=4)
#print "\nRead per POOl:\n"+json.dumps(rc_pool, indent=4)
#print "\nRead per PG:\n"+json.dumps(rc_pg, indent=4)
#print "\nRead per RBD image:\n"+json.dumps(rc_rbd, indent=4)
#print "\nRead per OBJECT:\n"+json.dumps(rc_object, indent=4)
#print "\nRead per LENGTH:\n"+json.dumps(rc_length, indent=4)
