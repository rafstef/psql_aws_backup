#!/usr/bin/env python

import boto3
import requests
import re
import getopt
import sys
import urllib2
import time
import json
import socket
import sys
import string
from psql import *

import logging as log
log.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                level=log.INFO,
                datefmt='%m/%d/%Y %H:%M:%S')


def psql_snapshot_name(v):
  name = None
  for tag in v.tags:
    if tag['Key'] == 'Name':
      name = tag.get('Value')
  s_name = "psql:%s" % name
  return s_name


def create_snapshot(psql_volumes):
  tstamp = datetime.now().strftime("%Y%m%d_%H%M")
  for v in psql_volumes:
    name = psql_snapshot_name(v)
    v.create_snapshot(
      Description = 'string',
      TagSpecifications = [
        {
          'ResourceType':  'volume' ,
          'Tags': [
            {
              'Key': 'Name',
              'Value': "%s_%s" % (name, tstamp)
            },
          ]
        },
      ],
      DryRun = False
    )



def find_psql_volumes(ec2, instance):
  psql_volumes = []
  instancename = None
  for tags in instance.tags:
    if tags["Key"] == 'Name':
      instancename = tags["Value"]
  for v in instance.block_device_mappings:
    if v['DeviceName']==instance.root_device_name:
      continue
    volume = ec2.Volume(v['Ebs']['VolumeId'])
    name = None
    for tag in volume.tags:
      if tag['Key'] == 'Name':
        name = tag.get('Value')
    pattern = ("pslq:%s_[*]" % name )
    if re.match(pattern, instancename):
      psql_volumes.append(volume)
  return psql_volumes



def main(args):
  if len(args) != 6:
    print("db_host,db_name,db_user,db_password,methenv,retention")
    sys.exit(1)
  else:
    conn = psql_open_connection(args[0],args[1],args[2],args[3])
    methenv = args[4]
    retention = args[5]
    pg_start_backup(conn, methenv)

    #hostname= socket.gethostname()

    hostname = "TCCAUSV1APL-EDICORE01"
    log.info("%s", hostname)
    inst_id = urllib2.urlopen("http://169.254.169.254/latest/meta-data/instance-id").read()
    response = urllib2.urlopen("http://169.254.169.254/latest/dynamic/instance-identity/document").read()
    data = json.loads(response)
    region = data["region"]
    session = boto3.Session(region_name=region)
    ec2 = session.resource('ec2')
    instance = ec2.Instance(inst_id)
    psql_volumes = find_psql_volumes(ec2, instance)
    create_snapshot(psql_volumes)
    remove_old_snapshot(psql_volumes,retention, instance)


if __name__ == '__main__':
  main(sys.argv[1:])



