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
from datetime import datetime
import logging as log


log.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                level=log.INFO,
                datefmt='%m/%d/%Y %H:%M:%S')


def create_snapshot(ec2, volumes_ids):
  tstamp = datetime.now().strftime("%Y%m%d_%H%M")
  for v in volumes_ids:
    try:
      volume = ec2.Volume(v)
    except Exception as e:
      log.error("%s" % e)
      sys.exit(1)
    name = None
    for tag in volume.tags:
      if tag['Key'] == 'Name':
        name = str(tag['Value'])
    snapshot_value = "%s_%s" % (name,tstamp)
    try:
      response = ec2.create_snapshot(
        VolumeId = v,
        Description = name.upper() ,
        TagSpecifications = [
          {
            'ResourceType':  'snapshot' ,
            'Tags': [
              {
                'Key': 'Name',
                'Value': snapshot_value.upper()
              },
              {
                'Key': 'Type',
                'Value': "ha_backup"
              },
              {
                'Key': 'Volume-id',
                'Value': v
              },
            ]
          },
        ]
      )
      log.info("Create snapshot %s" % response)
    except Exception as e:
      log.error("%s" % e)
      sys.exit(1)

def find_psql_volumes(ec2, instance):
  psql_volumes_ids = []
  instancename = None
  for tag in instance.tags:
    if tag["Key"] == 'Name':
      instancename = str(tag["Value"])
  for v in instance.block_device_mappings:
    if v['DeviceName']==instance.root_device_name:
      continue
    try:
      volume = ec2.Volume(v['Ebs']['VolumeId'])
    except Exception as e:
      log.error("%s" % e)
      sys.exit(1)
    volume_name = None
    volume_complete_name = None
    for tag in volume.tags:
      if tag['Key'] == 'Name':
        volume_complete_name = str(tag['Value'])
        volume_name=str(volume_complete_name.split("_")[0])
    pattern = "psql::%s" % instancename
    if pattern.lower() == volume_name.lower():
      psql_volumes_ids.append(volume.id)
  return psql_volumes_ids

def find_all_volume_snapshots(volume_id):
  try:
    client=boto3.client('ec2')
    r = client.describe_snapshots(Filters= [{'Name': 'tag:Type', 'Values': ['ha_backup']},{'Name': 'tag:Volume-id', 'Values': [volume_id]}])
    return r
  except Exception as e:
    log.error("%s" % e)
    sys.exit(1)

def order_snapshots(snapshots):
  return sorted(snapshots, key=lambda k: k['StartTime'], reverse=True)

def snapshots_to_remove(volumes_ids,retention):
  snapshots_to_remove = []
  for i in volumes_ids:
    snapshots = find_all_volume_snapshots(i)
    ordered_snapshots = order_snapshots(snapshots['Snapshots'])
    if len(ordered_snapshots) > retention:
      n = len(ordered_snapshots) - retention
      snapshots_to_delete = ordered_snapshots[-n:]
      snapshots_to_remove.append(snapshots_to_delete)
  return snapshots_to_remove

def delete_old_snaphots(ec2,snaphots):
  for s_volume in snaphots:
    for s in s_volume:
      try:
        snapshot = ec2.Snapshot(s["SnapshotId"])
        volume_id = None
        for tag in snapshot.tags:
          if tag['Key'] == 'Volume-id':
            volume_id = str(tag['Value'])
        response =snapshot.delete()
        log.info("\nVolume: %s \nSnapshot: %s \nDelete: %s" % (volume_id,snapshot,response))
      except Exception as e:
        log.error("Volume snapshot delete error %s" % (e))


def main(args):
  if len(args) != 6:
    print("db_host,db_name,db_user,db_password,methenv,retention")
    sys.exit(1)
  else:
    db_hostname = args[0]
    db_name = args[1]
    db_user=args[2]
    db_password = args[3]
    methenv = args[4]
    retention = args[5]
    conn = psql_open_connection(db_hostname, db_name, db_user,db_password)
    pg_start_backup(conn, methenv)
    hostname= socket.gethostname()
    log.info("%s", hostname)
    inst_id = urllib2.urlopen("http://169.254.169.254/latest/meta-data/instance-id").read()
    response = urllib2.urlopen("http://169.254.169.254/latest/dynamic/instance-identity/document").read()
    data = json.loads(response)
    region = data["region"]
    session = boto3.Session(region_name=region)
    ec2 = session.resource('ec2')
    instance = ec2.Instance(inst_id)
    psql_volumes_ids = find_psql_volumes(ec2, instance)
    log.info("Volume ids to backup: %s" % psql_volumes_ids)
    create_snapshot(ec2,psql_volumes_ids)
    pg_stop_backup()
    s = snapshots_to_remove(psql_volumes_ids,retention)
    delete_old_snaphots(ec2,s)

if __name__ == '__main__':
  main(sys.argv[1:])


