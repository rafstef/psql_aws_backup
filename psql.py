#!/usr/bin/python
import psycopg2
from datetime import datetime

def psql_open_connection(host,db_name,db_user,db_password):
    conn_string = "host='%s' dbname='%s' user='%s' password='%s'" % \
                  (host,db_name,db_user,db_password)
    print "Connecting to database\n	->%s" % (conn_string)
    conn = psycopg2.connect(conn_string)
    return conn

def pg_start_backup(conn,methenv):
    tstamp = datetime.now().strftime("%Y%m%d_%H%M")
    cursor = conn.cursor()
    pg_start_backup = "SELECT pg_start_backup('%s_%s', true, true)" % (methenv, tstamp)
    cursor.execute(pg_start_backup)

def pg_stop_backup(conn):
    cursor = conn.cursor()
    pg_stop_backup = "SELECT pg_stop_backup()"
    cursor.execute(pg_stop_backup)

def psql_close_connection(conn):
    conn.close()







