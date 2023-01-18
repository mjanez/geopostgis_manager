#!/usr/bin/env python3
## File: db_management.py
## Coding: UTF-8
## Author: Manuel Ángel Jáñez García (mjanez@tragsa.es)
## Institution: -
## Project: -
## Goal: The purpose of this script is to manage connections and operations with the database.
""" Changelog:
    v1.0 - 12 Dec 2022: Create the first version
"""
# Update the version when apply changes 
version = "1.0"

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
##           db_management.py           ##
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##

# Database management functions.

## Import libraries
import urllib.request
import json
import sys
from pprint import pprint
import psycopg2
from datetime import datetime
import logging
import os
from sqlalchemy.ext.declarative import declarative_base 
from sqlalchemy import MetaData
import sqlalchemy

log_module = "[" + __name__ + "]"

def get_connection(db_params, endpoint_name=None, db_type=None):
  if endpoint_name is not None:
    logging.info(log_module + ":" + "Connect to: " + endpoint_name + " | DB type: " + db_type)
  conn = psycopg2.connect(host=db_params.host, port=db_params.port, user=db_params.username, password=db_params.password, dbname=db_params.dbname)
  return(conn)

def create_engine(db_params):
    db_engine = sqlalchemy.create_engine('postgresql://' + db_params.username + ':' + db_params.password + '@'+ db_params.host + ':' + db_params.port + '/' + db_params.dbname)
    return db_engine

def get_query(conn, query):
    rv = True
    cur = conn.cursor()
    conn.set_client_encoding('UTF8') 
    print(str(query))
    logging.info("SQL query:\n" + str(query))
    cur.execute(query)
    conn.commit()
    return(rv)

# Delete tables before create function
def drop_table(tbl_name, engine, dbschema):
    print('Check if ' + tbl_name + ' exists')
    base = declarative_base()
    metadata = MetaData(schema=dbschema)
    metadata.reflect(bind=engine)
    table = metadata.tables[dbschema + '.' + tbl_name]
    if table is not None:
        logging.info(f'Deleting {tbl_name} table')
        print('Deleting ' + tbl_name + ' table')
        base.metadata.drop_all(engine, [table], checkfirst=True)