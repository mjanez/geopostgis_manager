#!/usr/bin/env python3
## File: run.py
## Coding: UTF-8
## Author: Manuel Ángel Jáñez García (mjanez@tragsa.es)
## Institution: -
## Project: -
## Goal: The goal of this script is to provide the program to test ckan/geopostgis-manager/ckan_management.py and ckan/geopostgis-manager/Dataset.py. Modified run.py to harvest CSW endpoints from geopostgis-manager.
## Parent: geopostgis-manager/run.py
""" Changelog:
    v1.0 - 12 Dec 2022: Create the first version
"""
# Update the version when apply changes 
version = "1.0"

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
##                run.py                ##
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##

# Main program to test geopostgis-manager/run.py
#  Call this script as "python3 run.py" inside "./geopostgis-manager/src/geopostgis-manager"

## Import libraries
import logging
from datetime import datetime   
import os
from config.config import config_get_parameters
from config.log import log_file
from controller.dbmanager import PostgisLoader

log_module = "[run.main]"

def generate_datasets_object(db_server, datasets_doc, db_type=None, log_folder=None):
    """
    Launch ingesting process dependes on type of db

    Parameters:
    - db_server -- DB server parameters
    - datasets_doc -- Dict contains all info about datasets_doc from config.yml
    - db_type -- Type of DB of server
    - log_folder -- Logging file folder

    Return:
    DB Records and New records counters
    """

    # datasets doc
    if datasets_doc.in_db:
        datasets_table = datasets_doc.db_table
        datasets_mode = "db"
    else:
        datasets_table = datasets_doc.path
        datasets_mode = "file"

    kwargs_db = dict(
        endpoint_name = db_server.name,
        log_folder = log_folder,
        db_type = db_type,
        db_params = dict(
            host = db_server.host,
            port = db_server.port,
            username = db_server.username,
            password = db_server.password,
            dbname = db_server.dbname,
        ),
        datasets_doc = datasets_doc,
        datasets_table = datasets_table,
        datasets_mode = datasets_mode,
        parallel = default_config.parallelization,
        load_to_db = default_config.load_to_db
    )

    logging.info(log_module + ":" + "Datasets mode: " + datasets_mode.upper() + " from: " + datasets_table)

    # Create a list with all Dataset objects to be loaded into the database
    if db_type.lower() == "postgres" or db_type.lower() == "postgis":
        datasets_to_db = PostgisLoader(**kwargs_db)

        return datasets_to_db

    else:
        logging.error("Database type: " + db_type.lower() + " not compatible.")

        return None

def ingest_db(datasets):
    """
    Launch ingesting process dependes on type of db

    Parameters:
    - datasets -- Datasets object
    Return:
    DB Records and New records counters
    """

    datasets.load_datasets()

    return datasets


def ingest_geoserver(datasets:
    #TODO
    print("Prueba")

if __name__ == '__main__':
    # Retrieve parameters and init log
    harvester_start = datetime.now()
    geoserver_servers, db_servers, datasets_doc, default_config  = config_get_parameters()
    log_folder = os.path.abspath(__file__ + "/../../../log")
    print("Log folder: " + log_folder)
    log_file(log_folder)

    # Starts software
    logging.info(log_module + ":" + "geopostgis-manager // Version:" + str(default_config.version))

    # Check invalid 'type' parameter in config.yml
    for db_endpoint in db_servers if db_servers is not None else None:
        # Generate Datasets object
        datasets = generate_datasets_object(db_server=db_endpoint, db_type=db_endpoint.type.lower(), log_folder=log_folder, datasets_doc=datasets_doc)

        # Ingest to DB
        #datasets = ingest_db()    Borrar # cuando funcione bien el de Geoserver de abajo

        #TODO: Ingest to Geoserver if dbname in db_endpoint and geoserver_endpoint is the same
        for geoserver_endpoint in geoserver_servers if geoserver_servers is not None and geoserver_servers.dbname_store == db_endpoint.dbname else None:
            datasets = ingest_geoserver()

    # geopostgis-manager outputinfo
    hrvst_diff =  datetime.now() - harvester_start

    try:
        new_records = sum(datasets)
    except:
        new_records = 0

    logging.info(log_module + ":" + "geopostgis-manager // config.yml DB: " + str(len(db_servers)) + " and new DB datasets: " + str(new_records) + " | Total time elapsed: " + str(hrvst_diff).split(".")[0])
