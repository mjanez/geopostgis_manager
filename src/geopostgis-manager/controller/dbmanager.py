#!/usr/bin/env python3
## File: dataset_loader.py
## Coding: UTF-8
## Author: Manuel Ángel Jáñez García (mjanez@tragsa.es)
## Institution: -
## Project: -
## Goal: The goal of this script is to convert and load spatial datasets in PostGIS
## Parent: geopostgis-manager/dataset_loader.py
""" Changelog:
    v1.0 - 12 Dec 2022: Create the first version
"""
# Update the version when apply changes 
version = "1.0"

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
##          dataset_loader.py           ##
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##

# Main program to test geopostgis-manager/dataset_loader.py
#  Call this script as "python3 dataset_loader.py" inside ""./geopostgis-manager/src/geopostgis-manager"

## Import libraries
#from dataset import Dataset
from config.log import  log_file
from model.dataset import Dataset
from model.db import get_connection, create_engine
from controller.postgismanager import shp_to_postgis, update_srid, create_index
from datetime import datetime
import argparse as ap
import logging
from subprocess import Popen, PIPE
import glob
from functools import reduce
from pathlib import Path
import os
import shutil
from joblib import Parallel, delayed
import shapely
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
from geoalchemy2 import Geometry, WKTElement
import pandas as pd
import pandas.io.sql as sqlio
import geopandas as gpd                                         # Requires fiona, pyproj, shapely and rtree

log_module = "[" + __name__ + "]"

class DBParams:
    def __init__(self, db_params=None):
        """
        Constructor of the DBParams class.

        Parameters:
        - dbname -- Specify the name of the database.
        - host -- Hostname or IP.
        - port -- Port of database.
        - username -- The name of the user that is connected to the database.
        - password -- Password of the username.
        """
        self.dbname = db_params['dbname']
        self.host = db_params['host']
        self.port = db_params['port']
        self.username =db_params['username']
        self.password = db_params['password']

    def set_dbname(self, dbname):
        self.dbname = dbname

    def set_host(self, host):
        self.host = host

    def set_port(self, port):
        self.port = port

    def set_username(self, username):
        self.username = username

    def set_password(self, password):
        self.password = password

class PostgisLoader:
    def __init__(self, endpoint_name, log_folder, db_type, db_params, datasets_doc, datasets_table, datasets_mode, parallel=False, db_conn=None, load_to_db=None):
            """
            Constructor of the PostgisLoader class.

            Parameters:
            - endpoint_name -- Name of the DB Server.
            - log_folder -- Logging file folder.
            - db_type -- Type of the DB Server.
            - db_params -- DB parameters.
            - db_conn -- SQLAlchemy connection.
            - datasets_doc -- schema.table or filepath, depends on datasets_mode.
            - datasets_mode -- Origin of the table with the dataset information.
            - datasets_table -- schema.table or filepath, depends on datasets_mode.
            - parallel -- Parallelization True/False.
            - load_to_db -- Load to Database True/False.
            """
            self.endpoint_name = endpoint_name
            self.logger = log_file(log_folder)
            self.db_type = db_type
            self.db_params = DBParams(db_params)
            self.db_conn = db_conn
            self.datasets = self.get_datasets_from_table(datasets_doc, datasets_table, datasets_mode)
            self.parallel = parallel
            self.load_to_db = load_to_db
            # Cores available to parallelization
            self.processes = os.cpu_count() - 1

    def get_db_conn(self):
        """
        Get a database connection using Database management functions from db_management.py

        Parameters:
        self -- Datasets object

        Return:
        DB connection object
        """
        self.db_conn = get_connection(self.db_params, self.endpoint_name, self.db_type)
        return self.db_conn

    def connect_db(self):
        """
        Restore database connection

        Parameters:
        self -- Datasets object

        Return:
        DB connection object
        """
        return self.get_db_conn()

    def create_datasets(self, datasets_doc_df, datasets_doc):
        """
        Returns a Datasets object from the pandas dataframe retrieved from the table with the original documentation.

        Parameters:
        - datasets_doc_df -- Datasets documentation pandas dataframe retrieved from DB/File
        - datasets_doc -- dataset_doc dictionary of config.yml

        Return:
        Dataset Object
        """
        datasets = []

        # Transform datasets_doc_df records (pandas dataframe of raw table) into a Dataset Object
        for index, row in datasets_doc_df.iterrows():
            if row[datasets_doc.field_identifier] is not None:
                # Set identifier and name
                try:
                    dataset = Dataset(row[datasets_doc.field_name], row[datasets_doc.field_identifier], datasets_doc.output_schema)
                    if datasets_doc.output_schema is not None:
                        dataset.set_schema(datasets_doc.output_schema)
                    else:
                        dataset.set_schema("public")
                    dataset.get_table_name()
                except:
                    logging.error(log_module + ":" + "The dataset: " + row[datasets_doc.field_name] + " has no identifier (field:[" + datasets_doc.field_identifier + "]), it will not be loaded.")
                    dataset.set_status('error')
                    dataset.set_status_info('missing or wrong identifier [' + datasets_doc.field_identifier + ']')

                # Set path of the spatial dataset
                try:
                    files_list = glob.glob(row[datasets_doc.field_path] + "/*")
                    for file in files_list:
                        if file.endswith('.shp'):
                            filepath = file
                            dataset.set_path(filepath)
                            dataset.set_file_format('SHP')
                        elif file.endswith('.tif'):
                            filepath = file
                            dataset.set_path(filepath)
                            dataset.set_file_format('TIFF')
                except:
                    logging.error(log_module + ":" + "The dataset: " + row[datasets_doc.field_name] + " has no path (field:[" + datasets_doc.field_path + "]), it will not be loaded.")
                    dataset.set_status('ignore')

                # Set path of the SLD
                try:
                    if row[datasets_doc.field_sld].endswith('.sld'):
                        dataset.set_sld_path(row[datasets_doc.field_sld])
                except:
                    logging.error(log_module + ":" + "The dataset: " + row[datasets_doc.field_name] + " has no sld_path (field:[" + datasets_doc.field_sld + "]), it will not be loaded.")

                # Set description
                try:
                    dataset.set_description(row[datasets_doc.field_description])
                except:
                    logging.info(log_module + ":" + "The dataset: " + row[datasets_doc.field_name] + " has no description (field:[" + datasets_doc.field_description + "]), it will be loaded.")

                # Set metadata_url
                try:
                    dataset.set_metadata_url(row[datasets_doc.field_metadata_url])
                except:
                    logging.info(log_module + ":" + "The dataset: " + row[datasets_doc.field_name] + " has no metadata_url (field:[" + datasets_doc.field_metadata_url + "]), it will be loaded.")

                # Set ogc_workspace
                try:
                    dataset.set_ogc_workspace(row[datasets_doc.field_ogc_workspace])
                except:
                    logging.info(log_module + ":" + "The dataset: " + row[datasets_doc.field_name] + " has no ogc_workspace (field:[" + datasets_doc.field_ogc_workspace + "]), it will be loaded.")

                # Set creator
                try:
                    dataset.set_creator(row[datasets_doc.field_creator])
                except:
                    logging.info(log_module + ":" + "The dataset: " + row[datasets_doc.field_name] + " has no creator (field:[" + datasets_doc.field_creator + "]), it will be loaded.")
                
                dataset.set_status('to load')
                datasets.append(dataset)

            else:
                dataset.set_status('review')
                try:
                    dataset.set_name(row[datasets_doc.field_name])
                    logging.error(log_module + ":" + "The dataset: " + row[datasets_doc.field_name] + " it will NOT be loaded.")
                except:
                    logging.error(log_module + ":" + "The dataset it will NOT be loaded.")

                datasets.append(dataset)
                
        return datasets

    def get_datasets_from_table(self, datasets_doc, datasets_table, datasets_mode):
        """
        Read a table with the documentation of the datasets to be uploaded to the database and generates a Datasets object (create_datasets()) that can be used in the rest of the DB/Geoserver upload processes.

        Parameters:
        - kwargs_db -- Parameters of the db
        - datasets_doc -- schema.table or filepath, depends on datasets_mode
        - datasets_mode -- Origin of the table with the dataset information
        - datasets_table -- schema.table or filepath, depends on datasets_mode

        Return:
        Datasets object
        """
        # Connect to DB
        db_conn = self.get_db_conn()

        datasets_doc_df = None

        # Retrieve publisher if exists
        if datasets_doc.publisher:
            publisher = datasets_doc.publisher

        #Import datasets_doc from DB
        if datasets_mode == "db":
            try:
                dataset_query = "SELECT * FROM {table}".format(
                        table=datasets_table
                    )
                    
                if publisher:
                    dataset_query += " WHERE {field_publisher} LIKE '{publisher}'".format(
                            field_publisher=datasets_doc.field_publisher,
                            publisher=publisher
                        )

                db_conn.set_client_encoding('UTF8') 
                logging.info(log_module + ":" + "SQL query:\n" + str(dataset_query))
                datasets_doc_df = sqlio.read_sql_query(dataset_query, db_conn)
                db_conn.close()
                db_conn = None
            except:
                logging.error(log_module + ":" + "The table " + datasets_table + " does not exist")

        #Import datasets_doc from file
        else:
            try:
                if "csv" in datasets_table:
                    df = pd.read_csv(os.path.abspath(datasets_table), encoding='utf8')
                elif "xls" in datasets_table:
                    df= pd.read_excel(os.path.abspath(datasets_table))

                if publisher:
                    datasets_doc_df = df.loc[df[datasets_doc.field_publisher] == publisher]
            except:
                logging.error(log_module + ":" + "The format of the dataset documentation file is not supported. Try a CSV or XLS/XLSX.")

        # Create datasets Object
        datasets = self.create_datasets(datasets_doc_df, datasets_doc)

        return datasets

    def shp2pgsql_batch(self, dataset, db_engine, db_params):
        """Create batch task to store into a PostGIS Database all ESRI Shapefiles ZIPs from a directory.

        Parameters:
        - self -- Datasets object to upload into PostGIS.
        - dataset -- Dataset to load into PostGIS DB.
        - db_engine -- SQLAlchemy database engine.
        - db_params -- Database connection details.

        Return:
        Datasets Object.
        """

        start = datetime.now()

        # Upload to PostGIS
        try:
            dataset = shp_to_postgis(dataset, db_engine)
        except Exception as e:
            logging.exception(
                "Error found during loading ESRI Shapefile to PostGIS!"
                f"Dataset table: {dataset.schema}.{dataset.table}, "
                f"exception: {e}"
            )

        #Transform to SRID 3857 the dataset uploaded
        try:
            dataset = update_srid(dataset, db_params)
        except Exception as e:
            logging.exception(
                "Error found during updating SRID!"
                f"Dataset table: {dataset.schema}.{dataset.table}, "
                f"exception: {e}"
            )
        
        # Create Geometry Index and clustering table
        try:
            dataset = create_index(dataset, db_params)
        except Exception as e:
            logging.exception(
                "Error found during creating geometry index!"
                f"Dataset table: {dataset.schema}.{dataset.table}, "
                f"exception: {e}"
            )


        # Outputinfo
        end = datetime.now()
        diff =  end - start
        logging.info("Finish: " +  dataset.schema + "." + dataset.table + " | Time elapsed: " + str(diff))

    def load_datasets(self):
        """
        Load all elements available (dataset.status = "to load") in the Datasets object and update the status ("done").

        Parameters:
        self -- Datasets object.

        Return:
        Datasets Object.
        """
        
        db_engine = create_engine(self.db_params)

        logging.info(log_module + ":" + "Multicore parallel processing: " + str(self.parallel))

        if self.parallel is True:
            logging.info(log_module + ":" + "Number of processes: " + str(self.processes))
            # TODO:Multi core processing [Funciona correctamente]
            # Load to DB
            if self.load_to_db is True:
                # SHP to Postgis
                Parallel(n_jobs=self.processes, prefer="threads")(delayed(self.shp2pgsql_batch)(dataset=d, db_engine=db_engine, db_params=self.db_params) for d in self.datasets if d.status == "to load" and d.file_format == "SHP")
                print("Prueba")

        else:
            # Single core processing
            # Load to DB
            if self.load_to_db is True:
                for dataset in self.datasets if dataset.status == "to load" else None:
                    # SHP to Postgis
                    if dataset.file_format == "SHP":
                        self.shp2pgsql_batch(dataset, db_engine, self.db_params)

        return self.datasets