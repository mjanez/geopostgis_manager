#!/usr/bin/env python3
## Coding: UTF-8
## Author: mjanez@tragsa.es
## Institution: -
## Project: -
# inbuilt libraries
from datetime import datetime
import argparse as ap
import logging
from subprocess import Popen, PIPE
import glob
from functools import reduce
from pathlib import Path
import os
import csv
from typing import List, Optional

# custom functions
from config.log import  log_file
from model.Db import get_connection, create_engine
from controller.postgismanager import shp_to_postgis, update_srid, create_index, get_srid, check_table_exists
from controller.geoservermanager import check_geoserver_datastore, check_geoserver_workspace, create_geoserver_layer

# custom classes
from model.Dataset import Dataset
from model.Geoserver import Geoserver

# third-party libraries
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
    def __init__(self, db_params: List[dict] = []):
        """
        Constructor of the DBParams class.

        Parameters
        ----------
        db_params: list, optional

        Notes
        ----------
        endpoint: str. Name of the DB Endpoint.
        dbname: str. Specify the name of the database.
        host: str. Hostname or IP.
        port: str. Port of database.
        username: str. The name of the user that is connected to the database.
        password: str. Password of the username.
        active: bool. DB is active, it is planned to load datasets. True/False
        """
        self.endpoint = db_params['endpoint']
        self.dbname = db_params['dbname']
        self.host = db_params['host']
        self.port = db_params['port']
        self.username =db_params['username']
        self.password = db_params['password']
        self.active = db_params['active']

    def set_endpoint(self, endpoint):
        self.endpoint = endpoint

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

    def set_active(self, active):
        self.active = active

class GeoserverParams:
    def __init__(self, geoserver_params: List[dict] = []):
        """
        Constructor of the GeoserverParams class.

        Parameters
        ----------
        geoserver_params: list, optional
        
        Notes
        ----------
        endpoint: str. Name of the Geoserver instance.
        datastore: str. Datastore name (Geoserver).
        url: str. URL of the Geoserver host.
        workspace: str.  Workspace name (Geoserver).
        username: str. The name of the user that is connected to the database.
        password: str. Password of the username.
        declared_srid: int. Declared Geoserver CRS. https://docs.geoserver.org/stable/en/user/configuration/crshandling/configurecrs.html
        active: bool. Geoserver is active, it is planned to load datasets. True/False
        """
        self.endpoint = geoserver_params['endpoint']
        self.datastore = geoserver_params['datastore']
        self.url = geoserver_params['url']
        self.workspace = geoserver_params['workspace']
        self.username =geoserver_params['username']
        self.password = geoserver_params['password']
        self.declared_srid = geoserver_params['geo_srid']
        self.active = geoserver_params['active']

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

    def set_active(self, active):
        self.active = active

class OutputInfo:
    def __init__(self, bundle_id):
        """
        Constructor of the OutputInfo class.
        
        Parameters
        ----------
        bundle_id: str.        
        """
        self.csv_id: str = f"geopostgis-bundle-{bundle_id}"
        self.csv_file = None
        self.total_records: int = 0
        self.db_records: int = 0
        self.geo_records: int = 0
        self.error_records: int = 0

    def set_csv(self, log_folder, datasets):
        # datasets to csv
        csv_name = self.csv_id.replace(" ", "-")
        datetime_str = str(datetime.now()).split(":")[0].replace(" ", "_") + "h"
        self.csv_file = os.path.abspath(f"{log_folder}/{csv_name}_{datetime_str}.csv")
        try:
            fieldnames = list(datasets[0].dataset_dict().keys())
            with open(self.csv_file, 'w+', newline='', encoding="utf-8") as data:
                wr = csv.writer(data, delimiter=",", quoting=csv.QUOTE_ALL)
                wr.writerow(fieldnames)
                for d in datasets:
                    wr.writerow(list(d.dataset_dict().values()))
        except:
            logging.error(f"{log_module}:The CSV: '{str(self.csv_file)}' with datasets log-info could not be created.")

    def set_output_info(self, datasets):
        self.total_records = len(datasets)
        self.geo_records = len([x for x in datasets if x.status == 'geoserver_uploaded'])
        self.db_records= len([x for x in datasets if x.status == 'db_uploaded'])
        self.error_records = len([x for x in datasets if x.status == 'error'])

        

class BaseLoader:
    def __init__(
        self, 
        bundle_id: str, 
        log_folder: str,
        db_type: str,
        datasets_doc: str,
        datasets_table: str,
        datasets_mode: str,
        db_params = None,
        geoserver_params = None, 
        parallel: bool = False,
        proxies = None,
        db_conn = None,
        load_to_db: bool = False,
        load_to_geoserver: bool = False):
            """
            Constructor of the BaseLoader class.

            Parameters
            ----------
            bundle_id: str. Name of the Bundle BD/Geoserver.
            log_folder: str. Logging file folder.
            db_type: str. Type of the DB Server.
            db_conn: optional. SQLAlchemy connection.
            datasets_doc: str. Tschema.table or filepath, depends on datasets_mode.
            datasets_mode: str. Origin of the table with the dataset information.
            datasets_table: str. schema.table or filepath, depends on datasets_mode.
            db_params: optional. DB parameters.
            geoserver_params: optional. Geoserver parameters.
            parallel: bool. Parallelization True/False.
            proxies: dict. HTTP/HTTPS proxies.
            load_to_db: bool. Load to Database True/False.
            load_to_geoserver: bool. Load to Geoserver True/False.
            """
            self.bundle_id = bundle_id
            self.logger = log_file(log_folder)
            self.db_type = db_type
            self.db_params = DBParams(db_params)
            self.geoserver_params = GeoserverParams(geoserver_params)
            self.db_conn = db_conn
            self.datasets = self.get_datasets_from_table(datasets_doc, datasets_table, datasets_mode)
            self.parallel = parallel
            self.proxies = proxies
            self.load_to_db = load_to_db
            self.load_to_geoserver = load_to_geoserver
            self.output_info = OutputInfo(self.bundle_id)
            # Cores available to parallelization
            self.processes = os.cpu_count() - 1

    def get_db_conn(self):
        """
        Get a database connection using Database management functions from db_management.py

        Return
        ----------
        DB connection object
        """
        self.db_conn = get_connection(self.db_params, self.db_type)
        return self.db_conn

    def connect_db(self):
        """
        Restore database connection

        Return
        ----------
        DB connection object
        """
        return self.get_db_conn()

    def create_datasets(self, datasets_doc_df, datasets_doc):
        """
        Returns a Datasets object from the pandas dataframe retrieved from the table with the original documentation.

        Parameters
        ----------
        - datasets_doc_df: str. Datasets documentation pandas dataframe retrieved from DB/File
        - datasets_doc: str. dataset_doc dictionary of config.yml

        Return
        ----------
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

                    dataset.set_dbname(self.db_params.dbname)
                    dataset.set_table_name(dataset.identifier)
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
                            dataset.set_file_path(filepath)
                            dataset.set_file_format('shp')
                        elif file.endswith('.tif'):
                            filepath = file
                            dataset.set_file_path(filepath)
                            dataset.set_file_format('tiff')
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

                # Set ogc_workspace. Use geo_workspace, if not exists, use field_ogc_workspace of dataset table
                try:
                    if self.geoserver_params.workspace is not None:
                        dataset.set_ogc_workspace(self.geoserver_params.workspace)
                    else:
                        dataset.set_ogc_workspace(row[datasets_doc.field_ogc_workspace])
                except:
                    logging.info(log_module + ":" + "The dataset: " + row[datasets_doc.field_name] + " has no ogc_workspace (field:[" + datasets_doc.field_ogc_workspace + "]), it will be loaded.")

                # Set creator
                try:
                    dataset.set_creator(row[datasets_doc.field_creator])
                except:
                    logging.info(log_module + ":" + "The dataset: " + row[datasets_doc.field_name] + " has no creator (field:[" + datasets_doc.field_creator + "]), it will be loaded.")

                # Set SRID
                if row[datasets_doc.field_srid] is not None:
                    dataset.set_file_srid(row[datasets_doc.field_srid])

                # Set Data type (Vector/Raster)
                if row[datasets_doc.field_carto_type] is not None:
                    dataset.set_carto_type(row[datasets_doc.field_carto_type])

                # Set default status
                if dataset.carto_type == "raster":
                    dataset.set_status('geo_to-load')
                else:
                    dataset.set_status('db_to-load')
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

        Parameters
        ----------
        - kwargs_db: Parameters of the db
        - datasets_doc: schema.table or filepath, depends on datasets_mode
        - datasets_mode: Origin of the table with the dataset information
        - datasets_table: schema.table or filepath, depends on datasets_mode

        Return
        ----------
        Datasets object
        """
        datasets_doc_df = None

        # Retrieve publisher if exists
        if datasets_doc.publisher:
            publisher = datasets_doc.publisher

        #Import datasets_doc from DB
        if datasets_mode == "db":
            # Connect to DB
            db_conn = self.get_db_conn()
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
                if df.empty:
                    raise Exception
            except:
                logging.error(log_module + ":" + "The format of the dataset documentation file is not supported. Try a CSV or XLS/XLSX.")

        # Create datasets Object
        obj_datasets = self.create_datasets(datasets_doc_df, datasets_doc)

        return obj_datasets

class DbLoader(BaseLoader):
    """
    Constructor of the DbLoader class.

    Subclass of: BaseLoader       
    """

    def batch_shp2pgsql(self, dataset, db_engine, db_params, geo_params):
        """Create batch task to store into a PostGIS Database all ESRI Shapefiles ZIPs from a directory.

        Parameters
        ----------
        - self: Datasets object to upload into PostGIS.
        - dataset: Dataset to load into PostGIS DB.
        - db_engine: SQLAlchemy database engine.
        - db_params: Database connection details.
        - geo_params: Geoserver connection details.

        Return
        ----------
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
            dataset = update_srid(dataset, db_params, geo_params.declared_srid)
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

    def load_datasets_to_postgis(self):
        """
        Load all elements available (dataset.status = "db_to-load") in the Datasets object to PostGIS DB and update the status ("db_uploaded").

        Parameters
        ----------
        - self: Datasets object.

        Return
        ----------
        Datasets Object.
        """
        db_engine = create_engine(self.db_params)

        logging.info(log_module + ":" + "Multicore parallel processing: " + str(self.parallel))

        # Load to DB
        if self.load_to_db is True:
            # Multi core processing
            if self.parallel is True:
                logging.info(log_module + ":" + "Number of processes: " + str(self.processes))
                # SHP to Postgis
                Parallel(n_jobs=self.processes, prefer="threads")(delayed(self.batch_shp2pgsql)(dataset=d, db_engine=db_engine, db_params=self.db_params, geo_params=self.geoserver_params) for d in self.datasets if d.status == "db_to-load" and d.file_format == "shp")

            # Single core processing
            else:
                for dataset in self.datasets:
                    if dataset.status == "db_to-load":
                        # SHP to Postgis
                        if dataset.file_format == "shp":
                            self.batch_shp2pgsql(dataset, db_engine, self.db_params, self.geoserver_params)

        return self

class GeoserverLoader(DbLoader):
    """
    Constructor of the GeoserverLoader class.

    Subclass of: DbLoader       
    """
    def load_datasets_to_geoserver(self):
        """
        Load all feature types/coverages available (dataset.status = "db_uploaded" or dataset.status = "geo_to-load") in the Datasets object to Geoserver and update the status ("geoserver_uploaded").

        Parameters
        ----------
        - self: Datasets object.

        Return
        ----------
        Datasets Object.
        """
        datasets = self.datasets
        db_type = self.db_type
        db_params = self.db_params
        geo_params = self.geoserver_params
        workspace = geo_params.workspace
        datastore = geo_params.datastore


        geo = Geoserver(geo_params.url, username=geo_params.username, password=geo_params.password, proxies=self.proxies)

        check_geoserver_workspace(geo, workspace, datastore)
        check_geoserver_datastore(geo, workspace, datastore, db_type, db_params)

        for dataset in datasets:
            if dataset.status == "db_uploaded" or dataset.status == "geo_to-load":
                if dataset.file_srid is None and dataset.carto_type == "vector":
                    dataset = get_srid(dataset, db_params)
                dataset = create_geoserver_layer(geo, workspace, datastore, dataset, db_type, dataset.file_srid, geo_params.declared_srid)

            elif dataset.status == 'db_to-load':
                try:
                    dataset = check_table_exists(dataset, db_params)
                    if dataset.status == "db_uploaded":
                        if dataset.file_srid is None and dataset.carto_type == "vector":
                            dataset = get_srid(dataset, db_params)
                        dataset = create_geoserver_layer(geo, workspace, datastore, dataset, db_type, dataset.file_srid, geo_params.declared_srid)
                except:
                    continue

        return self