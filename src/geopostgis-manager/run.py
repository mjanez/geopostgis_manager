#!/usr/bin/env python3
## Coding: UTF-8
## Author: mjanez@tragsa.es
## Institution: -
## Project: -
# inbuilt libraries
import logging
from datetime import datetime   
import os

# custom functions
from config.config import config_get_parameters
from config.log import log_file

# custom classes
from controller.dataset_loader import DbLoader
from controller.dataset_loader import GeoserverLoader


HERE = os.path.abspath(os.path.dirname(__file__))
log_module = "[run.main]"

def generate_datasets_object(bundle, bundle_doc, db_type=None, log_folder=None):
    """
    Launch ingesting process dependes on type of db

    Parameters
    ----------
    - bundle: DB/Geoserver server parameters
    - bundle_doc: Dict contains all info about datasets of the bundle_id from config.yml
    - db_type: Type of DB of server
    - log_folder: Logging file folder

    Return
    ----------
    DB Records and New records counters
    """

    # datasets doc
    if bundle_doc.db_datasets_doc_mode:
        datasets_table = bundle.db_datasets_doc_table
        datasets_mode = "db"
    else:
        datasets_table = bundle_doc.datasets_doc_path
        datasets_mode = "file"

    # proxies
    if default_config.proxy_http:
        proxies = {
            'http': default_config.proxy_http,
            'https': default_config.proxy_http
            }
    elif default_config.proxy_socks5:
        proxies = {
        'http': default_config.proxy_socks5,
        'https': default_config.proxy_socks5
        }
    else:
        proxies = None

    kwargs_db = dict(
        bundle_id = bundle.bundle_id,
        log_folder = log_folder,
        db_type = db_type.lower(),
        db_params = dict(
            endpoint = bundle.db_endpoint,
            host = bundle.db_host,
            port = bundle.db_port,
            username = bundle.db_username,
            password = bundle.db_password,
            dbname = bundle.db_dbname,
            active = bundle.db_active,
        ),
        geoserver_params = dict(
            endpoint = bundle.geo_endpoint,
            url = bundle.geo_url if "/geoserver" in bundle.geo_url else bundle.geo_url.split("/")[0] + '/geoserver',
            datastore = bundle.geo_datastore,
            username = bundle.geo_username,
            password = bundle.geo_password,
            workspace = bundle.geo_workspace,
            geo_srid = bundle.geo_srid,
            active = bundle.geo_active,
        ),
        datasets_doc = bundle_doc,
        datasets_table = datasets_table,
        datasets_mode = datasets_mode,
        parallel = default_config.parallelization,
        proxies = proxies,
        load_to_db = default_config.load_to_db,
        load_to_geoserver = default_config.load_to_geoserver
    )
    
    # Create a list with all Dataset objects to be loaded into the database
    if default_config.load_to_geoserver == True:
        obj_datasets = GeoserverLoader(**kwargs_db)
    else:
        obj_datasets = DbLoader(**kwargs_db)

    logging.info(f"{log_module}:Modes: Load datasets into Database='{default_config.load_to_db}' and Geoserver='{default_config.load_to_geoserver}'")

    if obj_datasets.geoserver_params.workspace is not None:
        logging.warning(f"{log_module}:The 'geo_workspace' parameter with value: '{obj_datasets.geoserver_params.workspace}' of 'config.yml' is being used as 'workspace' in Geoserver.")

    logging.info(f"{log_module}:Datasets origin: '{datasets_mode.upper()}'  from: {datasets_table}")

    return obj_datasets

def ingest_db(obj_datasets):
    """
    Launch ingesting process dependes on type of db

    Parameters
    ----------
    - obj_datasets: Datasets object
    Return
    ----------
    DB Records and New records counters
    """

    # PostGIS
    if obj_datasets.db_type == "postgres" or obj_datasets.db_type == "postgis":
        obj_datasets = obj_datasets.load_datasets_to_postgis()

        return obj_datasets

    #TODO: SQL Server
    elif obj_datasets.db_type == "sql-server":
        logging.warning(f"{log_module}:Database type: {obj_datasets.db_type} not supported yet.")

        return None

    #TODO: MongoDB
    elif obj_datasets.db_type == "mongo-db":
        logging.warning(f"{log_module}:Database type: {obj_datasets.db_type} not supported yet.")

        return None

    else:
        logging.error(f"{log_module}:Database type: {obj_datasets.db_type} not supported.")

        return None

def ingest_geoserver(obj_datasets):
    """
    Launch ingesting process on Geoserver

    Parameters
    ----------
    - obj_datasets: Datasets object
    Return
    ----------
    DB Records and New records counters
    """
    # PostGIS
    if obj_datasets.db_type == "postgres" or obj_datasets.db_type == "postgis":
        obj_datasets = obj_datasets.load_datasets_to_geoserver()

        return obj_datasets

    else:
        logging.error(f"{log_module}:Data origin: {obj_datasets.db_type} not supported.")

    return None

if __name__ == '__main__':
    # About (__version__.py)
    about = dict()
    with open(os.path.join(HERE, "__version__.py")) as f:
        exec(f.read(), about)

    # Retrieve parameters and init log
    harvester_start = datetime.now()
    geopostgis_bundles, datasets_doc, default_config  = config_get_parameters()
    log_folder = os.path.abspath(HERE + "/../../log")
    print("Log folder: " + log_folder)
    log_file(log_folder)

    # Starts software
    logging.info(f"{log_module}:{about['__name__']} | Version: {about['__version__']}")
    
    # Check invalid 'type' parameter in config.yml
    for bundle in geopostgis_bundles if geopostgis_bundles is not None else None:
        # Generate bundle datasets_doc
        bundle_doc = next((x for x in datasets_doc if x.bundle_id == bundle.bundle_id), None)

        # Generate Datasets object
        obj_datasets = generate_datasets_object(bundle=bundle, db_type=bundle.db_type.lower(), log_folder=log_folder, bundle_doc=bundle_doc)

        # Ingest to DB
        if bundle.db_active == True:
            if default_config.load_to_db == True:
                obj_datasets = ingest_db(obj_datasets)

            else:
                logging.warning(f"{log_module}:Try to upload into Database but in 'config.yml' the key default.load_to_db: '{default_config.load_to_db}'")

        # Ingest to Geoserver 
        if bundle.geo_active == True:
            if default_config.load_to_geoserver == True:
                obj_datasets = ingest_geoserver(obj_datasets)

            else:
                logging.warning(f"{log_module}:Try to upload into Geoserver but in 'config.yml' the key default.load_to_geoserver: '{default_config.load_to_geoserver}'")

        # geopostgis-manager output_info
        obj_datasets.output_info.set_csv(log_folder, obj_datasets.datasets)
        obj_datasets.output_info.set_output_info(obj_datasets.datasets)
        elapsedtime = str(datetime.now() - harvester_start).split(".")[0]
        logging.info(
            f"{log_module}:geopostgis-bundle: '{bundle.bundle_id}'\nResume: new DB datasets: {obj_datasets.output_info.db_records} - new Geoserver datasets: {obj_datasets.output_info.geo_records} - errors: {obj_datasets.output_info.error_records} - Total datasets in doc: {obj_datasets.output_info.total_records} | Total time elapsed: {elapsedtime}"
        )
        logging.info(f"{log_module}:Datasets logfile:'{obj_datasets.output_info.zip_file}'")