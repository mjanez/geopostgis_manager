#!/usr/bin/env python3
## File: run.py
## Coding: UTF-8
## Author: Manuel Ángel Jáñez García (mjanez@tragsa.es)
## Institution: Tragsatec
## Project: EIKOS
## Goal: The goal of this script is to provide the program to test ckan/ogc_ckan/ckan_management.py and ckan/ogc_ckan/Dataset.py. Modified run.py to harvest CSW endpoints from ogc_ckan.
## Parent: ogc_ckan/run.py
""" Changelog:
    v2.0 - 30 Ago 2022: Upgraded with process parallelization, management of different harvester types (OGC, CSW) and with expansion possibilities.
    v1.0 - 16 Ago 2022: Create the first version
"""
# Update the version when apply changes 
version = "2.0"

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
##                run.py                ##
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##

# Main program to test ckan/ogc_ckan/ckan_management.py and ckan/ogc_ckan/Dataset.py
#  Call this script as "python3 ckan/ogc_ckan/run.py" inside ""./ckan-harvester/src/ckan"

## Import libraries
import logging
from datetime import datetime   
import os
import ckan_management
from OGCMetadataHarvester import OGCMetadataHarvester
from ckan_config import config_getParameters, config_getConnection
from joblib import Parallel, delayed
from pathlib import Path

# Config default path
config_file = os.path.abspath(__file__ + "/../../../../config.yml")

# Logging
def log_file(log_folder):
    '''
    Starts the logger --log_folder parameter entered
    
    :param log_folder: Folder where log is stored 

    :return: Logger object
    '''
    logger = logging.getLogger()
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    logging.basicConfig(
                        handlers=[logging.FileHandler(filename=log_folder + "/ogc_ckan.log", encoding='utf-8', mode='a+')],
                        format="%(asctime)s %(levelname)s::%(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S", 
                        level=logging.INFO
                        )
    return logger

# Function disabled | Only new datasets are created, not updated (ingest) to avoid edits made on the CKAN panel
"""
def ingest_datasets(harvest_name, harvest_type, log_folder, ckan_site_url=None, harvest_url=None, groups=None, organization_name=None, default_dcat_info=None, authorization_key=None, workspaces=None, default_keywords=None, default_license="https://creativecommons.org/licenses/by/4.0/", license_id="cc-by", inspireid_theme="HB", inspireid_nutscode='ES', inspireid_versionid=None, default_bbox='{"type": "Polygon", "coordinates": [[[-19.0, 27.0], [4.57, 27.0], [4.57, 44.04], [-19.0, 44.04], [-19.0, 27.0]]]}', constraints=None):
    '''
    Ingest datasets if you are interested in creating or updating

    :param harvest_name: Name of server
    :param harvest_type: Type of server
    :param log_folder: Folder where log is stored 
    :param ckan_site_url: CKAN Server url
    :param harvest_url: CSW Server base url
    :param groups: Name of CKAN groups
    :param organization_name: CKAN Organization name (http://localhost:5000/api/action/organization_list)
    :param default_dcat_info: Default publisher info
    :param authorization_key: API Key (http://localhost:5000/user/admin)
    :param workspaces: OGC workspaces
    :param default_keywords: Default keyword for datasets
    :param default_license: Default dataset/distribution license
    :param license_id: CKAN license_id 
    :param inspireid_theme: INSPIRE Theme code
    :param inspireid_nutscode: NUTS0 Code
    :param inspireid_versionid: VersionID Code
    :param default_bbox: Default Bounding Box GeoJSON of Spain
    :param constraints: The list of constraints conditions of CSW GetRecords request. (https://github.com/geopython/OWSLib/blob/7ac4f2753bbb02215956043af00abf1040da5724/owslib/catalogue/csw2.py#L311)

    :return: Datasets object
    '''
    datasets = OGCMetadataHarvester(harvest_url).get_all_datasets(harvest_type=harvest_type, ckan_site_url=ckan_site_url, groups=groups, organization_name=organization_name, default_keywords=default_keywords,default_license=default_license, license_id=license_id, inspireid_theme=inspireid_theme, inspireid_nutscode=inspireid_nutscode, inspireid_versionid=inspireid_versionid, default_bbox=default_bbox, constraints=constraints, workspaces=workspaces, default_provenance=default_provenance)
    
    if workspaces is not None:
        logging.info(harvest_name + " (" + harvest_type.upper() + ") server " + "OGC workspaces selected: " + ', '.join([w.upper() for w in workspaces]))

        ckan_count, server_count = ckan_management.create_datasets(log_folder, ckan_site_url, authorization_key, datasets, inspireid_theme, inspireid_nutscode, workspaces)
    
    else:
        ckan_count, server_count = ckan_management.create_datasets(log_folder, ckan_site_url, authorization_key, datasets, inspireid_theme, inspireid_nutscode)

    return ckan_count, server_count, datasets
"""

def create_datasets(harvest_name, harvest_type, log_folder, ckan_site_url=None, harvest_url=None, groups=None, organization_name=None, default_dcat_info=None, authorization_key=None, workspaces=None, default_provenance=None, default_keywords=None, default_license="https://creativecommons.org/licenses/by/4.0/", license_id="cc-by", inspireid_theme="PS", inspireid_nutscode='ES', inspireid_versionid=None, default_bbox='{"type": "Polygon", "coordinates": [[[-19.0, 27.0], [4.57, 27.0], [4.57, 44.04], [-19.0, 44.04], [-19.0, 27.0]]]}', constraints=None):
    '''
    Create datasets if you are only interested in creating new datasets
    
    :param harvest_name: Name of server
    :param harvest_type: Type of server
    :param log_folder: Folder where log is stored 
    :param ckan_site_url: CKAN Server url
    :param harvest_url: CSW Server base url
    :param groups: Name of CKAN groups
    :param organization_name: CKAN Organization name (http://localhost:5000/api/action/organization_list)
    :param default_dcat_info: Default publisher info
    :param authorization_key: API Key (http://localhost:5000/user/admin)
    :param workspaces: OGC workspaces
    :param default_provenance: Default provenance statement
    :param default_keywords: Default keyword for datasets
    :param default_license: Default dataset/distribution license
    :param license_id: CKAN license_id 
    :param inspireid_theme: INSPIRE Theme code
    :param inspireid_nutscode: NUTS0 Code
    :param inspireid_versionid: VersionID Code
    :param default_bbox: Default Bounding Box GeoJSON of Spain
    :param constraints: The list of constraints conditions of CSW GetRecords request. (https://github.com/geopython/OWSLib/blob/7ac4f2753bbb02215956043af00abf1040da5724/owslib/catalogue/csw2.py#L311)

    :return: CSW Records and CKAN New records counters and Datasets object
    '''
    datasets = OGCMetadataHarvester(harvest_url).get_all_datasets(harvest_type=harvest_type, ckan_site_url=ckan_site_url, groups=groups, organization_name=organization_name, default_dcat_info=default_dcat_info, default_keywords=default_keywords,default_license=default_license, license_id=license_id, inspireid_theme=inspireid_theme, inspireid_nutscode=inspireid_nutscode, inspireid_versionid=inspireid_versionid, default_bbox=default_bbox, constraints=constraints, workspaces=workspaces, default_provenance=default_provenance)
    
    try:
        datasets_title = [x.title for x in datasets if x.contact_email.lower().replace(' ','') in constraints["mails"]]
        logging.info(harvest_name + " (" + harvest_type.upper() + ") server " + "records found: " + ', '.join([r for r in datasets_title]))
    except:
        pass

    

    if workspaces is not None:
        logging.info(harvest_name + " (" + harvest_type.upper() + ") server " + "OGC workspaces selected: " + ', '.join([w.upper() for w in workspaces]))

        ckan_count, server_count = ckan_management.create_datasets(log_folder, ckan_site_url, authorization_key, datasets, inspireid_theme, inspireid_nutscode, workspaces)
    
    else:
        ckan_count, server_count = ckan_management.create_datasets(log_folder, ckan_site_url, authorization_key, datasets, inspireid_theme, inspireid_nutscode)

    return ckan_count, server_count, datasets

def launch_harvest(harvest_type=None, harvest_server=None, ckan_info=None):
    """
    Launch harvesting process dependes on type of harvester

    :param harvest_type: Type of harvester of server
    :param harvest_server: Harvest server parameters
    :param ckan_info: CKAN Parameters from config.yml 

    :return: CSW Records and CKAN New records counters
    """
    start = datetime.now()
    log_file(log_folder)

    if harvest_type =="csw":
        kwargs = {
            'harvest_name': harvest_server["name"],
            'harvest_type': harvest_type,
            'log_folder': log_folder,
            'ckan_site_url' : ckan_info["ckan_site_url"],
            'harvest_url' : harvest_server["url"],
            'groups': harvest_server["groups"],
            'organization_name' : harvest_server["organization"],
            'default_dcat_info' : harvest_server["default_dcat_info"],
            'authorization_key' : ckan_info["authorization_key"],
            'default_keywords': harvest_server["default_keywords"],
            'default_license': ckan_info["default_license"],
            'license_id' : ckan_info["license_id"],
            'inspireid_theme': harvest_server["inspireid_theme"],
            'constraints' : harvest_server["constraints"],
            'default_bbox' : harvest_server["default_bbox"],
        }

    elif harvest_type =="ogc":
        kwargs = {
            'harvest_name': harvest_server["name"],
            'harvest_type': harvest_type,
            'log_folder': log_folder,
            'ckan_site_url' : ckan_info["ckan_site_url"],
            'harvest_url' : harvest_server["url"],
            'groups': harvest_server["groups"],
            'organization_name' : harvest_server["organization"],
            'default_dcat_info' : harvest_server["default_dcat_info"],
            'authorization_key' : ckan_info["authorization_key"],
            'workspaces' : harvest_server["workspaces"],
            'default_provenance': harvest_server["default_provenance"],
            'default_keywords': harvest_server["default_keywords"],
            'default_license': ckan_info["default_license"],
            'license_id' : ckan_info["license_id"],
            'inspireid_theme': harvest_server["inspireid_theme"],
            'inspireid_nutscode': harvest_server["inspireid_nutscode"],
            'inspireid_versionid': harvest_server["inspireid_versionid"],
        }

    logging.info(harvest_server["name"] + " (" + harvest_type.upper() + ") server: " +  harvest_server['url'] + " CKAN organization: " + ckan_info["ckan_site_url"] + "/organization/" + harvest_server["organization"])
    try:
        ckan_count, server_count, datasets = create_datasets(**kwargs)
        
        # Outputinfo
        end = datetime.now()
        diff =  end - start

        logging.info(harvest_server["name"] + " (" + harvest_type.upper() + ") server records retrieved (" + str(server_count) + ") with conflicts: (" + str(server_count - ckan_count) + ") from (" + harvest_type.upper() + "): [" + ', '.join([d.title for d in datasets]) + "]")
        logging.info(harvest_server["name"] + " (" + harvest_type.upper() + ") server time elapsed: " + str(diff).split(".")[0])
    except BaseException:
        logging.exception("An exception was thrown!")
        ckan_count = 0
        server_count = 0
        datasets = None

        # Outputinfo
        end = datetime.now()
        diff =  end - start

        logging.error(harvest_server["name"] + " (" + harvest_type.upper() + ") server: " +  harvest_server['url'] + ' failed connection.')
        print("ERROR::" + harvest_server["name"] + " (" + harvest_type.upper() + ") server: " +  harvest_server['url'] + ' failed connection.')

    return ckan_count, server_count

if __name__ == '__main__':
    # Retrieve parameters and init log
    harvester_start = datetime.now()
    default_config, log_folder, db_dsn, ckan_info, harvest_servers  = config_getParameters(config_file)
    log_folder = os.path.abspath(__file__ + "/../../../../log")
    print("Log folder:" + log_folder)
    log_file(log_folder)

    # Processes
    processes = os.cpu_count() - 1
    new_records = []

    # Type of server to harvest
    if ckan_info["ckan_harvester"] is not None:
        harvester_types = [h["type"] for h in ckan_info["ckan_harvester"] if h['active'] is True]
        
        # Filter harvest_servers by harvester_types
        harvest_servers = [e for e in harvest_servers if e['type'] in harvester_types and e['active'] is True]

        # Starts software
        logging.info("// MNR-DGPCE/ogc_ckan // Version:" + version)
        logging.info("Number of processes: " + str(processes))
        logging.info("Multicore parallel processing: " + str(default_config["parallelization"]))   
        logging.warning("Type of activated harvesters: " + ', '.join([h.upper() for h in harvester_types]))
        logging.info("ckan_site_url: " + ckan_info["ckan_site_url"])

        # Check invalid 'type' parameter in config.yml
        if harvest_servers is not None:
            #conn = config_getConnection(db_dsn[host], db_dsn[port], db_dsn[username], db_dsn[password], db_dsn[dbname])

            if default_config["parallelization"] is True:
                # Multicore parallel processing
                parallel_count = Parallel(n_jobs=processes)(delayed(launch_harvest)(harvest_type=endpoint['type'].lower(), harvest_server=endpoint, ckan_info=ckan_info) for endpoint in harvest_servers)
                new_records.append(sum(i[0] for i in parallel_count))

            else:
                # Single core processing
                for endpoint in harvest_servers:
                    ckan_count, server_count = launch_harvest(harvest_type=endpoint['type'].lower(), harvest_server=endpoint, ckan_info=ckan_info)
                    new_records.append(ckan_count)

        else:
            print("ERROR::Check invalid 'type' and 'active: True' in 'harvest_servers/{my-harvest-server}' at: " + str(Path(config_file)))
            logging.error("Check invalid 'type' and 'active: True' in 'harvest_servers/{my-harvest-server}' at: " + str(Path(config_file)))     
            new_records = 0

    else:
        print("ERROR::Activate at least one of the options of 'ckan_info/ckan_harvester' in the configuration file: " + str(Path(config_file)))
        logging.error("Activate at least one of the options of 'ckan_info/ckan_harvester' in the configuration file: " + str(Path(config_file)))  


    # ogc_ckan outputinfo
    harvester_end = datetime.now()
    hrvst_diff =  harvester_end - harvester_start

    try:
        new_records = sum(new_records)
    except:
        new_records = 0

    logging.info("// MNR-DGPCE/ogc_ckan // config.yml endpoints: " + str(len(harvest_servers)) + " and new CKAN datasets: " + str(new_records) + " | Total time elapsed: " + str(hrvst_diff).split(".")[0])