#!/usr/bin/env python3
## Coding: UTF-8
## Author: mjanez@tragsa.es
## Institution: -
## Project: -
# inbuilt libraries
import glob
import os
import logging
import hashlib
import unicodedata


log_module = f"[{__name__}]"
log_model_geo = f"[model.geoserver]"


def check_geoserver_workspace(geo, workspace: str, datastore: str):
    """
    Check and create (if needed) the spatial workspace in Geoserver.

    Parameters
    ----------
    - geo: Geoserver connection object.
    - workspace: Geoserver workspace.
    - datastore: Geoserver datastore.
    """
    try:
        geo.get_workspace(workspace=workspace)
        logging.warning(f"{log_module}:Workspace: '{workspace}' exists.")
    except:
        try:
            geo.create_workspace(workspace=workspace)
            logging.info(f"{log_module}:Created workspace: '{datastore}'")
        except Exception as e:
            logging.exception(f"{log_model_geo}:{e}")
                            
def check_geoserver_datastore(geo, workspace: str, datastore: str, db_type: str, db_params):
    """
    Check and create (if needed) the spatial datastore in Geoserver.

    Parameters
    ----------
    - geo: Geoserver connection object.
    - workspace: Geoserver workspace.
    - datastore: Geoserver datastore.
    - db_type: Database type to import to Geoserver.
    - db_params: Database connection details.
    """

    # PostGIS
    if db_type == "postgres" or db_type == "postgis":
        try:
            geo.get_datastore(store_name=datastore, workspace=workspace)
            logging.warning(f"{log_module}:Datastore: '{datastore}' exists.")
        except:
            try:
                geo.create_featurestore(store_name=datastore, workspace=workspace, db=db_params.dbname, host=db_params.host, pg_user=db_params.username, pg_password=db_params.password)
                logging.info(f"{log_module}:Created datastore: '{datastore}'")
            except Exception as e:
                logging.exception(f"{log_model_geo}:{e}")

    else:
        logging.info(f"{log_module}:Create Geoserver datastore of db_type: '{db_type}' not supported yet.")

def get_geoserver_layername(name: str):
    """
    Check and create (if needed) the layername in Geoserver.

    The name of a Geoserver dataset, must be between 2 and 80 characters long and contain only lowercase
    # alphanumeric characters, - and _, e.g. 'warandpeace'

    Parameters
    ----------
    - name: Layer name.

    Return
    ----------
    - geoserver_name: Layer name normalised for use in Geoserver.
    """
    # the name of a Geoserver dataset, must be between 2 and 80 characters long and contain only lowercase
    # alphanumeric characters, - and _, e.g. 'warandpeace'
    normal = str(unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore'))[2:-1]
    normal = normal.lower()
    geoserver_name = ''
    for c in normal:
        if ('0' <= c <= '9') or ('a' <= c <= 'z') or (c == '-') or (c == '_'):
            geoserver_name += c
        else:
            geoserver_name += '_'
    geoserver_name = geoserver_name
    # If name is longer than 80 characters, a hash function is applied
    if len(geoserver_name) >= 80:
        geoserver_name = hashlib.sha1(name.encode("utf-8")).hexdigest()
    return geoserver_name

def create_geoserver_layer(geo, workspace: str, datastore: str, dataset, db_type, file_srid, declared_srid):
    """
    Create a Geoserver layer from differente origin

    Parameters
    ----------
    geo: Geoserver connection object.
    workspace: Geoserver workspace.
    datastore: Geoserver datastore.
    dataset: Dataset object to upload into PostGIS.
    db_type: Database type.
    file_srid: Dataset native CRS code.
    declared_srid: Geoserver declared CRS code.

    Return
    ----------
    Dataset object
    """

    dataset.set_ogc_workspace(workspace)
    dataset.set_ogc_layer(get_geoserver_layername(dataset.table))

    # Vector data
    if dataset.carto_type == "vector":

        # PostGIS
        if db_type == "postgres" or db_type == "postgis":
            try:
                geo.get_layer(layer_name=dataset.table, workspace=workspace)
                logging.warning(f"{log_module}:Layer: '{dataset.table}' exists.")
            except:
                try:
                    geo.publish_featurestore(workspace=workspace, store_name=datastore, pg_table=dataset.table, title=dataset.name,srid=file_srid, declared_srid=declared_srid)
                    logging.info(f"{log_module}:Created table: '{dataset.schema}.{dataset.table}' as Geoserver FeatureType: '{workspace}:{dataset.ogc_layer}' with EPSG:{declared_srid}")
                    dataset.set_status('geoserver_uploaded')
                    dataset.set_declared_srid(declared_srid)
                    dataset.set_status_info(f"Created table: '{dataset.schema}.{dataset.table}' as Geoserver FeatureType: '{workspace}:{dataset.ogc_layer}' with EPSG:{declared_srid}")

                except Exception as e:
                    logging.exception(f"{log_model_geo}:{e}")
                    dataset.set_status('error')
                    dataset.set_status_info(f"Error when trying to publish table: '{dataset.schema}.{dataset.table}' as Geoserver FeatureType: '{workspace}:{dataset.ogc_layer}'.")

        # TODO:SQL Server
        elif db_type == "sql-server":
            logging.info(f"{log_module}:Create Geoserver FeatureType of db_type: '{db_type}' not supported yet.")
            dataset.set_status('error')
            dataset.set_status_info(f"Create Geoserver FeatureType of db_type: '{db_type}' not supported yet.")            

        else:
            logging.info(f"{log_module}:Create Geoserver FeatureType of db_type: '{db_type}' not supported yet.")
            dataset.set_status('error')
            dataset.set_status_info(f"Create Geoserver FeatureType of db_type: '{db_type}' not supported yet.")

    # Raster data
    elif dataset.carto_type == "raster":

        # GeoTIFF
        if dataset.file_format == "tiff":
            try:
                geo.get_layer(layer_name=dataset.table, workspace=workspace)
                logging.warning(f"{log_module}:Coverage Layer: '{dataset.table}' exists.")
            except:
                try:
                    geo.create_coveragestore(path=dataset.file_path,workspace=workspace, layer_name=dataset.ogc_layer, title=dataset.name,srid=file_srid, declared_srid=declared_srid)
                    logging.info(f"{log_module}:Created table: '{dataset.schema}.{dataset.table}' as Geoserver Coverage: '{workspace}:{dataset.ogc_layer}'")
                    dataset.set_status('geoserver_uploaded')
                    dataset.set_status_info(f"Created table: '{dataset.schema}.{dataset.table}' as Geoserver Coverage: '{workspace}:{dataset.ogc_layer}'")

                except Exception as e:
                    logging.exception(f"{log_model_geo}:{e}")
                    dataset.set_status('error')
                    dataset.set_status_info(f"Error when trying to publish table: '{dataset.schema}.{dataset.table}' as Geoserver FeatureType: '{workspace}:{dataset.ogc_layer}'.")

        else:
            logging.info(f"{log_module}:Create Geoserver Coverage layer of file_format: '{dataset.file_format}' not supported yet.")
            dataset.set_status('error')
            dataset.set_status_info(f"Create Geoserver Coverage layer of file_format: '{dataset.file_format}' not supported yet.")

    return dataset